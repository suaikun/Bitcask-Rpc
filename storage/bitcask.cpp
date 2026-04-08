#include "bitcask.h"
#include <iostream>
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <thread>

namespace storage {
bool BitcaskShard::write_all(int fd, const void* buf, size_t len) {
    const char* p = static_cast<const char*>(buf);
    size_t left = len;
    while (left > 0) {
        ssize_t n = ::write(fd, p, left);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false;
        p += static_cast<size_t>(n);
        left -= static_cast<size_t>(n);
    }
    return true;
}

bool BitcaskShard::pread_all(int fd, void* buf, size_t len, off_t offset) {
    char* p = static_cast<char*>(buf);
    size_t left = len;
    off_t off = offset;
    while (left > 0) {
        ssize_t n = ::pread(fd, p, left, off);
        if (n < 0) {
            if (errno == EINTR) continue;
            return false;
        }
        if (n == 0) return false; 
        p += static_cast<size_t>(n);
        off += static_cast<off_t>(n);
        left -= static_cast<size_t>(n);
    }
    return true;
}

bool BitcaskShard::file_exists(const std::string& path) {
    return ::access(path.c_str(), F_OK) == 0;
}

int BitcaskShard::next_free_file_id(int start_id) const {
    int id = start_id;
    for (;;) {
        const std::string data = db_dir_ + "/" + std::to_string(id) + ".data";
        const std::string hint = db_dir_ + "/" + std::to_string(id) + ".hint";
        if (!file_exists(data) && !file_exists(hint)) return id;
        ++id;
    }
}

BitcaskShard::~BitcaskShard() {
    if (active_fd_ != -1) close(active_fd_);
    for (auto& pair : old_fds_) close(pair.second);
}

bool BitcaskShard::init(const std::string& base_dir) {
    //为当前分片创建专属物理子目录
    db_dir_ = base_dir + "/shard_" + std::to_string(shard_id_);
    if (mkdir(db_dir_.c_str(), 0777) != 0 && errno != EEXIST) {
        return false;
    }

    std::unique_ptr<DIR, int(*)(DIR*)> dir(opendir(db_dir_.c_str()), closedir);
    if (!dir) return false;

    std::vector<int> file_ids;
    struct dirent* entry;
    
    while ((entry = readdir(dir.get())) != nullptr) {
        std::string fname = entry->d_name;
        if (fname.size() > 5 && fname.substr(fname.size() - 5) == ".data") {
            int id = std::stoi(fname.substr(0, fname.size() - 5));
            file_ids.push_back(id);
        }
    }

    std::sort(file_ids.begin(), file_ids.end());

    if (!file_ids.empty()) {
        for (int id : file_ids) {
            std::string data_path = db_dir_ + "/" + std::to_string(id) + ".data";
            std::string hint_path = db_dir_ + "/" + std::to_string(id) + ".hint";
            
            int data_fd = open(data_path.c_str(), O_RDONLY); 
            if (data_fd == -1) continue;
            
            int hint_fd = open(hint_path.c_str(), O_RDONLY);
            if (hint_fd != -1) {
                struct stat st;
                if (fstat(hint_fd, &st) != 0) {
                    close(hint_fd); close(data_fd); continue;
                }
                off_t offset = 0;
                while (offset < st.st_size) {
                    uint64_t ts = 0; int64_t v_pos64 = 0; uint8_t tombstone = 0; uint32_t k_len = 0, v_len = 0;
                    
                    if (offset + (off_t)HINT_HEADER_SIZE > st.st_size) break;
                    if (!pread_all(hint_fd, &ts, 8, offset)) break;
                    if (!pread_all(hint_fd, &tombstone, 1, offset + 8)) break;
                    if (!pread_all(hint_fd, &k_len, 4, offset + 9)) break;
                    if (!pread_all(hint_fd, &v_len, 4, offset + 13)) break;
                    if (!pread_all(hint_fd, &v_pos64, 8, offset + 17)) break;
                    if (k_len == 0 || k_len > (16u * 1024u * 1024u)) break;
                    
                    std::string k; k.resize(k_len);
                    if (!pread_all(hint_fd, &k[0], k_len, offset + (off_t)HINT_HEADER_SIZE)) break;
                    
                    if (tombstone == 1) index_.erase(k);
                    else index_[k] = {id, static_cast<off_t>(v_pos64), v_len, ts};
                    offset += HINT_HEADER_SIZE + k_len;
                }
                close(hint_fd);
            } else {
                struct stat st;
                if (fstat(data_fd, &st) != 0) { close(data_fd); continue; }
                off_t offset = 0;
                while (offset < st.st_size) {
                    uint32_t crc; uint64_t ts; uint8_t tombstone; uint32_t k_len, v_len;
                    if (offset + (off_t)DATA_HEADER_SIZE > st.st_size) break;
                    if (!pread_all(data_fd, &crc, 4, offset)) break;
                    if (!pread_all(data_fd, &ts, 8, offset + 4)) break;
                    if (!pread_all(data_fd, &tombstone, 1, offset + 12)) break;
                    if (!pread_all(data_fd, &k_len, 4, offset + 13)) break;
                    if (!pread_all(data_fd, &v_len, 4, offset + 17)) break;
                    if (k_len == 0 || k_len > (16u * 1024u * 1024u)) break;
                    if (offset + (off_t)DATA_HEADER_SIZE + (off_t)k_len + (off_t)v_len > st.st_size) break;
                    
                    std::string k; k.resize(k_len);
                    if (!pread_all(data_fd, &k[0], k_len, offset + (off_t)DATA_HEADER_SIZE)) break;
                    
                    if (tombstone == 1) index_.erase(k);
                    else index_[k] = {id, offset + DATA_HEADER_SIZE + (off_t)k_len, v_len, ts};
                    offset += DATA_HEADER_SIZE + k_len + v_len;
                }
            }
            old_fds_[id] = data_fd; 
            active_file_id_ = std::max(active_file_id_, id + 1); 
        }
    }
    rotate_active_file();
    return true;
}

void BitcaskShard::rotate_active_file() {
    if (active_fd_ != -1) {
        close(active_fd_); 
        std::string old_name = db_dir_ + "/" + std::to_string(active_file_id_) + ".data";
        int ro_fd = open(old_name.c_str(), O_RDONLY);
        if (ro_fd != -1) old_fds_[active_file_id_] = ro_fd;
        active_file_id_++;
    }

    std::string new_name = db_dir_ + "/" + std::to_string(active_file_id_) + ".data";
    active_fd_ = open(new_name.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
    if (active_fd_ != -1) {
        current_offset_ = lseek(active_fd_, 0, SEEK_END);
        if (current_offset_ < 0) current_offset_ = 0;
    } else {
        current_offset_ = 0;
    }
}

int BitcaskShard::append_record(const std::string& key, const std::string& value, uint8_t tombstone) {
    uint64_t ts = get_timestamp();
    uint32_t key_size = key.size();
    uint32_t val_size = value.size();

    std::string body;
    body.append((char*)&ts, 8);
    body.append((char*)&tombstone, 1);
    body.append((char*)&key_size, 4);
    body.append((char*)&val_size, 4);
    body.append(key);
    body.append(value);

    uint32_t crc = crc32_checksum(body.data(), body.size());

    std::string record;
    record.append((char*)&crc, 4);
    record.append(body);

    std::unique_lock<std::shared_timed_mutex> lock(rw_mutex_); 

    if (current_offset_ + (off_t)record.size() > MAX_FILE_SIZE) {
        rotate_active_file();
    }

    if (active_fd_ == -1) return -1;
    if (!write_all(active_fd_, record.data(), record.size())) return -1;

    if (tombstone == 1) {
        index_.erase(key);
    } else {
        index_[key] = {active_file_id_, current_offset_ + DATA_HEADER_SIZE + (off_t)key_size, val_size, ts};
    }
            
    current_offset_ += record.size(); 
    return 0; 
}

int BitcaskShard::set(const std::string& key, const std::string& value) {
    return append_record(key, value, 0);
}

int BitcaskShard::del(const std::string& key) {
    std::unique_lock<std::shared_timed_mutex> lock(rw_mutex_);
    if (index_.find(key) == index_.end()) return 0;
    lock.unlock(); 
    return append_record(key, "", 1);
}

int BitcaskShard::get(const std::string& key, std::string& value) {
    std::shared_lock<std::shared_timed_mutex> lock(rw_mutex_); 

    auto it = index_.find(key);
    if (it == index_.end()) return -1; 

    IndexNode node = it->second;
    value.resize(node.val_size);

    int target_fd = -1;
    if (node.file_id == active_file_id_) {
        target_fd = active_fd_;
    } else {
        auto fd_it = old_fds_.find(node.file_id);
        if (fd_it != old_fds_.end()) target_fd = fd_it->second;
    }
    if (target_fd == -1) return -1;

    std::string verify_buf;
    verify_buf.resize(DATA_HEADER_SIZE + key.size() + node.val_size);
    off_t record_start_pos = node.val_pos - DATA_HEADER_SIZE - key.size();
    
    if (!pread_all(target_fd, &verify_buf[0], verify_buf.size(), record_start_pos)) return -1;

    uint32_t saved_crc;
    memcpy(&saved_crc, verify_buf.data(), 4);
    
    uint32_t calc_crc = crc32_checksum(verify_buf.data() + 4, verify_buf.size() - 4);
    if (saved_crc != calc_crc) return -1; 

    value.assign(verify_buf.data() + DATA_HEADER_SIZE + key.size(), node.val_size);
    return 0; 
}

void BitcaskShard::merge() {
    std::vector<int> merge_old_ids;
    std::vector<std::string> keys_to_merge;
    {
        std::shared_lock<std::shared_timed_mutex> lock(rw_mutex_);
        for (auto& pair : old_fds_) merge_old_ids.push_back(pair.first);
        for (auto& it : index_) {
            if (it.second.file_id != active_file_id_) keys_to_merge.push_back(it.first);
        }
    }
    if (merge_old_ids.empty()) return;

    int merge_id_base = next_free_file_id(active_file_id_);
    int current_merge_fd = -1;
    int current_hint_fd = -1;
    off_t merge_offset = 0;
    std::vector<int> new_merge_file_ids;

    auto open_new_merge_file = [&]() {
        if (current_merge_fd != -1) { close(current_merge_fd); close(current_hint_fd); }
        std::string data_name = db_dir_ + "/" + std::to_string(merge_id_base) + ".data";
        std::string hint_name = db_dir_ + "/" + std::to_string(merge_id_base) + ".hint";
        current_merge_fd = open(data_name.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
        current_hint_fd  = open(hint_name.c_str(), O_CREAT | O_RDWR | O_APPEND, 0666);
        new_merge_file_ids.push_back(merge_id_base);
        merge_offset = 0;
    };

    open_new_merge_file();

    for (const auto& key : keys_to_merge) {
        std::string value;
        IndexNode node;
        int src_fd = -1;
        {
            std::shared_lock<std::shared_timed_mutex> lock(rw_mutex_);
            auto it = index_.find(key);
            if (it == index_.end()) continue;
            node = it->second;
            if (node.file_id != active_file_id_) {
                auto fd_it = old_fds_.find(node.file_id);
                if (fd_it != old_fds_.end()) src_fd = fd_it->second;
            }
        }
        if (node.file_id == active_file_id_ || src_fd == -1) continue; 

        value.resize(node.val_size);
        if (!pread_all(src_fd, &value[0], node.val_size, node.val_pos)) continue;

        uint64_t ts = node.ts; 
        uint8_t tombstone = 0;
        uint32_t k_size = key.size(), v_size = value.size();
        
        std::string body;
        body.append((char*)&ts, 8); body.append((char*)&tombstone, 1);
        body.append((char*)&k_size, 4); body.append((char*)&v_size, 4);
        body.append(key); body.append(value);
        
        uint32_t crc = crc32_checksum(body.data(), body.size());
        std::string data_record;
        data_record.append((char*)&crc, 4); data_record.append(body);

        if (merge_offset + (off_t)data_record.size() > MAX_FILE_SIZE) {
            merge_id_base = next_free_file_id(merge_id_base + 1);
            open_new_merge_file();
        }

        if (current_merge_fd == -1 || current_hint_fd == -1) break;
        if (!write_all(current_merge_fd, data_record.data(), data_record.size())) break;

        off_t new_val_pos = merge_offset + DATA_HEADER_SIZE + k_size;
        std::string hint_record;
        hint_record.append((char*)&ts, 8); hint_record.append((char*)&tombstone, 1);
        hint_record.append((char*)&k_size, 4); hint_record.append((char*)&v_size, 4);
        hint_record.append((char*)&new_val_pos, 8); 
        hint_record.append(key);
        
        if (!write_all(current_hint_fd, hint_record.data(), hint_record.size())) break;

        {
            std::unique_lock<std::shared_timed_mutex> lock(rw_mutex_);
            auto it = index_.find(key);
            if (it != index_.end() && it->second.file_id == node.file_id && it->second.val_pos == node.val_pos) {
                it->second.file_id = merge_id_base;
                it->second.val_pos = new_val_pos;
            }
        }
        merge_offset += data_record.size();
    }

    if (current_merge_fd != -1) { close(current_merge_fd); close(current_hint_fd); }

    {
        std::unique_lock<std::shared_timed_mutex> lock(rw_mutex_);
        for (int id : merge_old_ids) {
            close(old_fds_[id]);       
            old_fds_.erase(id);        
            std::string old_data = db_dir_ + "/" + std::to_string(id) + ".data";
            std::string old_hint = db_dir_ + "/" + std::to_string(id) + ".hint";
            unlink(old_data.c_str()); 
            unlink(old_hint.c_str()); 
        }
        for (int id : new_merge_file_ids) {
            std::string m_name = db_dir_ + "/" + std::to_string(id) + ".data";
            int fd = open(m_name.c_str(), O_RDONLY);
            if (fd != -1) old_fds_[id] = fd;
        }
    }
}

BitcaskEngine::~BitcaskEngine() {
    stop_auto_merge_.store(true);
    if (auto_merge_thread_.joinable()) {
        auto_merge_thread_.join();
    }
}

bool BitcaskEngine::init(const std::string& db_dir) {
    // 1. 先创建总的根目录
    if (mkdir(db_dir.c_str(), 0777) != 0 && errno != EEXIST) {
        return false;
    }
    
    std::cout << "\n准备并行初始化 16 个底层存储分片...\n";
    // 2. 依次实例化并初始化 16 个分片
    for (int i = 0; i < SHARD_NUM; ++i) {
        shards_[i] = std::make_unique<BitcaskShard>(i);
        if (!shards_[i]->init(db_dir)) {
            return false; // 如果有任何一个分片挂了，初始化宣告失败
        }
    }
    stop_auto_merge_.store(false);
    if (!auto_merge_thread_.joinable()) {
        auto_merge_thread_ = std::thread(&BitcaskEngine::auto_merge_loop, this);
    }
    return true;
}

// 🌟 对外透明的极速分发
int BitcaskEngine::set(const std::string& key, const std::string& value) {
    int idx = std::hash<std::string>{}(key) % SHARD_NUM;
    return shards_[idx]->set(key, value);
}

int BitcaskEngine::get(const std::string& key, std::string& value) {
    int idx = std::hash<std::string>{}(key) % SHARD_NUM;
    return shards_[idx]->get(key, value);
}

int BitcaskEngine::del(const std::string& key) {
    int idx = std::hash<std::string>{}(key) % SHARD_NUM;
    return shards_[idx]->del(key);
}

void BitcaskEngine::merge() {
    std::lock_guard<std::mutex> guard(merge_mutex_);
    std::cout << "\n触发全局后台垃圾回收，开始依次处理所有分片...\n";
    // 工业级环境这里甚至可以用多线程同时 merge
    for (int i = 0; i < SHARD_NUM; ++i) {
        shards_[i]->merge();
    }
}

void BitcaskEngine::auto_merge_loop() {
    while (!stop_auto_merge_.load()) {
        std::this_thread::sleep_for(std::chrono::seconds(AUTO_MERGE_INTERVAL_SEC));
        if (stop_auto_merge_.load()) break;
        merge();
    }
}

} // namespace storage
