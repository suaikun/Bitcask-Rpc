#ifndef BITCASK_H
#define BITCASK_H

#include <string>
#include <unordered_map>
#include <mutex>
#include <shared_mutex>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>
#include <dirent.h>
#include <vector>
#include <memory>
#include <chrono>
#include <cstdint>
#include <functional> // 引入 std::hash
#include <thread>
#include <atomic>

namespace storage {

inline uint32_t crc32_checksum(const char* data, size_t length) {
    uint32_t crc = 0xFFFFFFFF;
    for (size_t i = 0; i < length; ++i) {
        crc ^= (uint8_t)data[i];
        for (int j = 0; j < 8; ++j) {
            crc = (crc >> 1) ^ (0xEDB88320 & (-(crc & 1)));
        }
    }
    return ~crc;
}

struct IndexNode {
    int file_id;      
    off_t val_pos;    
    size_t val_size;  
    uint64_t ts;      
};

// =========================================================
// 🌟 核心升级 1：单分片处理单元 (封装你所有的完备底层逻辑)
// =========================================================
class BitcaskShard {
public:
    BitcaskShard(int shard_id) : shard_id_(shard_id), active_fd_(-1), active_file_id_(1), current_offset_(0) {}
    ~BitcaskShard();

    bool init(const std::string& base_dir);
    int set(const std::string& key, const std::string& value);
    int get(const std::string& key, std::string& value);
    int del(const std::string& key);
    void merge(); 

private:
    void rotate_active_file();
    static bool write_all(int fd, const void* buf, size_t len);
    static bool pread_all(int fd, void* buf, size_t len, off_t offset);
    static bool file_exists(const std::string& path);
    int next_free_file_id(int start_id) const;
    
    uint64_t get_timestamp() {
        return std::chrono::duration_cast<std::chrono::milliseconds>(
            std::chrono::system_clock::now().time_since_epoch()).count();
    }
    int append_record(const std::string& key, const std::string& value, uint8_t tombstone);

private:
    int shard_id_;           // 当前分片的编号
    std::string db_dir_;     // 当前分片的专属物理目录
    int active_fd_;                    
    int active_file_id_;               
    off_t current_offset_;             
    
    static const off_t MAX_FILE_SIZE = 5 * 1024 * 1024; 
    static const size_t DATA_HEADER_SIZE = 21;
    static const size_t HINT_HEADER_SIZE = 25;

    std::unordered_map<int, int> old_fds_; 
    std::unordered_map<std::string, IndexNode> index_; 
    std::shared_timed_mutex rw_mutex_; // 🌟 每个分片拥有自己独立的锁！
};

// =========================================================
// 🌟 核心升级 2：透明路由网关 (对外接口完全不变)
// =========================================================
class BitcaskEngine {
public:
    static BitcaskEngine* GetInstance() {
        static BitcaskEngine instance;
        return &instance;
    }

    bool init(const std::string& db_dir);
    int set(const std::string& key, const std::string& value);
    int get(const std::string& key, std::string& value);
    int del(const std::string& key);
    void merge(); 

private:
    BitcaskEngine() {}
    ~BitcaskEngine();
    void auto_merge_loop();

    static const int SHARD_NUM = 16; // 划分为 16 个并发分片桶
    static const int AUTO_MERGE_INTERVAL_SEC = 30;
    std::unique_ptr<BitcaskShard> shards_[SHARD_NUM];
    std::thread auto_merge_thread_;
    std::atomic<bool> stop_auto_merge_{false};
    std::mutex merge_mutex_;
};

} // namespace storage

#endif
