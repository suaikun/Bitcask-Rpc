#include "rpc_conn.h"
#include "../log/log.h"
#include "../proto/rpc_meta.pb.h"
#include "../storage/bitcask.h"

int rpc_conn::m_epollfd = -1;
int rpc_conn::m_user_count = 0;

// 1. 设置文件描述符非阻塞
void setnonblocking(int fd) {
    int old_option = fcntl(fd, F_GETFL);
    int new_option = old_option | O_NONBLOCK;
    fcntl(fd, F_SETFL, new_option);
}

// 2. 将文件描述符注册到 epoll 内核事件表
void addfd(int epollfd, int fd, bool one_shot, int trigMode) {
    epoll_event event;
    event.data.fd = fd;
    if (trigMode == 1) // ET 模式
        event.events = EPOLLIN | EPOLLET | EPOLLRDHUP;
    else               // LT 模式
        event.events = EPOLLIN | EPOLLRDHUP;

    if (one_shot) event.events |= EPOLLONESHOT;
    epoll_ctl(epollfd, EPOLL_CTL_ADD, fd, &event);
    setnonblocking(fd);
}

// 3. 从 epoll 中删除文件描述符
void removefd(int epollfd, int fd) {
    epoll_ctl(epollfd, EPOLL_CTL_DEL, fd, 0);
    close(fd);
}

// 4. 修改 epoll 监听事件
void modfd(int epollfd, int fd, int ev, int trigMode) {
    epoll_event event;
    event.data.fd = fd;
    if (trigMode == 1) // ET 模式
        event.events = ev | EPOLLET | EPOLLONESHOT | EPOLLRDHUP;
    else               // LT 模式
        event.events = ev | EPOLLONESHOT | EPOLLRDHUP;
    epoll_ctl(epollfd, EPOLL_CTL_MOD, fd, &event);
}

void rpc_conn::init(int sockfd, const sockaddr_in& addr, int trigMode) {
    m_sockfd = sockfd;
    m_address = addr;
    m_trigMode = trigMode;
    addfd(m_epollfd, sockfd, true, m_trigMode);
    m_user_count++;
    readBuffer_.RetrieveAll();
    writeBuffer_.RetrieveAll();
}

void rpc_conn::close_conn(bool real_close) {
    if (real_close && (m_sockfd != -1)) {
        removefd(m_epollfd, m_sockfd);
        m_sockfd = -1;
        m_user_count--;
    }
}

bool rpc_conn::read() {
    int err = 0;
    // ET 模式下必须循环读，直到读干净 (EAGAIN)
    while (true) {
        ssize_t bytes = readBuffer_.ReadFd(m_sockfd, &err);
        if (bytes < 0) {
            if (err == EAGAIN || err == EWOULDBLOCK) {
                break; // 内核没数据了，完美退出
            }
            return false; // 真正出错
        } else if (bytes == 0) {
            return false; // 对端关闭连接
        }
    }
    return true;
}

// 极其清爽的非阻塞写
bool rpc_conn::write() {
    int err = 0;
    if (writeBuffer_.ReadableBytes() == 0) {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_trigMode);
        return true;
    }
    
    ssize_t bytes = writeBuffer_.WriteFd(m_sockfd, &err);
    if (bytes < 0) {
        if (err == EAGAIN) { // 缓冲区满了，下次 epoll_wait 继续写
            modfd(m_epollfd, m_sockfd, EPOLLOUT, m_trigMode);
            return true;
        }
        return false;
    }
    
    // 写完了
    if (writeBuffer_.ReadableBytes() == 0) {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_trigMode);
        return true;
    }
    return true;
}

void rpc_conn::process() {
    while (readBuffer_.ReadableBytes() >= 4) {
        
        uint32_t message_len = 0;
        memcpy(&message_len, readBuffer_.Peek(), 4);

        // 半包判断：数据不够一个完整的包，等下一次 EPOLLIN
        if (readBuffer_.ReadableBytes() < 4 + message_len) {
            break; 
        }

        // 提取出一个完整的 protobuf 二进制流
        readBuffer_.Retrieve(4); 
        std::string buffer_data(readBuffer_.Peek(), message_len);
        readBuffer_.Retrieve(message_len); 

        // ---------------------------------------------------------
        // 1. 反序列化外层 RpcMessage (拆快递盒)
        // ---------------------------------------------------------
        myrpc::RpcMessage rpc_req;
        if (!rpc_req.ParseFromString(buffer_data)) {
            LOG_ERROR("RPC Meta Parse Failed!");
            continue; // 解析失败，丢弃这个包
        }

        uint64_t req_id = rpc_req.request_id();
        std::string service = rpc_req.service_name();
        std::string method = rpc_req.method_name();

        // 准备一个用于返回的空快递盒
        myrpc::RpcMessage rpc_resp;
        rpc_resp.set_request_id(req_id);
        rpc_resp.set_service_name(service);
        rpc_resp.set_method_name(method);

        // ---------------------------------------------------------
        // 2. RPC 路由分发机制 (真正连接磁盘引擎)
        // ---------------------------------------------------------
        if (service == "KVStore" && method == "Get") {
            myrpc::GetRequest get_req;
            get_req.ParseFromString(rpc_req.payload());
            myrpc::GetResponse get_resp;
            
            std::string value;
            // 🌟 核心：真正去磁盘里拿数据
            int ret = storage::BitcaskEngine::GetInstance()->get(get_req.key(), value);
            if (ret == 0) {
                get_resp.set_status(0);
                get_resp.set_value(value);
            } else {
                get_resp.set_status(-1);
                get_resp.set_value("磁盘中未找到该 Key");
            }

            std::string resp_payload;
            get_resp.SerializeToString(&resp_payload);
            rpc_resp.set_payload(resp_payload);
            
        } else if (service == "KVStore" && method == "Set") {
            // 🌟 核心：处理写入磁盘请求
            myrpc::SetRequest set_req;
            set_req.ParseFromString(rpc_req.payload());
            myrpc::SetResponse set_resp;

            // 调用 Bitcask 的 O_APPEND 极速落盘
            int ret = storage::BitcaskEngine::GetInstance()->set(set_req.key(), set_req.value());
            set_resp.set_status(ret);

            std::string resp_payload;
            set_resp.SerializeToString(&resp_payload);
            rpc_resp.set_payload(resp_payload);
            
        } else {
            LOG_WARN("Unknown Service or Method: %s.%s", service.c_str(), method.c_str());
        }
        // ---------------------------------------------------------
        // 3. 序列化整体响应，准备通过 TCP 发送
        // ---------------------------------------------------------
        std::string final_bytes;
        rpc_resp.SerializeToString(&final_bytes);
        uint32_t final_len = final_bytes.size();

        // 组装 TCP 报文：【4字节长度】 + 【Protobuf二进制流】
        writeBuffer_.Append((const char*)&final_len, 4);
        writeBuffer_.Append(final_bytes.data(), final_bytes.size());
    }

    // 处理完后触发 epoll_wait 去把写缓冲区的数据发出去
    if (writeBuffer_.ReadableBytes() > 0) {
        modfd(m_epollfd, m_sockfd, EPOLLOUT, m_trigMode);
    } else {
        modfd(m_epollfd, m_sockfd, EPOLLIN, m_trigMode);
    }
}