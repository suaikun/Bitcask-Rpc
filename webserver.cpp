#include "webserver.h"
#include <netinet/tcp.h> 

std::atomic<bool> g_stop_server(false);

//信号捕捉函数
void sig_int_handler(int sig) {
    if (sig == SIGINT || sig == SIGTERM) {
        g_stop_server = true;
        LOG_WARN("Catch signal %d, server is shutting down gracefully...", sig);
    }
}

// 声明前面定义的辅助函数
extern void addfd(int epollfd, int fd, bool one_shot, int trigMode);
extern void setnonblocking(int fd);


WebServer::WebServer() {
    users = std::make_unique<rpc_conn[]>(MAX_FD);
}

WebServer::~WebServer() {
    close(m_epollfd);
    close(m_listenfd);
}

void WebServer::init(int port, int threadnum, int log_open) 
{
    m_port = port;
    m_thread_num = threadnum;
    m_log_open = log_open; 
    m_pool = std::make_unique<ThreadPool<rpc_conn>>(m_thread_num, 10000);
}

void WebServer::log_write() {
    int close_log = (m_log_open == 1) ? 0 : 1;
    Log::getInstance()->init("Server.log", close_log, 2000);
}

// 只要用到断开连接，就调这个，它会同时清理 TCP 和 定时器
void WebServer::CloseConn_(int sockfd) {
    users[sockfd].close_conn();
    timer_.popNode(sockfd); // 从堆里删掉，防止野指针
}

void WebServer::trig_mode() {
    m_LISTENTrigmode = 0; // LT
    m_CONNTrigmode = 1;   // ET
}

void WebServer::eventListen() {
    m_listenfd = socket(AF_INET, SOCK_STREAM, 0);
    assert(m_listenfd >= 0);

    int opt = 1;
    setsockopt(m_listenfd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    struct sockaddr_in address;
    bzero(&address, sizeof(address));
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(m_port);

    int ret = bind(m_listenfd, (struct sockaddr *)&address, sizeof(address));
    assert(ret >= 0);

    // 监听队列大小已优化为 2048
    ret = listen(m_listenfd, 2048);
    assert(ret >= 0);

    LOG_INFO("listening on port %d ...", m_port);

    m_epollfd = epoll_create(5);
    assert(m_epollfd != -1);
    
    // 初始化 rpc_conn 静态变量
    rpc_conn::m_epollfd = m_epollfd;

    signal(SIGINT, sig_int_handler);  // 捕捉 Ctrl+C
    signal(SIGTERM, sig_int_handler); // 捕捉 kill 命令

    addfd(m_epollfd, m_listenfd, false, m_LISTENTrigmode);

}

bool WebServer::dealclientdata() {
    struct sockaddr_in client_address;
    socklen_t client_addrlength = sizeof(client_address); 
    int connfd = accept(m_listenfd, (struct sockaddr *)&client_address, &client_addrlength);
    if (connfd < 0) {
        LOG_ERROR("accept failed: %s", std::strerror(errno));
        return false;
    }
    if (rpc_conn::m_user_count >= MAX_FD) {
        LOG_ERROR("%s", "internal server busy");
        close(connfd);
        return false;
    }

    int optval = 1;
    setsockopt(connfd, IPPROTO_TCP, TCP_NODELAY, (const void*)&optval, sizeof(int));

    struct linger tmp = {1, 1}; // 开启优雅关闭，最多逗留（等待） 1 秒钟
    setsockopt(connfd, SOL_SOCKET, SO_LINGER, &tmp, sizeof(tmp));
    
    // 初始化新连接
    users[connfd].init(connfd, client_address, m_CONNTrigmode);

    timer_.add(connfd, TIMEOUT_MS, [this, connfd]() {
        CloseConn_(connfd); 
    });
    return true;
}

void WebServer::dealwithread(int sockfd) {
    if (users[sockfd].read()) {
        timer_.adjust(sockfd, TIMEOUT_MS); // 续命 15 秒
        m_pool->append(&users[sockfd]);
    } else {
        CloseConn_(sockfd); // 读失败直接关
    }
}

void WebServer::dealwithwrite(int sockfd) {
    if (users[sockfd].write()) {
        timer_.adjust(sockfd, TIMEOUT_MS); // 写完如果是长连接，续命
    } else {
        CloseConn_(sockfd);
    }
}

void WebServer::eventLoop() {
    while (!g_stop_server) {
        // 从堆顶拿最近的超时时间
        int timeMS = timer_.getNextTick(); 
        
        // 动态传入 timeMS，抛弃 -1
        int number = epoll_wait(m_epollfd, events, MAX_EVENT_NUMBER, timeMS);
        if (number < 0 && errno != EINTR) {
            LOG_ERROR("epoll failure: %s", std::strerror(errno));
            break;
        }

        for (int i = 0; i < number; i++) {
            int sockfd = events[i].data.fd;

            if (sockfd == m_listenfd) {
                dealclientdata();
            } else if (events[i].events & (EPOLLRDHUP | EPOLLHUP | EPOLLERR)) {
                CloseConn_(sockfd);
            } else if (events[i].events & EPOLLIN) {
                dealwithread(sockfd);
            } else if (events[i].events & EPOLLOUT) {
                dealwithwrite(sockfd);
            }
        }
    }
    LOG_INFO("%s", "Stop accepting new connections. Waiting for workers to finish...");
}