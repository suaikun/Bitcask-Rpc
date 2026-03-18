#ifndef WEBSERVER_H
#define WEBSERVER_H

#include <sys/epoll.h>
#include <arpa/inet.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <string>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cassert>
#include <signal.h>
#include <csignal>
#include <atomic>
#include <memory>
#include "./rpc/rpc_conn.h"
#include "./threadpool/threadpool.h"
#include "./timer/timer.h"
#include "./log/log.h"

const int MAX_FD = 65536;           // 系统最大文件描述符数量 (用于开辟 users 数组)
const int MAX_EVENT_NUMBER = 10000; // epoll 最大能监听的事件数量

class WebServer {
public:
    WebServer();
    ~WebServer();

    void init(int port, int threadnum, int log_open);
    void log_write();
    void trig_mode();
    void eventListen();
    void eventLoop();
    bool dealclientdata();
    void dealwithread(int sockfd);
    void dealwithwrite(int sockfd);
    void CloseConn_(int sockfd);

private:

    int m_port;
    int m_epollfd;

    //日志相关
    int m_log_open;

    //定时器相关
    HeapTimer timer_;
    static const int TIMEOUT_MS = 15000; // 设定15秒不说话踢掉

    //线程池相关
    std::unique_ptr<ThreadPool<rpc_conn>> m_pool;
    int m_thread_num;

    //用户相关
    std::unique_ptr<rpc_conn[]> users;

    //epoll_event相关
    epoll_event events[MAX_EVENT_NUMBER];//epoll内核事件表，用于存储epoll_wait返回的事件

    int m_listenfd;//监听套接字
    int m_LISTENTrigmode;//监听触发模式
    int m_CONNTrigmode;//连接触发模式
};

#endif