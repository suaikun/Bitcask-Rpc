#ifndef RPC_CONN_H
#define RPC_CONN_H

#include <unistd.h>
#include <sys/epoll.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <assert.h>
#include <string.h>
#include <errno.h>
#include <iostream>
#include "../buffer.h"  // 引入你刚写的核武器

class rpc_conn {
public:
    rpc_conn() {}
    ~rpc_conn() {}

    void init(int sockfd, const sockaddr_in& addr, int trigMode);
    void close_conn(bool real_close = true);
    
    // 工作线程核心逻辑：解析 RPC 报文
    void process(); 
    
    // I/O 读取和发送
    bool read();
    bool write();

public:
    static int m_epollfd;
    static int m_user_count;

private:
    int m_sockfd;
    sockaddr_in m_address;
    int m_trigMode;

    // 彻底抛弃 char m_read_buf[2048];
    Buffer readBuffer_;  
    Buffer writeBuffer_; 
};

#endif