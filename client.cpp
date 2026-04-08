#include <iostream>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include "./proto/rpc_meta.pb.h"

// 辅助封装：发送 RPC 请求并接收返回值的函数
void CallRpc(int sockfd, const std::string& method, const std::string& payload_str, std::string& resp_payload) {
    myrpc::RpcMessage rpc_req;
    rpc_req.set_request_id(rand() % 10000);
    rpc_req.set_service_name("KVStore");
    rpc_req.set_method_name(method);
    rpc_req.set_payload(payload_str);

    std::string send_data;
    rpc_req.SerializeToString(&send_data);
    
    uint32_t len = send_data.size();
    write(sockfd, &len, 4);
    write(sockfd, send_data.data(), len);

    uint32_t resp_len = 0;
    read(sockfd, &resp_len, 4);
    char recv_buf[4096] = {0};
    read(sockfd, recv_buf, resp_len);

    myrpc::RpcMessage rpc_resp;
    rpc_resp.ParseFromArray(recv_buf, resp_len);
    resp_payload = rpc_resp.payload();
}

int main() {
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(9006);
    inet_pton(AF_INET, "127.0.0.1", &servaddr.sin_addr);
    
    if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        std::cerr << "连接服务器失败！\n"; return -1;
    }
    std::cout << "成功连接到高性能 RPC 数据库!\n\n";

    // ------------------ 动作 1：写入数据 ------------------
    std::cout << " 准备写入硬盘数据 [Key: offer_2026, Value: ByteDance_Arch_Intern]...\n";
    myrpc::SetRequest set_req;
    set_req.set_key("offer_2026");
    set_req.set_value("ByteDance_Arch_Intern");
    
    std::string set_payload, set_resp_payload;
    set_req.SerializeToString(&set_payload);
    
    CallRpc(sockfd, "Set", set_payload, set_resp_payload);
    
    myrpc::SetResponse set_resp;
    set_resp.ParseFromString(set_resp_payload);
    if(set_resp.status() == 0) std::cout << "落盘成功！\n\n";

    // ------------------ 动作 2：读取刚才写入的数据---------
    std::cout << "准备从硬盘读取 [Key: offer_2026] 的数据...\n";
    myrpc::GetRequest get_req;
    get_req.set_key("offer_2026");

    std::string get_payload, get_resp_payload;
    get_req.SerializeToString(&get_payload);
    
    CallRpc(sockfd, "Get", get_payload, get_resp_payload);

    myrpc::GetResponse get_resp;
    get_resp.ParseFromString(get_resp_payload);
    std::cout << "读取结果: " << get_resp.value() << "\n";

    close(sockfd);
    return 0;
}