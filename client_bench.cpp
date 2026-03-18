#include <iostream>
#include <vector>
#include <thread>
#include <chrono>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <string.h>
#include <atomic>
#include "./proto/rpc_meta.pb.h"

using namespace std;
using namespace std::chrono;

// --- 压测参数配置 ---
const int NUM_THREADS = 50;            // 并发线程数（模拟 10 个客户端）
const int REQUESTS_PER_THREAD = 4000; // 每个客户端发送的请求数
// 总请求数 = 10 * 10000 = 100,000 次

// 统计成功和失败的请求数
atomic<int> success_count(0);
atomic<int> fail_count(0);

// 辅助封装：发送 RPC 请求并接收返回值
bool CallRpc(int sockfd, const string& method, const string& payload_str, string& resp_payload) {
    myrpc::RpcMessage rpc_req;
    rpc_req.set_request_id(rand() % 10000);
    rpc_req.set_service_name("KVStore");
    rpc_req.set_method_name(method);
    rpc_req.set_payload(payload_str);

    string send_data;
    rpc_req.SerializeToString(&send_data);
    
    uint32_t len = send_data.size();
    
    // 🌟 优化：拼接包头和包体，一次性发给内核，绝不触发 Nagle 延迟！
    string packet;
    packet.append((char*)&len, 4);
    packet.append(send_data);
    
    if (write(sockfd, packet.data(), packet.size()) != packet.size()) return false;

    uint32_t resp_len = 0;
    if (read(sockfd, &resp_len, 4) != 4) return false;
    
    char recv_buf[4096] = {0};
    int total_read = 0;
    while (total_read < resp_len) {
        int n = read(sockfd, recv_buf + total_read, resp_len - total_read);
        if (n <= 0) return false;
        total_read += n;
    }

    myrpc::RpcMessage rpc_resp;
    rpc_resp.ParseFromArray(recv_buf, resp_len);
    resp_payload = rpc_resp.payload();
    return true;
}

// 压测工作线程函数
void benchmark_worker(int thread_id, bool is_write) {
    // 1. 每个线程建立自己的独立连接
    int sockfd = socket(AF_INET, SOCK_STREAM, 0);
    struct sockaddr_in servaddr;
    servaddr.sin_family = AF_INET;
    servaddr.sin_port = htons(9006);
    inet_pton(AF_INET, "127.0.0.1", &servaddr.sin_addr);
    
    if (connect(sockfd, (struct sockaddr*)&servaddr, sizeof(servaddr)) < 0) {
        cerr << "线程 " << thread_id << " 连接失败！\n";
        return;
    }

    // 2. 疯狂发送请求
    for (int i = 0; i < REQUESTS_PER_THREAD; ++i) {
        // 构造独一无二的 Key，例如 "key_3_456" (线程3的第456个key)
        string key = "key_" + to_string(thread_id) + "_" + to_string(i);
        string payload_str, resp_payload;

        if (is_write) {
            myrpc::SetRequest set_req;
            set_req.set_key(key);
            set_req.set_value("benchmark_value_data_" + to_string(i));
            set_req.SerializeToString(&payload_str);
            
            if (CallRpc(sockfd, "Set", payload_str, resp_payload)) {
                myrpc::SetResponse set_resp;
                set_resp.ParseFromString(resp_payload);
                if (set_resp.status() == 0) success_count++;
                else fail_count++;
            } else fail_count++;
        } else {
            myrpc::GetRequest get_req;
            get_req.set_key(key);
            get_req.SerializeToString(&payload_str);
            
            if (CallRpc(sockfd, "Get", payload_str, resp_payload)) {
                myrpc::GetResponse get_resp;
                get_resp.ParseFromString(resp_payload);
                if (get_resp.status() == 0) success_count++;
                else fail_count++;
            } else fail_count++;
        }
    }
    close(sockfd);
}

void run_benchmark(const string& test_name, bool is_write) {
    success_count = 0;
    fail_count = 0;
    cout << "\n🚀 开始 " << test_name << " 压测 (10万次请求)...\n";
    
    auto start_time = high_resolution_clock::now();

    vector<thread> threads;
    for (int i = 0; i < NUM_THREADS; ++i) {
        threads.emplace_back(benchmark_worker, i, is_write);
    }

    for (auto& t : threads) {
        t.join();
    }

    auto end_time = high_resolution_clock::now();
    auto duration = duration_cast<milliseconds>(end_time - start_time).count();
    
    double qps = (success_count + fail_count) * 1000.0 / duration;
    
    cout << "----------------------------------------\n";
    cout << "📊 测试结果: " << test_name << "\n";
    cout << "总耗时: " << duration << " ms\n";
    cout << "成功数: " << success_count << " | 失败数: " << fail_count << "\n";
    cout << "🔥 QPS : " << qps << " req/sec\n";
    cout << "平均延迟: " << (double)duration / (success_count + fail_count) << " ms/req\n";
    cout << "----------------------------------------\n";
}

int main() {
    cout << "====== Bitcask RPC 极限压测工具 ======\n";
    
    // 1. 先测疯狂写入 10万 次
    run_benchmark("Set (写入磁盘)", true);

    // 等待 2 秒，让服务器稍微喘口气
    std::this_thread::sleep_for(std::chrono::seconds(2));

    // 2. 再测疯狂读取刚才写入的 10万 个 Key
    run_benchmark("Get (读取数据)", false);

    return 0;
}