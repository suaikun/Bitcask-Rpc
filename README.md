# Bitcask RPC

一个基于 `C++14` 实现的高性能 KV 存储服务，采用 `epoll + 线程池 + Protobuf RPC + Bitcask` 架构，支持高并发 `Set/Get`，并提供可直接运行的压测客户端。

这个项目适合放在简历中，重点体现你在网络编程、并发控制、存储引擎和性能优化上的综合能力。

## 项目亮点

- 高性能网络层：`epoll` 非阻塞 I/O，监听 LT + 连接 ET，结合 `EPOLLONESHOT` 避免同一连接被并发处理。
- 自研 RPC 协议：`4 字节长度头 + Protobuf`，支持请求 ID、服务名、方法名、二进制 payload。
- 多线程处理模型：主线程负责事件分发，线程池异步执行业务逻辑，降低主循环阻塞。
- Bitcask 落盘引擎：
  - Append-Only 写入，减少随机写放大。
  - 16 分片（shard）并发，按 key 哈希路由。
  - 内存索引 + Hint 文件加速恢复。
  - CRC32 校验保证数据完整性。
  - Tombstone 删除标记 + merge 压缩回收旧数据。
- 连接生命周期管理：最小堆定时器 + O(1) 映射表，支持连接超时自动清理。
- 异步日志系统：阻塞队列 + 后台线程写盘，错误日志可强制 `flush`。

## 架构设计

1. Client 按 RPC 协议发送请求（Set/Get）。
2. Server 主线程通过 `epoll_wait` 监听事件。
3. 读事件触发后将连接任务投递到线程池。
4. 工作线程解析 RPC，调用 Bitcask 引擎执行 `set/get`。
5. 响应序列化后写回客户端。

## 存储引擎设计（Bitcask）

- 数据文件：`*.data`，记录格式为 `crc + ts + tombstone + key_size + val_size + key + value`。
- Hint 文件：`*.hint`，用于快速重建内存索引，减少冷启动扫描成本。
- 分片目录：数据按 `shard_0 ~ shard_15` 分目录存储。
- 文件轮转：单文件达到阈值（5MB）后自动 rotate。
- 读取流程：按索引定位 value，`pread` 读取并二次 CRC 校验。
- 合并流程：将仍有效的旧记录重写到新文件，更新索引并删除历史文件。

## 协议定义

协议文件：[proto/rpc_meta.proto](proto/rpc_meta.proto)

- `RpcMessage`
  - `request_id`
  - `service_name`（当前为 `KVStore`）
  - `method_name`（`Set` / `Get`）
  - `payload`（具体请求体）
- 业务消息：
  - `SetRequest/SetResponse`
  - `GetRequest/GetResponse`

## 项目结构

```text
.
├─main.cpp                # 程序入口
├─webserver.*             # 网络框架与事件循环
├─rpc/rpc_conn.*          # 连接对象、RPC 编解码、业务分发
├─storage/bitcask.*       # Bitcask 分片引擎
├─threadpool/threadpool.h # 通用线程池模板
├─timer/timer.*           # 小根堆定时器
├─log/                    # 异步日志模块
├─buffer.h                # 动态读写缓冲区
├─client.cpp              # 功能验证客户端
└─client_bench.cpp        # 压测客户端
```

## 环境依赖

- Linux（推荐 Ubuntu 20.04+/22.04）
- `g++`（支持 C++14）
- `pthread`
- `protobuf`（包含 `libprotobuf` 与 `protoc`）

## 编译与运行

### 1. 编译服务端

```bash
make
```

生成可执行文件：`./server`

### 2. 启动服务

```bash
./server -p 9006 -t 8 -c 0
```

参数说明：

- `-p`：端口（默认 `9006`）
- `-t`：线程池线程数（默认 `8`）
- `-c`：日志开关（`0` 开启日志，`1` 关闭日志）

### 3. 编译并运行功能客户端

```bash
g++ -std=c++14 -O2 -Wall -g -pthread -I. client.cpp proto/rpc_meta.pb.cc -o client -lprotobuf
./client
```

### 4. 编译并运行压测客户端

```bash
g++ -std=c++14 -O2 -Wall -g -pthread -I. client_bench.cpp proto/rpc_meta.pb.cc -o bench_client -lprotobuf
./bench_client
```

## 压测结果

以下结果来自项目截图中的本地压测（`10 万请求`）：

| 场景 | 总耗时 | 成功/失败 | QPS | 平均延迟 |
|---|---:|---:|---:|---:|
| Set（写入磁盘） | 3250 ms | 100000 / 0 | 30769.2 req/s | 0.0325 ms/req |
| Get（读取数据） | 3321 ms | 100000 / 0 | 30111.4 req/s | 0.03321 ms/req |

额外观测值（截图中显示）：`31191.5 req/s`，平均延迟 `0.03206 ms/req`。
