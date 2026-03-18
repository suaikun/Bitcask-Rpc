#ifndef TIMER_H
#define TIMER_H

#include <vector>
#include <chrono>
#include <functional>
#include <algorithm>
#include <assert.h>

// 使用 C++11 的 chrono 库获取高精度绝对时间
typedef std::chrono::high_resolution_clock::time_point TimeStamp;
typedef std::chrono::milliseconds MS;
typedef std::function<void()> TimeoutCallBack;

// 定时器节点
struct TimerNode {
    int id;             // 用来存放 sockfd
    TimeStamp expires;  // 绝对超时时间点
    TimeoutCallBack cb; // 超时后执行的回调函数（比如关闭连接）
    
    // 重载 < 运算符，为了建最小堆
    bool operator<(const TimerNode& t) const {
        return expires < t.expires;
    }
};

class HeapTimer {
public:
    HeapTimer();
    ~HeapTimer();

    // 核心接口
    void add(int id, int timeout_ms, const TimeoutCallBack& cb); // 添加新定时器
    void adjust(int id, int timeout_ms);                         // 活跃连接续命
    void popNode(int id);                                        // 主动删除某个定时器
    void tick();                                                 // 清理所有已超时的节点
    int getNextTick();                                           // 获取下一个超时还要等多久（给 epoll 用的）
    void clear();

private:
    void del_(size_t i);
    void siftup_(size_t i);
    bool siftdown_(size_t index, size_t n);
    void swapNode_(size_t i, size_t j);

    std::vector<TimerNode> heap_;      // 最小堆底层数组
    int ref_[65536];                   // 极其关键：映射 sockfd 到 heap_ 数组的索引号，实现 O(1) 查找
};

#endif