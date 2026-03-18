#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <list>
#include <mutex>
#include <condition_variable>
#include <thread>
#include <vector>
#include <exception>

// 模板类，T 代表任务类
template <typename T>
class ThreadPool {
public:
    // thread_number: 线程池中预先创建的线程数量
    // max_requests: 请求队列中最多允许排队的请求数量，防止内存撑爆
    ThreadPool(int thread_number = 8, int max_requests = 10000);
    ~ThreadPool();
    
    // 向请求队列中添加任务（主线程调用）
    bool append(T* request);

private:
    // 工作线程运行的核心循环函数
    void run();

private:
    int m_thread_number;                 // 线程池中的线程数
    int m_max_requests;                  // 请求队列的最大容量
    std::vector<std::thread> m_threads;  // 存放线程对象的数组
    std::list<T*> m_workqueue;           // 请求队列
    std::mutex m_queuelocker;            // 保护请求队列的互斥锁
    std::condition_variable m_cond;      // 条件变量：用于线程休眠与唤醒
    bool m_stop;                         // 是否结束线程的标志位
};

// --- 构造函数 ---
template <typename T>
ThreadPool<T>::ThreadPool(int thread_number, int max_requests) : 
    m_thread_number(thread_number), m_max_requests(max_requests), m_stop(false) 
{
    if (thread_number <= 0 || max_requests <= 0) {
        throw std::exception();
    }

    // 提前创建好所有线程
    for (int i = 0; i < thread_number; ++i) {
        // 使用 C++11 的 lambda 表达式启动 run 函数
        m_threads.emplace_back([this]() {
            this->run();
        });
    }
}

// --- 析构函数 ---
template <typename T>
ThreadPool<T>::~ThreadPool() {
    {
        std::unique_lock<std::mutex> lock(m_queuelocker);
        m_stop=true;
    }
    m_cond.notify_all(); // 唤醒所有正在睡觉的线程，让它们安全退出死循环

    for (auto& thread : m_threads) {
        if (thread.joinable()) {
            thread.join(); 
        }
    }
}

// --- 扔任务（生产者） ---
template <typename T>
bool ThreadPool<T>::append(T* request) {
    // std::unique_lock 会自动加锁，并在离开作用域时自动解锁
    std::unique_lock<std::mutex> lock(m_queuelocker);
    
    if (m_workqueue.size() >= m_max_requests) {
        return false; // 队列满了，拒接新客
    }
    
    m_workqueue.push_back(request);
    lock.unlock();       // 放进队列后就可以提前解锁了，别一直占着
    m_cond.notify_one(); // 唤醒一个正在休眠的工作线程来干活
    
    return true;
}

// --- 抢任务干活（消费者） ---
template <typename T>
void ThreadPool<T>::run() {
    while (!m_stop) {
        std::unique_lock<std::mutex> lock(m_queuelocker);
        
        // 一旦被 notify 唤醒，会重新加锁并检查队列是不是真的有东西
        m_cond.wait(lock, [this]() {
            return !m_workqueue.empty() || m_stop;
        });

        if (m_stop && m_workqueue.empty()) {
            break;
        }

        if (m_workqueue.empty()) {
            continue;
        }

        // 抢走队列最前面的任务
        T* request = m_workqueue.front();
        m_workqueue.pop_front();
        
        // 拿到任务后赶紧解锁，让其他兄弟也能去队列里拿任务
        lock.unlock();

        if (!request) {
            continue;
        }
        
        // 核心步骤：处理具体的业务逻辑！
        request->process(); 
    }
}

#endif