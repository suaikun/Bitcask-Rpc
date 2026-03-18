#ifndef BLOCK_QUEUE_H
#define BLOCK_QUEUE_H

#include <iostream>
#include <queue>
#include <mutex>
#include <condition_variable>

template <class T>
class BlockQueue {
public:
    BlockQueue(int max_size = 1000) : m_max_size(max_size), m_is_close(false) {}

    ~BlockQueue() {
        close();
    }

    void close() {
        {
            std::unique_lock<std::mutex> lock(m_mutex);
            m_queue = std::queue<T>(); // 清空队列
            m_is_close = true;
        }
        m_cond_consumer.notify_all();
        m_cond_producer.notify_all();
    }

    // 生产者（工作线程）向队列扔日志
    bool push(const T& item) {
        std::unique_lock<std::mutex> lock(m_mutex);
        
        while (m_queue.size() >= (size_t)m_max_size && !m_is_close) {
            m_cond_producer.wait(lock);
        }

        if (m_is_close) return false;

        m_queue.push(item);
        
        //唤醒沉睡的线程
        m_cond_consumer.notify_one();
        return true;
    }

    // 消费者（后台线程）从队列拿日志
    bool pop(T& item) {
        std::unique_lock<std::mutex> lock(m_mutex);
        
        while (m_queue.empty() && !m_is_close) {
            m_cond_consumer.wait(lock);
        }

        if (m_is_close && m_queue.empty()) return false;

        item = m_queue.front();
        m_queue.pop();
        
        m_cond_producer.notify_one();
        return true;
    }

private:
    std::queue<T> m_queue;                
    int m_max_size;                      
    bool m_is_close;                       
    std::mutex m_mutex;                    
    std::condition_variable m_cond_consumer; 
    std::condition_variable m_cond_producer; 
};

#endif