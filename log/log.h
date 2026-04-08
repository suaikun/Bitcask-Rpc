#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <iostream>
#include <string>
#include <stdarg.h>
#include <thread>
#include <memory>
#include "block_queue.h"

class Log {
public:
    static Log* getInstance() {
        static Log instance;
        return &instance;
    }

    // 初始化接口：文件名、是否关闭日志、队列大小
    bool init(const char* file_name, int close_log, int max_queue_size = 1000);

    // 供外部调用的写日志接口
    void write_log(int level, const char* format, ...);

    // 强制刷新缓冲区
    void flush();

private:
    Log();
    virtual ~Log();

    // 5. 后台专门写硬盘的线程函数
    void async_write_log();

private:
    char m_dir_name[128]; // 路径名
    char m_log_name[128]; // log文件名
    int m_close_log;      // 日志开关 (1代表关闭，0代表开启)
    
    int m_today;          // 记录当前时间是哪一天
    FILE* m_fp;           // 打开的 log 文件指针
    
    std::unique_ptr<BlockQueue<std::string>> m_log_queue; // 阻塞队列指针
    std::mutex m_mutex;                   // 保护写入操作的锁
};

#define LOG_DEBUG(format, ...) { Log::getInstance()->write_log(0, format, ##__VA_ARGS__); }
#define LOG_INFO(format, ...)  { Log::getInstance()->write_log(1, format, ##__VA_ARGS__); }
#define LOG_WARN(format, ...)  { Log::getInstance()->write_log(2, format, ##__VA_ARGS__); }
// 只有 ERROR 级别才触发强制刷盘，防止进程崩溃时丢失致命报错
#define LOG_ERROR(format, ...) { Log::getInstance()->write_log(3, format, ##__VA_ARGS__); Log::getInstance()->flush(); }

#endif