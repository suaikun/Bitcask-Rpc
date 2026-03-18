#include "log.h"
#include <sys/time.h>
#include <string.h>

Log::Log() : m_close_log(0), m_fp(nullptr), m_log_queue(nullptr) {}

Log::~Log() {
    if (m_fp != nullptr) {
        fclose(m_fp);
    }
}

// 异步后台写线程的入口
void Log::async_write_log() {
    std::string single_log;
    // 死循环：疯狂从队列里拿日志，拿不到就睡觉
    while (m_log_queue->pop(single_log)) {
        std::unique_lock<std::mutex> lock(m_mutex);
        fputs(single_log.c_str(), m_fp); // 真正的耗时操作：写硬盘
        fflush(m_fp); // 🌟 核心修改2：由后台线程慢条斯理地刷盘，不占用主线程时间
    }
}

bool Log::init(const char* file_name, int close_log, int max_queue_size) {
    m_close_log = close_log;
    if (m_close_log == 1) return false;

    // 1. 创建阻塞队列
    m_log_queue = std::make_unique<BlockQueue<std::string>>(max_queue_size);
    
    // 2. 启动后台独立写线程 (分离状态，因为它是守护线程)
    std::thread background_thread([this]() {
        this->async_write_log();
    });
    background_thread.detach();

    // 3. 创建日志文件
    time_t t = time(NULL);
    struct tm sys_tm;
    localtime_r(&t, &sys_tm); // 🌟 核心修改3：使用线程安全的 localtime_r
    m_today = sys_tm.tm_mday;

    // 拼接文件名：比如 "2026_03_07_Server.log"
    char log_full_name[256] = {0};
    snprintf(log_full_name, 255, "%d_%02d_%02d_%s", sys_tm.tm_year + 1900, sys_tm.tm_mon + 1, sys_tm.tm_mday, file_name);

    m_fp = fopen(log_full_name, "a");
    if (m_fp == nullptr) {
        return false;
    }
    return true;
}

void Log::write_log(int level, const char* format, ...) {
    struct timeval now = {0, 0};
    gettimeofday(&now, NULL);
    time_t t = now.tv_sec;
    struct tm sys_tm;
    localtime_r(&t, &sys_tm); // 🌟 使用线程安全版本，防止多线程时间错乱

    // 日志级别前缀
    char s[16] = {0};
    switch (level) {
        case 0: strcpy(s, "[debug]:"); break;
        case 1: strcpy(s, "[info]:"); break;
        case 2: strcpy(s, "[warn]:"); break;
        case 3: strcpy(s, "[erro]:"); break;
        default: strcpy(s, "[info]:"); break;
    }

    // 🌟 核心修改4：删除了这里的 m_mutex 锁！
    // 因为 log_str 是局部变量，各自线程写各自的栈内存，互不干扰
    char log_str[2048] = {0};
    // 写入时间头部： 2026-03-07 20:15:30.123456 [info]: 
    int n = snprintf(log_str, 48, "%d-%02d-%02d %02d:%02d:%02d.%06ld %s ",
                     sys_tm.tm_year + 1900, sys_tm.tm_mon + 1, sys_tm.tm_mday,
                     sys_tm.tm_hour, sys_tm.tm_min, sys_tm.tm_sec, now.tv_usec, s);
    
    // 提取可变参数并拼接到尾部
    va_list valst;
    va_start(valst, format);
    // 留出最后放 '\n' 和 '\0' 的两个空位
    int max_len = 2048 - 2 - n; 
    int m = vsnprintf(log_str + n, max_len, format, valst);
    
    if (m < 0) {
        m = 0; 
    } else if (m >= max_len) {
        m = max_len - 1; // 如果超出容量，强行截断，保证不越界
    }
    
    log_str[n + m] = '\n';
    log_str[n + m + 1] = '\0';
    va_end(valst);

    std::string log_str_cpp = log_str;

    // 将拼凑好的字符串扔进阻塞队列，底层 Queue 已经自带锁了，直接 push 即可！
    if (m_log_queue && !m_close_log) {
        m_log_queue->push(log_str_cpp);
    }
}

void Log::flush() {
    std::unique_lock<std::mutex> lock(m_mutex);
    if (m_fp) {
        fflush(m_fp); // 强制让操作系统把内核文件缓冲区的数据刷进物理硬盘
    }
}