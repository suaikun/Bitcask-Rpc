#include "config.h"
#include "webserver.h"
#include "./storage/bitcask.h"


int main(int argc, char* argv[]) {
    Config cfg;
    cfg.parse_arg(argc, argv);

    if (!storage::BitcaskEngine::GetInstance()->init("bitcask_data.db")) {
        std::cerr << "致命错误: Bitcask 引擎初始化失败!\n";
        return -1;
    }
    std::cout << "✅ Bitcask 磁盘存储引擎启动成功!\n";

    WebServer server;

    //初始化
    server.init(cfg.PORT, cfg.THREAD_NUM, cfg.LOG_OPEN);

    // 日志系统
    server.log_write();

    //触发模式
    server.trig_mode();

    //监听
    server.eventListen();

    //运行
    server.eventLoop();
    return 0;
}

