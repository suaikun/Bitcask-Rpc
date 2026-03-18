#include "config.h"
#include <cstdlib>
#include <getopt.h>

Config::Config() : PORT(9006), THREAD_NUM(8),LOG_OPEN(0) {} 
void Config::parse_arg(int argc, char* argv[]) {
    int opt;
    const char* str = "p:t:c:"; 
    while ((opt = getopt(argc, argv, str)) != -1) {
        switch (opt) {
        case 'p':
            PORT = std::atoi(optarg);
            break;
        case 't': 
            THREAD_NUM = std::atoi(optarg);
            break;
        case 'c':
            LOG_OPEN = std::atoi(optarg);
            break;
        default:
            break;
        }
    }
}