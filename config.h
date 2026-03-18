#ifndef CONFIG_H
#define CONFIG_H

class Config {
public:
    Config();
    void parse_arg(int argc, char* argv[]);

    int PORT;
    int THREAD_NUM;
    int LOG_OPEN;  
};

#endif