CXX = g++
CFLAGS = -std=c++14 -O2 -Wall -g -pthread -I.
LIBS = -lprotobuf

TARGET = server

SRCS = main.cpp \
       config.cpp \
       webserver.cpp \
       rpc/rpc_conn.cpp \
       timer/timer.cpp \
       log/log.cpp \
       storage/bitcask.cpp \
       proto/rpc_meta.pb.cc

all: $(TARGET)

$(TARGET): $(SRCS)
	$(CXX) $(CFLAGS) $(SRCS) -o $(TARGET) $(LIBS)

clean:
	rm -rf $(TARGET) *.o *.log