#ifndef BUFFER_H
#define BUFFER_H

#include <vector>
#include <atomic>
#include <string>
#include <sys/uio.h> 
#include <unistd.h>  
#include <assert.h>
#include <errno.h>

class Buffer {
public:
    Buffer(int initBuffSize = 1024) : buffer_(initBuffSize), readPos_(0), writePos_(0) {}
    ~Buffer() = default;

    // 可读的数据字节数 (writePos - readPos)
    size_t ReadableBytes() const { return writePos_ - readPos_; }
    
    // 可写的剩余空间字节数 (buffer.size() - writePos)
    size_t WritableBytes() const { return buffer_.size() - writePos_; }

    // 返回当前可读数据的起始地址
    const char* Peek() const { return BeginPtr_() + readPos_; }

    // 取出指定长度的数据后，移动读指针
    void Retrieve(size_t len) {
        assert(len <= ReadableBytes());
        readPos_ += len;
    }

    // 清空缓冲区
    void RetrieveAll() {
        bzero(&buffer_[0], buffer_.size());
        readPos_ = 0;
        writePos_ = 0;
    }

    // 往缓冲区追加数据
    void Append(const char* str, size_t len) {
        EnsureWriteable(len); // 确保空间足够，不够会自动扩容
        std::copy(str, str + len, BeginWrite());
        HasWritten(len);
    }

    char* BeginWrite() { return BeginPtr_() + writePos_; }
    void HasWritten(size_t len) { writePos_ += len; }

    //分散读：从 socket 读数据到 Buffer 中
    ssize_t ReadFd(int fd, int* saveErrno)
    {
        char extrabuf[65536]; // 栈上的临时空间 (64KB)
        struct iovec iov[2];
        const size_t writable = WritableBytes();
        
        // 分散读配置：第一部分是 Buffer 目前剩余的可写空间
        iov[0].iov_base = BeginPtr_() + writePos_;
        iov[0].iov_len = writable;
        // 分散读配置：第二部分是栈上的临时空间
        iov[1].iov_base = extrabuf;
        iov[1].iov_len = sizeof(extrabuf);
    
        // 如果 Buffer 剩余空间足够大，就不需要临时空间了
        const int iovcnt = (writable < sizeof(extrabuf)) ? 2 : 1;
        const ssize_t len = readv(fd, iov, iovcnt);
    
        if (len < 0) {
            *saveErrno = errno;
        } 
        else if (static_cast<size_t>(len) <= writable) {
            // 数据完全装入了现有的 Buffer 中
            writePos_ += len;
        } 
        else {
            // Buffer 被装满了，剩余的数据装在了 extrabuf 中
            writePos_ = buffer_.size(); // Buffer 读满
            // 将溢出在 extrabuf 的数据追加到 Buffer 
            Append(extrabuf, len - writable); 
        }
        return len;
    }

    // 从 Buffer 写数据到 socket
    ssize_t WriteFd(int fd, int* saveErrno)
    {
        size_t readSize = ReadableBytes();
        ssize_t len = write(fd, Peek(), readSize);
        if (len < 0) {
            *saveErrno = errno;
            return len;
        } 
        readPos_ += len; // 成功发送后，移动读指针
        return len;
    }

private:
    char* BeginPtr_() { return &*buffer_.begin(); }
    const char* BeginPtr_() const { return &*buffer_.begin(); }
    
    // 扩容逻辑
    void EnsureWriteable(size_t len) {
        if(WritableBytes() < len) {
            MakeSpace_(len);
        }
    }
    
    void MakeSpace_(size_t len) {
        // 如果前面空闲的部分 + 后面可写的部分 < 需要的空间，直接 resize
        if(WritableBytes() + readPos_ < len) {
            buffer_.resize(writePos_ + len + 1);
        } else {
            // 否则把数据挪到最前面，腾出空间
            size_t readable = ReadableBytes();
            std::copy(BeginPtr_() + readPos_, BeginPtr_() + writePos_, BeginPtr_());
            readPos_ = 0;
            writePos_ = readPos_ + readable;
            assert(readable == ReadableBytes());
        }
    }

    std::vector<char> buffer_;
    std::atomic<std::size_t> readPos_;
    std::atomic<std::size_t> writePos_;
};

#endif