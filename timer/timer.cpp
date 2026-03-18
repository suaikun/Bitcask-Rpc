#include "timer.h"

HeapTimer::HeapTimer() { 
    heap_.reserve(64); 
    std::fill_n(ref_, 65536, -1); // 初始化所有映射为 -1
}

HeapTimer::~HeapTimer() { 
    clear(); 
}

// 核心：交换堆里的两个节点，并同步更新映射表
void HeapTimer::swapNode_(size_t i, size_t j) {
    std::swap(heap_[i], heap_[j]);
    ref_[heap_[i].id] = i;
    ref_[heap_[j].id] = j;
}

// 向上调整（新节点插入到末尾时用）
void HeapTimer::siftup_(size_t i) {
    while (i > 0) {
        size_t parent = (i - 1) / 2;
        if (heap_[parent] < heap_[i]) {
            break; // 父亲已经比自己小，满足最小堆特性
        }
        swapNode_(i, parent);
        i = parent;
    }
}

// 向下调整（堆顶弹出或节点续命时间延后时用）
bool HeapTimer::siftdown_(size_t index, size_t n) {
    size_t i = index;
    size_t child = i * 2 + 1;
    while (child < n) {
        if (child + 1 < n && heap_[child + 1] < heap_[child]) {
            child++; // 找到左右孩子中较小的那个
        }
        if (heap_[i] < heap_[child]) {
            break; // 自己已经比最小的孩子还小了，停下
        }
        swapNode_(i, child);
        i = child;
        child = i * 2 + 1;
    }
    return i > index; // 返回是否发生了真正的移动
}

void HeapTimer::del_(size_t i) {
    assert(!heap_.empty() && i >= 0 && i < heap_.size());
    size_t n = heap_.size() - 1;
    // 将要删除的节点换到队尾，然后集中精力调整换上来的那个节点
    if (i < n) {
        swapNode_(i, n);
        if (!siftdown_(i, n)) {
            siftup_(i);
        }
    }
    // 删除队尾
    ref_[heap_.back().id] = -1;
    heap_.pop_back();
}

void HeapTimer::add(int id, int timeout_ms, const TimeoutCallBack& cb) {
    assert(id >= 0 && id < 65536);
    if (ref_[id] >= 0) { 
        // 之前就有，说明是旧连接覆盖，直接更新
        size_t i = ref_[id];
        heap_[i].expires = std::chrono::high_resolution_clock::now() + MS(timeout_ms);
        heap_[i].cb = cb;
        if (!siftdown_(i, heap_.size())) siftup_(i);
    } else { 
        // 全新连接
        size_t i = heap_.size();
        ref_[id] = i;
        heap_.push_back({id, std::chrono::high_resolution_clock::now() + MS(timeout_ms), cb});
        siftup_(i);
    }
}

void HeapTimer::adjust(int id, int timeout_ms) {
    // 续命：时间只会往后延，所以在堆里只会往下沉
    assert(id >= 0 && id < 65536);
    if (heap_.empty() || ref_[id] < 0) return;
    size_t i = ref_[id];
    heap_[i].expires = std::chrono::high_resolution_clock::now() + MS(timeout_ms);
    siftdown_(i, heap_.size()); 
}

void HeapTimer::popNode(int id) {
    if (heap_.empty() || ref_[id] < 0) return;
    del_(ref_[id]);
}

void HeapTimer::tick() {
    if (heap_.empty()) return;
    while (!heap_.empty()) {
        TimerNode node = heap_.front();
        if (std::chrono::duration_cast<MS>(node.expires - std::chrono::high_resolution_clock::now()).count() > 0) {
            break; // 堆顶还没超时，后面的一定也没超时，直接跳出
        }
        del_(0);   // 把堆顶踢掉
        node.cb(); // 执行超时回调（关闭连接）
    }
}

int HeapTimer::getNextTick() {
    tick(); // 每次拿之前，先把该超时的全踢了
    if (heap_.empty()) return -1; // -1 给 epoll，表示没定时器，无限阻塞直到有网络事件
    int res = std::chrono::duration_cast<MS>(heap_.front().expires - std::chrono::high_resolution_clock::now()).count();
    if (res < 0) res = 0;
    return res;
}

void HeapTimer::clear() {
    heap_.clear();
    std::fill_n(ref_, 65536, -1);
}