#ifndef COMMON_CONNECTION_QUEUE_HPP
#define COMMON_CONNECTION_QUEUE_HPP

#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <vector>

// Steady-clock timestamp is captured at push time so queue dwell can be
// subtracted by the consuming worker. steady_clock = CLOCK_MONOTONIC on
// Linux/glibc and shares an epoch across threads in the same process.
struct DataType {
    std::vector<uint8_t> data;
    std::chrono::steady_clock::time_point timestamp;

    DataType(const std::vector<uint8_t>& d) : data(d), timestamp(std::chrono::steady_clock::now()) {}
    DataType(std::vector<uint8_t>&& d) : data(std::move(d)), timestamp(std::chrono::steady_clock::now()) {}
};

// The rvalue push() overload exists to avoid a per-hop memcpy of large payloads
// (e.g. 1 MB compressed images) when the caller has no further use for the buffer.
struct ConnectionQueue {
    std::queue<DataType> data_queue;
    std::mutex mtx;
    std::condition_variable cv;

    void push(const std::vector<uint8_t>& data) {
        std::lock_guard<std::mutex> lock(mtx);
        data_queue.emplace(data);
        cv.notify_one();
    }

    void push(std::vector<uint8_t>&& data) {
        std::lock_guard<std::mutex> lock(mtx);
        data_queue.emplace(std::move(data));
        cv.notify_one();
    }

    DataType pop() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return !data_queue.empty(); });
        DataType data = std::move(data_queue.front());
        data_queue.pop();
        return data;
    }

    std::optional<std::vector<uint8_t>> try_pop() {
        std::lock_guard<std::mutex> lock(mtx);
        if (data_queue.empty()) {
            return std::nullopt;
        }
        auto data = std::move(data_queue.front().data);
        data_queue.pop();
        return data;
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx);
        return data_queue.empty();
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(mtx);
        return data_queue.size();
    }
};

class ConnectionQueueMap {
public:
    std::shared_ptr<ConnectionQueue> create(const std::string& key) {
        std::lock_guard<std::mutex> lock(map_mtx_);
        if (map_.count(key) == 0) {
            map_[key] = std::make_shared<ConnectionQueue>();
        }
        return map_[key];
    }

    void remove(const std::string& key) {
        std::lock_guard<std::mutex> lock(map_mtx_);
        map_.erase(key);
    }

    std::shared_ptr<ConnectionQueue> get(const std::string& key) {
        std::lock_guard<std::mutex> lock(map_mtx_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        return nullptr;
    }

    std::vector<std::string> list_keys() {
        std::lock_guard<std::mutex> lock(map_mtx_);
        std::vector<std::string> keys;
        for (auto& kv : map_) {
            keys.push_back(kv.first);
        }
        return keys;
    }

private:
    std::map<std::string, std::shared_ptr<ConnectionQueue>> map_;
    std::mutex map_mtx_;
};

#endif  // COMMON_CONNECTION_QUEUE_HPP
