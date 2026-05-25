#ifndef EXTERNAL_EXT_NODE_HPP
#define EXTERNAL_EXT_NODE_HPP

#include <cstdint>
#include <functional>
#include <vector>

#include "common/bench_util.hpp"

// External-endpoint publisher. send() prepares a fresh bench-header payload
// and writes it to the underlying middleware.
class ExtPublisher {
public:
    virtual ~ExtPublisher() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    // seq_key = (pair_id << 32) | local_seq. payload_bytes excludes bench header.
    virtual void send(uint64_t seq_key, size_t payload_bytes) = 0;
};

// External-endpoint subscriber. on_message receives the raw payload (bench
// header still attached) and the local steady_clock receive timestamp (ns).
class ExtSubscriber {
public:
    using OnMessageFn = std::function<void(const std::vector<uint8_t>& payload, int64_t recv_ns)>;
    virtual ~ExtSubscriber() = default;
    virtual void start() = 0;
    virtual void stop() = 0;
    void setOnMessage(OnMessageFn fn) { on_message_ = std::move(fn); }

protected:
    OnMessageFn on_message_;
};

#endif  // EXTERNAL_EXT_NODE_HPP
