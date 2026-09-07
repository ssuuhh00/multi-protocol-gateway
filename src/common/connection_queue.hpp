#ifndef COMMON_CONNECTION_QUEUE_HPP
#define COMMON_CONNECTION_QUEUE_HPP

#include <atomic>
#include <chrono>
#include <condition_variable>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <utility>
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
//
// Since the QoS work the queue also carries the mapping's declared history, as a
// depth bound with counted drop-oldest, so that the bridge hop behaves like the
// two ends of the path rather than being the one unbounded stage in the middle
// of a bounded one. It is not standing in for anything the middleware cannot do
// (see the QueueQos note in common/qos_spec.hpp). configure() is what turns that
// on; a mapping that declares nothing leaves every field at its zero and the
// class behaves exactly as it did before.
//
// Accounting identity: enq_total - deq_total - depth == drop_total. It holds of
// the queue at any instant. A reader that samples the counters one at a time
// sees it off by one whenever a pop lands mid-read, so check it at rest.
// What a configured queue does when it is full. The declared HISTORY maps onto
// exactly these: KEEP_LAST(n) bounds the queue at n and discards the oldest,
// KEEP_ALL and an undeclared mapping both leave it unbounded -- KEEP_ALL is a
// separate code only so that "asked for lossless" is distinguishable from
// "asked for nothing".
enum QueuePolicy : uint32_t { Q_UNBOUNDED = 0, Q_KEEP_LAST = 1, Q_KEEP_ALL = 2 };

struct ConnectionQueue {
    std::queue<DataType> data_queue;
    std::mutex mtx;
    std::condition_variable cv;

    // ---- Depth accounting ------------------------------------------------
    //
    // The submitted gateway logs "Queue: N us" -- how long a message waited --
    // but never how many were waiting. Dwell time cannot answer whether the
    // queue is bounded: a queue growing without limit and one held at a cap
    // both show rising dwell, and only the depth tells them apart.
    //
    // Every store below happens under a lock the caller already holds, is
    // relaxed, and adds no clock read: the values are ones the surrounding
    // code has computed anyway. There is no enable flag, deliberately -- a
    // branch on a runtime setting would make the hot path differ between runs,
    // which is the thing the flag would have been for.
    std::atomic<uint32_t> depth{0};      // current size, updated under mtx
    std::atomic<uint32_t> peak{0};       // high-water mark since start
    std::atomic<uint64_t> enq_total{0};
    std::atomic<uint64_t> deq_total{0};
    std::atomic<uint64_t> dwell_us_sum{0};
    std::atomic<uint64_t> dwell_us_max{0};

    // Retention in bytes, not just in entries. A depth of 8 means nothing
    // without the element size once a run mixes payload classes, and the
    // memory question is denominated in bytes: "8 entries" and
    // "4 MB held" are the same queue described in the units of two different
    // arguments. data.size() is already in hand at both call sites, so this
    // costs no clock read and no traversal.
    std::atomic<uint64_t> bytes_cur{0};
    std::atomic<uint64_t> bytes_peak{0};

    // ---- Bounded operation ------------------------------------------------
    //
    // Inert unless configure() gave it a depth. The submitted gateway's path is
    // `max_depth_ == 0`, which costs one predictable compare in push() and
    // changes pop() not at all.
    //
    // Two behaviours, and only two: unbounded, or bounded with the oldest
    // discarded. That is not a simplification of something richer -- it is the
    // whole of what a declared history means for a hop like this one, and a
    // third rule would be behaviour neither side's middleware asked for.
    std::atomic<uint64_t> drop_total{0};

    // A queue is torn down by its *sink* handler, so a source-only mapping
    // leaves its key in the map until shutdown sweeps it. Both paths want to
    // report the same totals once; this is how the second one knows to keep
    // quiet rather than double-count into the log.
    std::atomic<bool> summary_logged{false};

    /**
     * @brief Apply a declared queue policy. Call before the queue is published
     *        into ConnectionQueueMap, never after.
     *
     * The fields it writes are plain, not atomic, and are read from the
     * an observer without a lock. That is only sound
     * because publication into the map happens-after this call: ConnectionQueueMap
     * inserts under map_mtx_ and every reader takes map_mtx_ to obtain the
     * shared_ptr, so the stores are ordered ahead of any possible read.
     * Configuring an already-published queue would be a data race.
     */
    void configure(size_t max_depth, uint32_t policy_code) {
        max_depth_ = max_depth;
        policy_code_ = policy_code;
    }

    size_t max_depth() const { return max_depth_; }
    uint32_t policy_code() const { return policy_code_; }
    bool bounded() const { return max_depth_ != 0; }
    /// True when the mapping said something about retention, KEEP_ALL included.
    bool configured() const { return policy_code_ != 0; }

    void push(const std::vector<uint8_t>& data) {
        std::lock_guard<std::mutex> lock(mtx);
        size_t n = evict_if_full(data_queue.size());
        const size_t nbytes = data.size();
        data_queue.emplace(data);
        note_push(n + 1, nbytes);
        cv.notify_one();
    }

    void push(std::vector<uint8_t>&& data) {
        std::lock_guard<std::mutex> lock(mtx);
        size_t n = evict_if_full(data_queue.size());
        const size_t nbytes = data.size();
        data_queue.emplace(std::move(data));
        note_push(n + 1, nbytes);
        cv.notify_one();
    }

    /**
     * @brief Wake a blocked worker at teardown, without going through the cap.
     *
     * Every sink handler stops its worker by enqueuing an empty payload, which
     * the worker recognises as its termination condition. Routing that through
     * push() would make a full KEEP_LAST(n) queue evict a real sample to admit
     * the sentinel and charge it to drop_total -- so a clean shutdown would add
     * exactly one fabricated drop per bridge to the number the response letter
     * quotes. With 128 bridges in an overload cell that is 128 drops that never
     * happened. It also skips peak, because a transient n+1 at teardown is not
     * a high-water mark anyone means to report.
     *
     * enq_total *is* incremented, because the worker's pop() will increment
     * deq_total when it takes the sentinel out; skipping one and not the other
     * would break enq - deq - depth == drop.
     */
    void push_wakeup() {
        std::lock_guard<std::mutex> lock(mtx);
        data_queue.emplace(std::vector<uint8_t>{});
        depth.store(static_cast<uint32_t>(data_queue.size()), std::memory_order_relaxed);
        enq_total.fetch_add(1, std::memory_order_relaxed);
        cv.notify_one();
    }

    /// Blocking dequeue. Byte-for-byte the submitted version: a declared
    /// history bounds what goes in, never how it comes out.
    DataType pop() {
        std::unique_lock<std::mutex> lock(mtx);
        cv.wait(lock, [&] { return !data_queue.empty(); });
        DataType data = std::move(data_queue.front());
        data_queue.pop();
        note_pop(data_queue.size(), data.data.size());
        return data;
    }

    // Unused by the gateway (no polling consumer exists); kept as part of the
    // container's interface.
    std::optional<std::vector<uint8_t>> try_pop() {
        std::lock_guard<std::mutex> lock(mtx);
        if (data_queue.empty()) {
            return std::nullopt;
        }
        auto data = std::move(data_queue.front().data);
        const size_t n = data.size();
        data_queue.pop();
        note_pop(data_queue.size(), n);
        return data;
    }

    // Dwell is reported by the sink worker, not measured here. All three
    // workers (dds_publisher.cpp:358, zenoh_publisher.cpp:235,
    // someip_server.cpp:289) already compute this value from the DataType
    // timestamp to log it, so handing it over costs one relaxed add and keeps
    // the queue free of any clock call.
    void record_dwell_us(uint64_t us) {
        dwell_us_sum.fetch_add(us, std::memory_order_relaxed);
        uint64_t prev = dwell_us_max.load(std::memory_order_relaxed);
        while (us > prev && !dwell_us_max.compare_exchange_weak(prev, us, std::memory_order_relaxed)) {
        }
    }

    bool empty() {
        std::lock_guard<std::mutex> lock(mtx);
        return data_queue.empty();
    }

    size_t size() {
        std::lock_guard<std::mutex> lock(mtx);
        return data_queue.size();
    }

private:
    // Configured once before publication into ConnectionQueueMap; const in
    // effect thereafter. 0 means "no cap".
    size_t max_depth_ = 0;
    uint32_t policy_code_ = 0;  // QueuePolicy; 0 == Q_UNBOUNDED

    // HISTORY KEEP_LAST(n) at the bridge: the newest n survive, so the sample
    // that goes is the oldest. Caller holds mtx and passes the size it already
    // read; returns the size after any eviction, so a push costs exactly one
    // std::deque::size() whether or not a cap is configured. The guard is
    // `!= 0` rather than a saturating sentinel because popping an empty
    // std::queue is undefined behaviour and `size() >= 0` is always true.
    size_t evict_if_full(size_t n) {
        if (max_depth_ != 0 && n >= max_depth_) {
            const size_t nbytes = data_queue.front().data.size();
            data_queue.pop();
            drop_total.fetch_add(1, std::memory_order_relaxed);
            const uint64_t held = bytes_cur.load(std::memory_order_relaxed);
            bytes_cur.store(held >= nbytes ? held - nbytes : 0, std::memory_order_relaxed);
            return n - 1;
        }
        return n;
    }

    // Both callers hold mtx, so depth/peak need no CAS against each other.
    void note_push(size_t n, size_t nbytes) {
        const uint32_t d = static_cast<uint32_t>(n);
        depth.store(d, std::memory_order_relaxed);
        if (d > peak.load(std::memory_order_relaxed)) peak.store(d, std::memory_order_relaxed);
        enq_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t held = bytes_cur.load(std::memory_order_relaxed) + nbytes;
        bytes_cur.store(held, std::memory_order_relaxed);
        if (held > bytes_peak.load(std::memory_order_relaxed)) {
            bytes_peak.store(held, std::memory_order_relaxed);
        }
    }

    void note_pop(size_t n, size_t nbytes) {
        depth.store(static_cast<uint32_t>(n), std::memory_order_relaxed);
        deq_total.fetch_add(1, std::memory_order_relaxed);
        const uint64_t held = bytes_cur.load(std::memory_order_relaxed);
        bytes_cur.store(held >= nbytes ? held - nbytes : 0, std::memory_order_relaxed);
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

    /**
     * @brief Create a queue that is already carrying its declared policy.
     *
     * The configuration has to land before the shared_ptr is visible to anyone.
     * An observer reaching every
     * queue through snapshot(); if the fields were written after insertion, a
     * tick landing in that window would publish the bridge as unbounded and the
     * observer would show a cap of none for a mapping that declared one. Doing
     * it under map_mtx_, before the insert, makes the window not exist.
     *
     * An existing queue is returned unchanged rather than reconfigured: the
     * fields are read without a lock, which is only sound while they are
     * written exactly once, before publication.
     */
    std::shared_ptr<ConnectionQueue> create(const std::string& key, size_t max_depth, uint32_t policy_code) {
        std::lock_guard<std::mutex> lock(map_mtx_);
        auto it = map_.find(key);
        if (it != map_.end()) {
            return it->second;
        }
        auto q = std::make_shared<ConnectionQueue>();
        q->configure(max_depth, policy_code);
        map_[key] = q;
        return q;
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

    /**
     * @brief Copy (key, queue) pairs into a caller-owned vector.
     *
     * For an observer. It must not call ConnectionQueue::size() or
     * empty(), which take the same mutex as push/pop and would put the sampler
     * on the per-message path. Reading the atomic counters off the shared_ptr
     * touches no queue lock at all. The caller reuses `out` across ticks so
     * sampling allocates nothing in steady state.
     */
    void snapshot(std::vector<std::pair<std::string, std::shared_ptr<ConnectionQueue>>>& out) {
        std::lock_guard<std::mutex> lock(map_mtx_);
        out.clear();
        for (auto& kv : map_) {
            out.emplace_back(kv.first, kv.second);
        }
    }

private:
    std::map<std::string, std::shared_ptr<ConnectionQueue>> map_;
    std::mutex map_mtx_;
};

#endif  // COMMON_CONNECTION_QUEUE_HPP
