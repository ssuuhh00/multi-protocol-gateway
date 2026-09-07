#include "zenoh_publisher.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"
#include "zenoh/zenoh_qos_apply.hpp"

ZenohPublisher::ZenohPublisher(std::shared_ptr<zenoh::Session> session, std::shared_ptr<ZenohResourceTracker> tracker,
                               ConnectionQueueMap* queue_map)
    : session_(session), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("ZEN_PUB");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("ZEN_PUB");
        log_->set_level(spdlog::level::info);
    }

    if (!session_) {
        log_->critical("FATAL: Injected Zenoh Session is nullptr");
        throw std::runtime_error("ZenohPublisher received null session");
    }
}

ZenohPublisher::~ZenohPublisher() { stop(); }

void ZenohPublisher::stop() {
    if (!running_.exchange(false)) return;

    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        for (auto& [key, worker] : worker_threads_) {
            if (!worker) continue;
            worker->stop_flag->store(true);
            // Wake the blocked pop() with a poison pill. push_wakeup(), not
            // push(): a bounded queue would evict a real sample to admit a
            // sentinel and charge it to drop_total.
            if (auto q = queue_map_->get(key)) {
                q->push_wakeup();
            }
            if (worker->thread.joinable()) {
                worker->thread.join();
            }
        }
        worker_threads_.clear();
    }

    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        // Drop liveliness tokens first so peers see DELETE before publishers vanish.
        liveliness_tokens_.clear();
        publishers_.clear();
    }
}

bool ZenohPublisher::handleCreate(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string zenoh_key = cmd.zenoh_key;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (worker_threads_.count(conn_key)) {
        log_->warn("Creating: SKIPPED: Connection already exists for key '{}'", conn_key);
        return false;
    }
    if (zenoh_key.empty()) {
        log_->error("Creating: FAILED: zenoh_key is empty");
        return false;
    }

    queue_map_->create(conn_key);

    if (publishers_.find(zenoh_key) != publishers_.end()) {
        log_->warn("Creating: REJECTED: Publisher '{}' already exists (no reuse for publisher)", zenoh_key);
        return false;
    }

    // Create new publisher.
    //
    // Which one depends on what the mapping declared. z_publisher_options_t has
    // a `reliability` field, but the upstream header is explicit that it "does
    // not trigger any data retransmission on the wire" -- it is an attribute
    // carried on the sample. The mechanism that actually recovers a lost sample
    // is the Advanced Publisher's cache plus sample-miss detection, so a leg
    // that declared RELIABLE (or history, durability, liveliness) gets that
    // instead. A leg that declared none of them keeps the plain publisher this
    // gateway creates by default.
    log_->trace("Creating: New Zenoh publisher: '{}'", zenoh_key);
    try {
        if (zenoh_qos::needsAdvancedPublisher(cmd.qos_policies)) {
            auto opts = zenoh_qos::buildPublisherOptions(cmd.qos_policies);
            auto publisher = session_->ext().declare_advanced_publisher(zenoh_key, std::move(opts));
            publishers_[zenoh_key] = std::make_unique<PublisherHandle>(std::move(publisher));
            log_->info("Creating: AdvancedPublisher created: '{}' [{}]", zenoh_key,
                       zenoh_qos::describePublisher(cmd.qos_policies));
        } else {
            auto publisher = session_->declare_publisher(zenoh_key);
            publishers_[zenoh_key] = std::make_unique<PublisherHandle>(std::move(publisher));
            log_->debug("Creating: Publisher created: '{}' (QoS: {})", zenoh_key, qosToString(cmd.qos));
        }

        // LIVELINESS QoS, on the leg where this gateway is the writer. This is
        // the policy token on the mapping's own key expression -- not the
        // "@mpg" discovery token declared further down, which announces this
        // gateway to peer gateways and is always present.
        //
        // AdvancedPublisher's publisher_detection makes this publisher visible
        // to a subscriber that is looking; it is not an assertion of liveness
        // and it is not what a DDS DataWriter offers. The assertion is a
        // liveliness token: while it is declared the peers see a PUT on its key
        // expression, and the moment it is undeclared -- or the session drops
        // -- they get a DELETE. That is the counterpart of a lease expiring,
        // and it is why this is declared here rather than being inferred from
        // the Advanced options.
        //
        // The token's key expression is the mapping's own, so a peer that
        // subscribes to liveliness on the key it already reads from finds it
        // without being told a second name.
        //
        // Its lifetime is the bridge's: DiscoveryManager deletes the bridge
        // when the source's own assertion is withdrawn, the handle below goes
        // with it, and the DELETE the peer then sees is the source's loss
        // carried across. Nothing here has to know why the bridge went away.
        if (cmd.qos_policies.liveliness) {
            auto& handle = publishers_[zenoh_key];
            handle->adopt_liveliness(session_->liveliness_declare_token(zenoh::KeyExpr(zenoh_key)));
            log_->info("[QoS:{}] LIVELINESS: token declared on '{}'; peers see a DELETE when it is undeclared or "
                       "the session drops",
                       conn_key, zenoh_key);
        }
    } catch (const std::exception& e) {
        log_->error("Creating: FAILED: Exception while creating publisher '{}': {}", zenoh_key, e.what());
        return false;
    }

    tracker_->addPubKey(zenoh_key);

    // Liveliness: advertise our role so peer gateways' DiscoveryManager can
    // detect this publisher. Non-fatal — data path still works without it.
    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRolePub, self_zid, zenoh_key);
        auto token = session_->liveliness_declare_token(token_key);
        liveliness_tokens_.emplace(zenoh_key, std::move(token));
    } catch (const std::exception& e) {
        log_->warn("Creating: Liveliness token declare failed for '{}': {}", zenoh_key, e.what());
    }

    auto worker = std::make_unique<WorkerThread>();
    worker->stop_flag = std::make_shared<std::atomic<bool>>(false);
    worker->thread = std::thread(&ZenohPublisher::publisherWorker, this, cmd, worker->stop_flag);
    worker_threads_[conn_key] = std::move(worker);

    log_->debug("Creating: SUCCESS: Publisher and worker started for '{}' (key: {})", zenoh_key, conn_key);
    return true;
}

bool ZenohPublisher::handleDelete(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string zenoh_key = cmd.zenoh_key;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (!worker_threads_.count(conn_key)) {
        log_->warn("Deleting: SKIPPED: No worker found for key '{}'", conn_key);
        return false;
    }

    worker_threads_[conn_key]->stop_flag->store(true);
    if (auto queue = queue_map_->get(conn_key)) {
        // push_wakeup(), not push(): a bounded queue would evict a real sample
        // to admit the sentinel and count it as a policy drop.
        queue->push_wakeup();
    }
    if (worker_threads_[conn_key]->thread.joinable()) {
        worker_threads_[conn_key]->thread.join();
    }
    worker_threads_.erase(conn_key);

    publishers_.erase(zenoh_key);
    tracker_->removePubKey(zenoh_key);

    if (auto it = liveliness_tokens_.find(zenoh_key); it != liveliness_tokens_.end()) {
        try {
            std::move(it->second).undeclare();
        } catch (const std::exception& e) {
            log_->warn("Deleting: Liveliness undeclare failed for '{}': {}", zenoh_key, e.what());
        }
        liveliness_tokens_.erase(it);
    }

    queue_map_->remove(conn_key);

    log_->debug("Deleting: SUCCESS: Cleaned up key '{}'", conn_key);
    return true;
}

void ZenohPublisher::publisherWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag) {
    std::string conn_key = make_conn_key(cmd);
    std::string zenoh_key = cmd.zenoh_key;

    auto data_queue = queue_map_->get(conn_key);
    if (!data_queue) {
        log_->error("[Worker:{}] FAILED: Queue is nullptr for key '{}'", zenoh_key, conn_key);
        return;
    }

    PublisherHandle* publisher = nullptr;
    {
        std::lock_guard<std::mutex> lock(resource_mutex_);
        auto it = publishers_.find(zenoh_key);
        if (it != publishers_.end()) publisher = it->second.get();
    }
    if (!publisher) {
        log_->error("[Worker:{}] FAILED: Publisher not found", zenoh_key);
        return;
    }

    while (running_ && !stop_flag->load()) {
        DataType data_box = data_queue->pop();

        if (!running_ || stop_flag->load() || data_box.data.empty()) break;

        auto t_bytes_leave = std::chrono::steady_clock::now();
        auto queue_latency_us =
            std::chrono::duration_cast<std::chrono::microseconds>(t_bytes_leave - data_box.timestamp).count();
        // Hand the queue the dwell it just computed: one relaxed add, no extra
        // clock read. The queue owns the aggregate because it is the only thing
        // that also knows the depth the message waited behind.
        data_queue->record_dwell_us(static_cast<uint64_t>(queue_latency_us));

        // Bench (Zenoh): no msg-object conversion. Stamp T_BYTES_LEAVE and
        // capture seq_key before publisher->put() moves data_box.data.
        uint64_t bench_seq_key = 0;
        if (cmd.bench_mode) {
            bench::stamp_value(data_box.data, bench::Slot::TS, t_bytes_leave.time_since_epoch().count());
            bench_seq_key = bench::read_slot(data_box.data, bench::Slot::SEQ_KEY);
        }

        try {
            size_t data_size = data_box.data.size();
            // Move into Bytes to avoid a payload copy on the way out.
            publisher->put(zenoh::Bytes(std::move(data_box.data)));

            if (cmd.bench_mode) {
                log_->trace("[Worker:{}] | Seq: {} | Queue: {} us | Size: {} B",
                            zenoh_key, bench_seq_key, queue_latency_us, data_size);
            } else {
                log_->trace("[Worker:{}] | Queue: {} us | Size: {} B", zenoh_key, queue_latency_us, data_size);
            }
        } catch (const std::exception& e) {
            log_->error("[Worker:{}] Exception during put: {}", zenoh_key, e.what());
        }
    }
}

std::string ZenohPublisher::qosToString(Qos qos) {
    switch (qos) {
        case Qos::RELIABLE:    return "RELIABLE";
        case Qos::BEST_EFFORT: return "BEST_EFFORT";
        default:               return "DEFAULT";
    }
}
