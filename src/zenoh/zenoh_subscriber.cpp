#include "zenoh_subscriber.hpp"

#include <spdlog/sinks/stdout_color_sinks.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"
#include "zenoh/zenoh_qos_apply.hpp"

ZenohSubscriber::ZenohSubscriber(std::shared_ptr<zenoh::Session> session, std::shared_ptr<ZenohResourceTracker> tracker,
                                 ConnectionQueueMap* queue_map)
    : session_(session), tracker_(tracker), queue_map_(queue_map) {
    log_ = spdlog::get("ZEN_SUB");
    if (!log_) {
        log_ = spdlog::stdout_color_mt("ZEN_SUB");
        log_->set_level(spdlog::level::info);
    }

    if (!session_) {
        log_->critical("FATAL: Injected Zenoh Session is nullptr");
        throw std::runtime_error("ZenohSubscriber received null session");
    }
}

ZenohSubscriber::~ZenohSubscriber() { stop(); }

void ZenohSubscriber::stop() {
    if (!running_.exchange(false)) return;

    std::lock_guard<std::mutex> lock(resource_mutex_);
    // Drop liveliness tokens first so peers see DELETE before subscribers vanish.
    liveliness_tokens_.clear();
    subscribers_.clear();
}

bool ZenohSubscriber::handleCreate(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string zenoh_key = cmd.zenoh_key;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (subscribers_.count(conn_key)) {
        log_->warn("Creating: SKIPPED: Subscriber already exists for key '{}'", conn_key);
        return false;
    }
    if (zenoh_key.empty()) {
        log_->error("Creating: FAILED: zenoh_key is empty");
        return false;
    }

    queue_map_->create(conn_key);

    try {
        bool bench_mode = cmd.bench_mode;
        auto on_sample = [this, conn_key, zenoh_key, bench_mode](const zenoh::Sample& sample) {
            auto t_recv_start = std::chrono::steady_clock::now();

            std::vector<uint8_t> data = sample.get_payload().as_vector();
            if (data.empty()) return;

            uint64_t bench_seq_key = 0;
            int64_t bench_src_mw_lumped_us = 0;
            if (bench_mode) {
                bench_seq_key = bench::read_slot(data, bench::Slot::SEQ_KEY);
                const int64_t t_pub_send = static_cast<int64_t>(bench::read_slot(data, bench::Slot::TS));
                bench_src_mw_lumped_us = (t_recv_start.time_since_epoch().count() - t_pub_send) / 1000;
            }

            auto queue = queue_map_->get(conn_key);
            if (!queue) {
                log_->error("[Callback:{}] Queue not found for key: {}", zenoh_key, conn_key);
                return;
            }

            size_t data_size = data.size();
            queue->push(std::move(data));

            if (bench_mode) {
                log_->trace("[Callback:{}] | Seq: {} | SrcMW: {} us | Size: {} B",
                            zenoh_key, bench_seq_key, bench_src_mw_lumped_us, data_size);
            } else {
                log_->trace("[Callback:{}] | Size: {} B", zenoh_key, data_size);
            }
        };

        auto on_drop = []() {};

        // A leg that declared recovery, late-joiner history or liveliness needs
        // the Advanced Subscriber; anything else keeps the plain one. Note that a declared HISTORY does not put
        // us here: the subscriber's history option queries a publisher's cache
        // for past data, which is durability, and receive-side depth on this
        // leg is the bridge queue's job.
        SubscriberEntry entry;
        if (zenoh_qos::needsAdvancedSubscriber(cmd.qos_policies)) {
            auto opts = zenoh_qos::buildSubscriberOptions(cmd.qos_policies);
            auto subscriber = session_->ext().declare_advanced_subscriber(zenoh_key, std::move(on_sample),
                                                                          std::move(on_drop), std::move(opts));
            entry.advanced = std::make_unique<zenoh::ext::AdvancedSubscriber<void>>(std::move(subscriber));

            if (cmd.qos_policies.reliability && *cmd.qos_policies.reliability == ReliabilityKind::RELIABLE) {
                // The Zenoh counterpart of the DDS violation callbacks. Recovery
                // re-queries the publisher's cache for a detected gap; this
                // reports the gaps that were detected, so a leg that declares
                // reliability can say how often it had to recover rather than
                // asserting that it never lost anything.
                auto log = log_;
                auto key = conn_key;
                auto miss = entry.advanced->declare_sample_miss_listener(
                    [log, key](const zenoh::ext::Miss& m) {
                        log->warn("[QoS:{}] RELIABILITY: {} sample(s) missed from one publisher, recovery requested",
                                  key, m.nb);
                    },
                    []() {});
                entry.miss = std::make_unique<zenoh::ext::SampleMissListener<void>>(std::move(miss));
            }
            log_->info("Creating: AdvancedSubscriber created: '{}' [{}]", zenoh_key,
                       zenoh_qos::describeSubscriber(cmd.qos_policies));
        } else {
            auto subscriber = session_->declare_subscriber(zenoh_key, std::move(on_sample), std::move(on_drop));
            entry.plain = std::make_unique<zenoh::Subscriber<void>>(std::move(subscriber));
            log_->debug("Creating: Subscriber created: '{}' (QoS: {})", zenoh_key, qosToString(cmd.qos));
        }

        // The LIVELINESS QoS policy is not declared on this leg. The peer's
        // token is watched by DiscoveryManager, because the token is what
        // brings this subscriber into existence and takes it away again; by the
        // time handleCreate runs, the token has already been seen. The
        // discovery token declared just below is a different thing: it lives
        // under "@mpg" and announces this gateway to peer gateways.

        subscribers_[conn_key] = std::move(entry);
    } catch (const std::exception& e) {
        log_->error("Creating: FAILED: Exception while creating subscriber '{}': {}", zenoh_key, e.what());
        return false;
    }

    tracker_->addSubKey(zenoh_key);

    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRoleSub, self_zid, zenoh_key);
        auto token = session_->liveliness_declare_token(token_key);
        liveliness_tokens_.emplace(conn_key, std::move(token));
    } catch (const std::exception& e) {
        log_->warn("Creating: Liveliness token declare failed for '{}': {}", zenoh_key, e.what());
    }

    log_->debug("Creating: SUCCESS: Subscriber started for '{}' (key: {})", zenoh_key, conn_key);
    return true;
}

bool ZenohSubscriber::handleDelete(const Command& cmd) {
    std::string conn_key = make_conn_key(cmd);
    std::string zenoh_key = cmd.zenoh_key;

    std::lock_guard<std::mutex> lock(resource_mutex_);

    if (!subscribers_.count(conn_key)) {
        log_->warn("Deleting: SKIPPED: No subscriber found for key '{}'", conn_key);
        return false;
    }

    if (auto it = liveliness_tokens_.find(conn_key); it != liveliness_tokens_.end()) {
        try {
            std::move(it->second).undeclare();
        } catch (const std::exception& e) {
            log_->warn("Deleting: Liveliness undeclare failed for '{}': {}", zenoh_key, e.what());
        }
        liveliness_tokens_.erase(it);
    }

    subscribers_.erase(conn_key);
    tracker_->removeSubKey(zenoh_key);

    log_->debug("Deleting: SUCCESS: Cleaned up key '{}'", conn_key);
    return true;
}

std::string ZenohSubscriber::qosToString(Qos qos) {
    switch (qos) {
        case Qos::RELIABLE:    return "RELIABLE";
        case Qos::BEST_EFFORT: return "BEST_EFFORT";
        default:               return "DEFAULT";
    }
}
