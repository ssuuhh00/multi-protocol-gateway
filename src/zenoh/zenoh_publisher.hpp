#ifndef ZENOH_PUBLISHER_HPP
#define ZENOH_PUBLISHER_HPP

#include <spdlog/spdlog.h>
#include <zenoh.hxx>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <string>
#include <thread>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"

// One Zenoh Publisher per bridge, driven by a dedicated worker thread that
// pulls payloads off the per-bridge ConnectionQueue.
class ZenohPublisher {
public:
    ZenohPublisher(std::shared_ptr<zenoh::Session> session, std::shared_ptr<ZenohResourceTracker> tracker,
                   ConnectionQueueMap* queue_map);
    ~ZenohPublisher();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    struct WorkerThread {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> stop_flag;
    };

    void publisherWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag);
    std::string qosToString(Qos qos);

    std::shared_ptr<spdlog::logger> log_;

    std::shared_ptr<zenoh::Session> session_;
    std::shared_ptr<ZenohResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    /**
     * @brief Holds whichever kind of publisher this mapping asked for.
     *
     * zenoh::Publisher and zenoh::ext::AdvancedPublisher are unrelated types
     * with no common base, but the worker only ever calls put(), so a pair of
     * optionals is enough and keeps the choice visible at every use. Exactly
     * one is engaged. A mapping that declared nothing gets the plain one.
     */
    class PublisherHandle {
    public:
        explicit PublisherHandle(zenoh::Publisher&& p) : plain_(std::move(p)) {}
        explicit PublisherHandle(zenoh::ext::AdvancedPublisher&& p) : advanced_(std::move(p)) {}

        void put(zenoh::Bytes&& payload) {
            if (advanced_) {
                advanced_->put(std::move(payload));
            } else {
                plain_->put(std::move(payload));
            }
        }

        bool is_advanced() const { return advanced_.has_value(); }

        /**
         * @brief Take ownership of this leg's QoS liveliness token.
         *
         * This is the LIVELINESS policy token, declared on the mapping's own
         * data key expression -- distinct from the discovery token this class
         * keeps in liveliness_tokens_, which lives under the reserved "@mpg"
         * namespace and announces the gateway to peer gateways.
         *
         * The token belongs here rather than in a map of its own so that its
         * lifetime is exactly the publisher's. Undeclaring it is what tells the
         * peers this writer is gone -- a DELETE sample on the token's key
         * expression -- and that must happen when the publisher goes away, not
         * a step earlier or later. Destroying this handle does both.
         */
        void adopt_liveliness(zenoh::LivelinessToken&& t) { liveliness_ = std::move(t); }
        bool has_liveliness() const { return liveliness_.has_value(); }

    private:
        std::optional<zenoh::Publisher> plain_;
        std::optional<zenoh::ext::AdvancedPublisher> advanced_;
        std::optional<zenoh::LivelinessToken> liveliness_;
    };

    std::map<std::string, std::unique_ptr<PublisherHandle>> publishers_;
    std::map<std::string, std::unique_ptr<WorkerThread>> worker_threads_;

    // One liveliness token per active publisher (keyed by zenoh_key, same as
    // publishers_). Declared on create so peer DiscoveryManagers can detect
    // this endpoint; undeclared on delete.
    std::map<std::string, zenoh::LivelinessToken> liveliness_tokens_;
};

#endif  // ZENOH_PUBLISHER_HPP
