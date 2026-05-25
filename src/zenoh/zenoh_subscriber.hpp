#ifndef ZENOH_SUBSCRIBER_HPP
#define ZENOH_SUBSCRIBER_HPP

#include <spdlog/spdlog.h>
#include <zenoh.hxx>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"

// One Zenoh Subscriber per bridge; the sample callback pushes payloads into
// the bridge's ConnectionQueue.
class ZenohSubscriber {
public:
    ZenohSubscriber(std::shared_ptr<zenoh::Session> session, std::shared_ptr<ZenohResourceTracker> tracker,
                    ConnectionQueueMap* queue_map);
    ~ZenohSubscriber();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    std::string qosToString(Qos qos);

    std::shared_ptr<spdlog::logger> log_;

    std::shared_ptr<zenoh::Session> session_;
    std::shared_ptr<ZenohResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    std::map<std::string, std::unique_ptr<zenoh::Subscriber<void>>> subscribers_;

    // Liveliness tokens are keyed by conn_key (not zenoh_key) so multiple
    // bridges sharing the same data key each carry their own token.
    std::map<std::string, zenoh::LivelinessToken> liveliness_tokens_;
};

#endif  // ZENOH_SUBSCRIBER_HPP
