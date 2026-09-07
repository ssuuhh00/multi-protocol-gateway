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

    /**
     * @brief Holds whichever kind of subscriber this mapping asked for.
     *
     * Nothing is called on these after declaration -- the sample callback does
     * the work and destruction is what undeclares -- so this only has to keep
     * the right one alive. The miss listener is engaged only for a leg that
     * declared RELIABLE, and must outlive nothing but its own subscriber.
     *
     * The peer's liveliness is NOT watched from here. That watcher decides
     * whether this subscriber exists at all -- the bridge is created when the
     * peer's token appears and deleted when it goes -- so it lives one level
     * up, in DiscoveryManager, beside the DDS and SOME/IP discovery it
     * parallels.
     */
    struct SubscriberEntry {
        std::unique_ptr<zenoh::Subscriber<void>> plain;
        std::unique_ptr<zenoh::ext::AdvancedSubscriber<void>> advanced;
        std::unique_ptr<zenoh::ext::SampleMissListener<void>> miss;
    };

    std::map<std::string, SubscriberEntry> subscribers_;

    // Discovery liveliness tokens, under the reserved "@mpg" namespace, keyed by
    // conn_key (not zenoh_key) so multiple bridges sharing the same data key
    // each carry their own token. Unrelated to the LIVELINESS QoS policy, which
    // is declared on the data key expression itself by ZenohPublisher.
    std::map<std::string, zenoh::LivelinessToken> liveliness_tokens_;
};

#endif  // ZENOH_SUBSCRIBER_HPP
