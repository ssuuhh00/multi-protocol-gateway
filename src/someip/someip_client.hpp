#ifndef SOMEIP_CLIENT_HPP
#define SOMEIP_CLIENT_HPP

#include <spdlog/spdlog.h>
#include <vsomeip/vsomeip.hpp>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"

// Subscribes to SOME/IP services and pushes received events into the bridge's
// ConnectionQueue for each source=SOMEIP bridge.
class SomeipClient {
public:
    SomeipClient(std::shared_ptr<vsomeip::application> app, std::shared_ptr<SomeipResourceTracker> tracker,
                 ConnectionQueueMap* queue_map);
    ~SomeipClient();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    // Invoked on a vSomeIP worker thread for every received event.
    void onMessageReceived(const std::shared_ptr<vsomeip::message>& msg, ConnectionQueueMap* queue_map, Command cmd,
                           std::shared_ptr<spdlog::logger> log);
    std::string formatSomeipId(vsomeip::service_t s, vsomeip::instance_t i, vsomeip::eventgroup_t g,
                               vsomeip::event_t e);

    std::shared_ptr<spdlog::logger> log_;
    std::shared_ptr<vsomeip::application> app_;
    std::shared_ptr<SomeipResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;
};

#endif  // SOMEIP_CLIENT_HPP
