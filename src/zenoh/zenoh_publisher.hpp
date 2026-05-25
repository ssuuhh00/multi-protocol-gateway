#ifndef ZENOH_PUBLISHER_HPP
#define ZENOH_PUBLISHER_HPP

#include <spdlog/spdlog.h>
#include <zenoh.hxx>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
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

    std::map<std::string, std::unique_ptr<zenoh::Publisher>> publishers_;
    std::map<std::string, std::unique_ptr<WorkerThread>> worker_threads_;

    // One liveliness token per active publisher (keyed by zenoh_key, same as
    // publishers_). Declared on create so peer DiscoveryManagers can detect
    // this endpoint; undeclared on delete.
    std::map<std::string, zenoh::LivelinessToken> liveliness_tokens_;
};

#endif  // ZENOH_PUBLISHER_HPP
