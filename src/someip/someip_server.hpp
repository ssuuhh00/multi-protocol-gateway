#ifndef SOMEIP_SERVER_HPP
#define SOMEIP_SERVER_HPP

#include <spdlog/spdlog.h>
#include <vsomeip/vsomeip.hpp>

#include <atomic>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/resource_tracker.hpp"

// Offers SOME/IP services and notifies events for each sink=SOMEIP bridge.
class SomeipServer {
public:
    SomeipServer(std::shared_ptr<vsomeip::application> app, std::shared_ptr<SomeipResourceTracker> tracker,
                 ConnectionQueueMap* queue_map);
    ~SomeipServer();

    bool handleCreate(const Command& cmd);
    bool handleDelete(const Command& cmd);
    void stop();

private:
    struct WorkerThread {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> stop_flag;
    };

    void serverWorker(Command cmd, std::shared_ptr<std::atomic<bool>> stop_flag);
    std::string formatSomeipId(vsomeip::service_t s, vsomeip::instance_t i, vsomeip::eventgroup_t g,
                               vsomeip::event_t e);

    std::shared_ptr<spdlog::logger> log_;
    std::shared_ptr<vsomeip::application> app_;
    std::shared_ptr<SomeipResourceTracker> tracker_;
    ConnectionQueueMap* queue_map_;

    std::atomic<bool> running_{true};
    std::mutex resource_mutex_;

    std::map<std::string, std::unique_ptr<WorkerThread>> worker_threads_;
};

#endif  // SOMEIP_SERVER_HPP
