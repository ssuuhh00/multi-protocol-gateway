#ifndef GATEWAY_CORE_HPP
#define GATEWAY_CORE_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <fastdds/dds/domain/qos/DomainParticipantQos.hpp>
#include <spdlog/spdlog.h>
#include <vsomeip/vsomeip.hpp>
#include <zenoh.hxx>

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

#include "common/command.hpp"
#include "common/conn_key_util.hpp"
#include "common/connection_queue.hpp"
#include "common/logger_config.hpp"
#include "common/resource_tracker.hpp"
#include "dds/dds_publisher.hpp"
#include "dds/dds_subscriber.hpp"
#include "discovery/discovery_manager.hpp"
#include "someip/someip_client.hpp"
#include "someip/someip_server.hpp"
#include "zenoh/zenoh_publisher.hpp"
#include "zenoh/zenoh_subscriber.hpp"

class DDSPublisher;
class DDSSubscriber;
class SomeipServer;
class SomeipClient;
class ZenohPublisher;
class ZenohSubscriber;

// Central orchestrator. Owns the shared backends (DDS DomainParticipant,
// vSomeIP application, Zenoh session), the six per-protocol handlers, the
// per-connection queue map, and the DiscoveryManager. Bridges are created /
// torn down via createBridge() / deleteBridge(), invoked synchronously from
// the DiscoveryManager dispatch thread.
class GatewayCore {
public:
    GatewayCore();
    ~GatewayCore();

    // Idempotent and thread-safe.
    void shutdown();

    // DDS topic names are auto-prefixed with "rt/" inside the validator.
    bool createBridge(Command& cmd);
    bool deleteBridge(Command& cmd);

    std::shared_ptr<ConnectionQueue> getDataQueue(const std::string& key);
    void createDataQueue(const std::string& key);
    void logResourceStatus();

private:
    std::shared_ptr<spdlog::logger> log_;

    bool validateCommand(Command& cmd);
    void logCommandDetails(const Command& cmd, const std::string& action);
    std::string qosToString(Qos qos);
    std::string protocolRoutingToString(ProtocolType source, ProtocolType sink);

    eprosima::fastdds::dds::DomainParticipant* dds_participant_{nullptr};
    std::shared_ptr<vsomeip::application> someip_app_;
    std::shared_ptr<zenoh::Session> zenoh_session_;

    std::shared_ptr<DDSResourceTracker> dds_tracker_;
    std::shared_ptr<ZenohResourceTracker> zenoh_tracker_;
    std::shared_ptr<SomeipResourceTracker> someip_tracker_;

    std::shared_ptr<ConnectionQueueMap> queue_map_;

    std::unique_ptr<DDSPublisher> dds_publisher_;
    std::unique_ptr<DDSSubscriber> dds_subscriber_;
    std::unique_ptr<SomeipServer> someip_server_;
    std::unique_ptr<SomeipClient> someip_client_;
    std::unique_ptr<ZenohPublisher> zenoh_publisher_;
    std::unique_ptr<ZenohSubscriber> zenoh_subscriber_;

    std::unique_ptr<DiscoveryManager> discovery_manager_;

    std::atomic<bool> running_{false};
    std::vector<DiscoveryMapping> discovery_mappings_;
    std::mutex mutex_;
    std::thread someip_app_thread_;
};

#endif  // GATEWAY_CORE_HPP
