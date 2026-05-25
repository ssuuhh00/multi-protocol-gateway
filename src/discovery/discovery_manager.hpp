#ifndef DISCOVERY_MANAGER_HPP
#define DISCOVERY_MANAGER_HPP

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantListener.hpp>
#include <fastdds/rtps/common/GuidPrefix_t.hpp>
#include <spdlog/spdlog.h>
#include <vsomeip/vsomeip.hpp>
#include <zenoh.hxx>

#include <atomic>
#include <condition_variable>
#include <deque>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <tuple>
#include <vector>

#include "common/command.hpp"
#include "common/resource_tracker.hpp"
#include "discovery/discovery_mapping.hpp"

class GatewayCore;

// Watches DDS / SOME-IP / Zenoh discovery, evaluates configured mappings, and
// dispatches createBridge / deleteBridge to GatewayCore on a dedicated thread.
class DiscoveryManager {
public:
    DiscoveryManager(eprosima::fastdds::dds::DomainParticipant* dds_participant,
                     std::shared_ptr<vsomeip::application> someip_app,
                     std::shared_ptr<SomeipResourceTracker> someip_tracker,
                     std::shared_ptr<zenoh::Session> zenoh_session, GatewayCore* core,
                     const std::vector<DiscoveryMapping>& mappings);
    ~DiscoveryManager();

    void start();
    void stop();

private:
    class DiscoveryListener : public eprosima::fastdds::dds::DomainParticipantListener {
    public:
        DiscoveryListener(DiscoveryManager* manager);

        void on_publisher_discovery(eprosima::fastdds::dds::DomainParticipant* participant,
                                    eprosima::fastrtps::rtps::WriterDiscoveryInfo&& info) override;

        void on_subscriber_discovery(eprosima::fastdds::dds::DomainParticipant* participant,
                                     eprosima::fastrtps::rtps::ReaderDiscoveryInfo&& info) override;

    private:
        DiscoveryManager* manager_;
    };

    struct MappingState {
        DiscoveryMapping mapping;
        bool source_condition = true;
        bool sink_condition = true;
        bool is_active = false;
    };

    // DDS: GUID-prefix compare. SOME/IP: tracker server_count + own client_id.
    // ZENOH: zid string compare against the zid embedded in the liveliness key.
    bool isSelfGuid(const eprosima::fastrtps::rtps::GuidPrefix_t& prefix) const;
    bool isSelfOfferedService(vsomeip::service_t sid, vsomeip::instance_t iid) const;
    bool isSelfZid(std::string_view zid_hex) const;
    bool isSelfSomeipClient(vsomeip::client_t client) const;

    void onDdsWriterDiscovered(const std::string& topic, bool alive);
    void onDdsReaderDiscovered(const std::string& topic, bool alive);
    void onSomeipAvailability(vsomeip::service_t sid, vsomeip::instance_t iid, bool available);
    void onSomeipSubscription(vsomeip::client_t client, vsomeip::service_t sid, vsomeip::instance_t iid,
                              vsomeip::eventgroup_t gid, bool subscribed);
    void onZenohPublisherDiscovered(const std::string& zenoh_key, bool alive);
    void onZenohSubscriberDiscovered(const std::string& zenoh_key, bool alive);

    bool validateMapping(Command& cmd, size_t mapping_idx);
    void evaluateMappings();

    std::shared_ptr<spdlog::logger> log_;

    eprosima::fastdds::dds::DomainParticipant* dds_participant_{nullptr};
    std::unique_ptr<DiscoveryListener> disc_listener_;
    eprosima::fastrtps::rtps::GuidPrefix_t self_guid_prefix_;

    std::shared_ptr<vsomeip::application> someip_app_;
    std::shared_ptr<SomeipResourceTracker> someip_tracker_;

    std::shared_ptr<zenoh::Session> zenoh_session_;
    std::string self_zid_hex_;
    std::vector<zenoh::Subscriber<void>> liveliness_subs_;

    // For each sink=SOMEIP mapping we pre-offer (service, instance, group,
    // event) and register a subscription handler so external clients flip
    // sink_condition true. The tracker hold keeps the offer alive across
    // createBridge / deleteBridge cycles, so SomeipServer's own offer /
    // stop_offer calls become no-ops while the manager is running.
    using SomeipResourceTuple =
        std::tuple<vsomeip::service_t, vsomeip::instance_t, vsomeip::eventgroup_t, vsomeip::event_t>;
    using SomeipEventgroupKey = std::tuple<vsomeip::service_t, vsomeip::instance_t, vsomeip::eventgroup_t>;

    std::vector<SomeipResourceTuple> someip_sink_holds_;
    std::map<SomeipEventgroupKey, int> someip_sink_sub_counts_;
    std::set<SomeipEventgroupKey> someip_sub_handler_keys_;

    std::vector<MappingState> states_;
    std::mutex mutex_;

    GatewayCore* core_;
    std::atomic<bool> running_{true};

    // Discovery callbacks must NOT enter Fast-DDS entity teardown directly:
    // delete_datareader / delete_topic from inside on_*_discovery deadlocks
    // on the participant's internal listener mutex. Instead, callbacks update
    // state and push a BridgeAction; the dispatch thread serially calls
    // GatewayCore::createBridge / deleteBridge from a safe context.
    struct BridgeAction {
        Command cmd;
        bool is_create;
        size_t mapping_idx;
    };

    std::mutex action_queue_mutex_;
    std::condition_variable action_cv_;
    std::deque<BridgeAction> action_queue_;
    std::thread dispatch_thread_;

    void dispatchLoop();
    void enqueueAction(BridgeAction&& action);
};

#endif  // DISCOVERY_MANAGER_HPP
