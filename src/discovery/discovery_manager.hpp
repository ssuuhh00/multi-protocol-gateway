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

/**
 * @brief One mapping's live state, copied out for observers.
 *
 * map_idx is the mapping's index in the JSON array, not its index in states_.
 * The two diverge whenever validateMapping() rejects an entry, and the JSON
 * index is the only identifier an external endpoint process can also compute --
 * it reads the same array in the same order. conn_key is what
 * ConnectionQueueMap is keyed on, so it joins this row to a queue row.
 */
struct MappingInfo {
    size_t map_idx = 0;
    std::string conn_key;
    ProtocolType source = ProtocolType::NONE;
    ProtocolType sink = ProtocolType::NONE;
    std::string src_addr;
    std::string sink_addr;
    bool source_condition = false;
    bool sink_condition = false;
    bool is_active = false;
};

/**
 * @class DiscoveryManager
 * @brief Watches DDS / SOME-IP / Zenoh discovery, evaluates configured mappings,
 *        and dispatches createBridge / deleteBridge to GatewayCore on a
 *        dedicated thread.
 *
 * A bridge exists exactly while its source's assertion does. That sentence is
 * the whole liveliness contract across the bridge, and it is enforced here,
 * not in the legs: a DDS source is a discovered DataWriter, a SOME/IP source
 * is an offered service, and a Zenoh source is a peer endpoint announcing
 * itself. When the assertion is withdrawn the bridge is deleted, which takes
 * the sink endpoint with it, and that is what the far side observes: a DDS
 * reader is unmatched, a Zenoh peer sees the sink's token DELETE, a SOME/IP
 * client sees the service withdrawn. When the assertion returns the bridge is
 * created again.
 *
 * The Zenoh half of that runs on two separate token namespaces, which do
 * different jobs and never see each other's keys:
 *
 *   DISCOVERY   "@mpg/<role>/<zid>/<user_key>", declared by every Zenoh
 *               endpoint (gateway legs and the example ext nodes alike).
 *               liveliness_subs_ watches it, and it is what makes a Zenoh
 *               peer discoverable at all -- the counterpart of DDS's
 *               participant discovery, which Zenoh does not do for us.
 *
 *   LIVELINESS QoS  the mapping's own key expression, declared only by a
 *               source that asked for the LIVELINESS policy. zenoh_liveliness_
 *               watches it. A Zenoh source that declares no liveliness is
 *               unaffected and stays discovery-gated only.
 *
 * This is one mechanism for three protocols rather than a per-leg
 * withdraw/restore: the DDS and SOME/IP halves are what the gateway already
 * did, and a second lifecycle beside the first would have been two mechanisms
 * that could disagree.
 */
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

    /**
     * @brief Copy every mapping's current state.
     *
     * For observers. Takes the same mutex the discovery callbacks use, so it is
     * consistent with what evaluateMappings() last decided.
     */
    std::vector<MappingInfo> snapshot();

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
        // Index in the mapping JSON array. Kept because validateMapping() can
        // skip entries, which makes this differ from the index in states_.
        size_t map_idx = 0;
        bool source_condition = true;  // true when source side is ready for bridging
        bool sink_condition = true;    // true when sink side is ready for bridging
        bool is_active = false;

        // A Zenoh source carries two independent assertions and the bridge
        // needs both, so neither callback may write source_condition directly:
        //   zenoh_src_discovered -- the peer's "@mpg/pub" discovery token
        //   zenoh_src_liveliness -- the peer's LIVELINESS QoS token on the data
        //                           key expression; stays true when the mapping
        //                           declared no liveliness
        // They arrive on different subscribers, so whichever fired last would
        // otherwise win and a mapping could go ready on discovery alone.
        // refreshZenohSource() folds them into source_condition.
        bool zenoh_src_discovered = true;
        bool zenoh_src_liveliness = true;
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

    // LIVELINESS QoS callback: a source's token on the mapping's own key
    // expression appeared (PUT) or went away (DELETE). Only mappings whose
    // Zenoh source declared liveliness are keyed on this; the others never get
    // here and are gated by the "@mpg" discovery tokens alone.
    void onZenohPeerLiveliness(const std::string& key, bool alive);

    // Fold a Zenoh source's two assertions into source_condition.
    // Returns true when source_condition changed.
    static bool refreshZenohSource(MappingState& state);

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

    // Discovery watchers on "@mpg/<role>/*/<user_key>", one pair per Zenoh
    // key expression a mapping touches.
    std::vector<zenoh::Subscriber<void>> liveliness_subs_;

    // LIVELINESS QoS watchers, one per key expression a liveliness-declaring
    // Zenoh source mapping reads from. Declared in start(), dropped in stop() --
    // the same lifetime as the SOME/IP availability handlers, and for the same
    // reason: the watcher has to exist BEFORE the bridge, since it is what
    // decides whether the bridge exists. zenoh_tokens_ counts the tokens
    // currently declared on each key, so two peers publishing to one key
    // expression keep the bridge up until the last of them is gone.
    std::map<std::string, std::unique_ptr<zenoh::Subscriber<void>>> zenoh_liveliness_;
    std::map<std::string, int> zenoh_tokens_;

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
