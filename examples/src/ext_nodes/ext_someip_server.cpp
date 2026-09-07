#include "ext_someip_server.hpp"

#include <chrono>
#include <cstdlib>
#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "someip/someip_qos_apply.hpp"

ExtSomeipServer::ExtSomeipServer(std::shared_ptr<vsomeip::application> app, EndpointSpec spec, uint16_t service_id,
                                 uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                                 const QosProfile& profile)
    : ExtPublisher(std::move(spec)),
      app_(std::move(app)),
      s_id_(service_id),
      i_id_(instance_id),
      g_id_(eventgroup_id),
      e_id_(event_id),
      profile_(profile) {
    if (!app_) throw std::runtime_error("ExtSomeipServer: null vsomeip app");
}

ExtSomeipServer::~ExtSomeipServer() { stop(); }

// The mapping decides, the environment overrides.
//
// The two sides of an event must register the same type or vsomeip rejects the
// later registration, so the default is whatever the gateway's SOURCE leg
// derives from the same profile -- that is what makes this endpoint a
// counterpart rather than an unrelated app that happens to share an event id.
// The override stays because a demo may want deliberately mismatched pairs,
// and a mismatch has to be requestable without editing a mapping file.
// Tri-state on purpose: unset = follow the mapping, "1" = force, "0" = refuse.
static vsomeip::event_type_e extSrvEventType(const QosProfile& p) {
    if (const char* v = std::getenv("MPG_EXT_SOMEIP_FIELD")) {
        return *v == '1' ? vsomeip::event_type_e::ET_FIELD : vsomeip::event_type_e::ET_EVENT;
    }
    return someip_qos::eventType(p);
}

// Reliability is a matched pair too: vsomeip logs the client's choice as
// `reliable=255` (RT_UNKNOWN) unless it is given one, and a TCP endpoint is only
// selected when both ends ask for it AND the providing side declares a
// `reliable` port in its configuration.
static vsomeip::reliability_type_e extSrvReliability(const QosProfile& p) {
    if (const char* v = std::getenv("MPG_EXT_SOMEIP_RELIABLE")) {
        return *v == '1' ? vsomeip::reliability_type_e::RT_RELIABLE : vsomeip::reliability_type_e::RT_UNRELIABLE;
    }
    return someip_qos::reliability(p);
}

std::string ExtSomeipServer::declared() const {
    // What offer_event() was actually handed. RT_UNKNOWN is reported as unset
    // rather than as a policy, because vsomeip's "you decide" is not a
    // declaration and must not read as one in the monitor.
    const auto rel = extSrvReliability(profile_);
    return std::string(someip_qos::toString(extSrvEventType(profile_))) + " " +
           (rel == vsomeip::reliability_type_e::RT_UNKNOWN ? "reliability=unset" : someip_qos::toString(rel)) +
           (has_subscriber() ? " sub=yes" : " sub=NO");
}

void ExtSomeipServer::start() {
    if (started_) return;

    std::set<vsomeip::eventgroup_t> groups{g_id_};
    app_->offer_event(s_id_, i_id_, e_id_, groups, extSrvEventType(profile_), std::chrono::milliseconds::zero(),
                      false, true, nullptr, extSrvReliability(profile_));
    app_->offer_service(s_id_, i_id_);

    // Subscription handler: the only signal a SOME/IP producer gets that a peer
    // is actually there. Returning true accepts the subscription.
    app_->register_subscription_handler(s_id_, i_id_, g_id_,
                                        [this](vsomeip::client_t client, uid_t, gid_t, bool subscribed) -> bool {
                                            subscribed_.store(subscribed, std::memory_order_relaxed);
                                            spdlog::get("SMP_SRV")->debug("[S{:x}:E{:x}] client 0x{:x} {}", s_id_,
                                                                          e_id_, client,
                                                                          subscribed ? "subscribed" : "unsubscribed");
                                            return true;
                                        });

    started_ = true;
    spdlog::get("SMP_SRV")->debug("[{}] offering S{:04x}:I{:04x} E{:04x} as {}", spec_.node_name, s_id_, i_id_, e_id_,
                                 declared());
}

void ExtSomeipServer::stop() {
    if (!started_) return;
    started_ = false;
    app_->unregister_subscription_handler(s_id_, i_id_, g_id_);
    app_->stop_offer_event(s_id_, i_id_, e_id_);
    app_->stop_offer_service(s_id_, i_id_);
}

int64_t ExtSomeipServer::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return 0;
    std::vector<uint8_t> data(payload_bytes, static_cast<uint8_t>('A'));
    const int64_t t_pub_send = bench::prepend_header(data, seq_key);
    if (stamp_) stamp_(data, seq_key);
    const size_t bench_size = data.size();

    auto payload = vsomeip::runtime::get()->create_payload();
    payload->set_data(std::move(data));
    app_->notify(s_id_, i_id_, e_id_, payload);

    spdlog::get("SMP_SRV")->trace("[Send:S{:x}:E{:x}] | Seq: {} | Size: {} B", s_id_, e_id_, seq_key, bench_size);
    return t_pub_send;
}
