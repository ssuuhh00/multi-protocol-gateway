#include "ext_someip_client.hpp"

#include <cstdlib>
#include <set>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "someip/someip_qos_apply.hpp"

ExtSomeipClient::ExtSomeipClient(std::shared_ptr<vsomeip::application> app, EndpointSpec spec, uint16_t service_id,
                                 uint16_t instance_id, uint16_t eventgroup_id, uint16_t event_id,
                                 const QosProfile& profile)
    : ExtSubscriber(std::move(spec)),
      app_(std::move(app)),
      s_id_(service_id),
      i_id_(instance_id),
      g_id_(eventgroup_id),
      e_id_(event_id),
      profile_(profile) {
    if (!app_) throw std::runtime_error("ExtSomeipClient: null vsomeip app");
}

ExtSomeipClient::~ExtSomeipClient() { stop(); }

// vsomeip registers one event type per (service, instance, event). If the two
// sides of an event disagree, routing_manager_base rejects the second
// registration outright -- "Event registration update failed. Specified
// arguments do not match existing registration." -- and that side then receives
// nothing. So the default has to be whatever the gateway's SINK leg derives
// from the same mapping; someip_qos::eventType() reads durability alone and
// answers the same on both roles, so it is. MPG_EXT_SOMEIP_FIELD exists to
// break that agreement on purpose (a deliberately mismatched pair). Tri-state:
// unset = follow the mapping, "1" = force ET_FIELD, "0" = force ET_EVENT.
static vsomeip::event_type_e extEventType(const QosProfile& p) {
    if (const char* v = std::getenv("MPG_EXT_SOMEIP_FIELD")) {
        return *v == '1' ? vsomeip::event_type_e::ET_FIELD : vsomeip::event_type_e::ET_EVENT;
    }
    return someip_qos::eventType(p);
}

// Reliability is a matched pair too: vsomeip logs the client's choice as
// `reliable=255` (RT_UNKNOWN) unless it is given one, and a TCP endpoint is only
// selected when both ends ask for it AND the providing side declares a
// `reliable` port in its configuration.
static vsomeip::reliability_type_e extReliability(const QosProfile& p) {
    if (const char* v = std::getenv("MPG_EXT_SOMEIP_RELIABLE")) {
        return *v == '1' ? vsomeip::reliability_type_e::RT_RELIABLE : vsomeip::reliability_type_e::RT_UNRELIABLE;
    }
    return someip_qos::reliability(p);
}

std::string ExtSomeipClient::declared() const {
    // What request_event() was actually handed, not what we wish it were.
    const auto rel = extReliability(profile_);
    return std::string(someip_qos::toString(extEventType(profile_))) + " " +
           (rel == vsomeip::reliability_type_e::RT_UNKNOWN ? "reliability=unset" : someip_qos::toString(rel)) +
           (service_available() ? " svc=up" : " svc=DOWN");
}

void ExtSomeipClient::start() {
    if (started_) return;

    std::set<vsomeip::eventgroup_t> groups{g_id_};
    app_->request_event(s_id_, i_id_, e_id_, groups, extEventType(profile_), extReliability(profile_));
    app_->register_message_handler(s_id_, i_id_, e_id_,
                                   [this](const std::shared_ptr<vsomeip::message>& msg) { onMessage(msg); });
    app_->register_availability_handler(
        s_id_, i_id_, [this](vsomeip::service_t s, vsomeip::instance_t i, bool avail) {
            available_.store(avail, std::memory_order_relaxed);
            spdlog::get("SMP_CLI")->debug("[S{:x}:I{:x}] service {}", s, i, avail ? "AVAILABLE" : "UNAVAILABLE");
        });
    app_->subscribe(s_id_, i_id_, g_id_);
    app_->request_service(s_id_, i_id_);

    started_ = true;
    spdlog::get("SMP_CLI")->debug("[{}] requesting S{:04x}:I{:04x} E{:04x} as {}", spec_.node_name, s_id_, i_id_, e_id_,
                                 declared());
}

void ExtSomeipClient::stop() {
    if (!started_) return;
    started_ = false;
    app_->unregister_availability_handler(s_id_, i_id_);
    app_->unregister_message_handler(s_id_, i_id_, e_id_);
    app_->release_event(s_id_, i_id_, e_id_);
    app_->unsubscribe(s_id_, i_id_, g_id_);
    app_->release_service(s_id_, i_id_);
}

void ExtSomeipClient::onMessage(const std::shared_ptr<vsomeip::message>& msg) {
    const int64_t t_ready_ns = bench::now_ns();  // raw bytes; no msg-object conversion
    auto payload = msg->get_payload();
    if (!payload) return;
    vsomeip::byte_t* ptr = payload->get_data();
    uint32_t len = payload->get_length();
    if (!ptr || len == 0) return;
    std::vector<uint8_t> data(ptr, ptr + len);

    const uint64_t seq_key = bench::read_slot(data, bench::Slot::SEQ_KEY);
    const int64_t t_leave = static_cast<int64_t>(bench::read_slot(data, bench::Slot::TS));
    const int64_t sink_mw_us = (t_ready_ns - t_leave) / 1000;
    spdlog::get("SMP_CLI")->trace("[Callback:S{:x}:E{:x}] | Seq: {} | SinkMW: {} us | Size: {} B", s_id_, e_id_,
                                  seq_key, sink_mw_us, data.size());

    if (on_message_) on_message_(data, t_ready_ns);
}
