#include "ext_someip_client.hpp"

#include <set>
#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"

ExtSomeipClient::ExtSomeipClient(std::shared_ptr<vsomeip::application> app, uint16_t service_id, uint16_t instance_id,
                                 uint16_t eventgroup_id, uint16_t event_id)
    : app_(std::move(app)), s_id_(service_id), i_id_(instance_id), g_id_(eventgroup_id), e_id_(event_id) {
    if (!app_) {
        throw std::runtime_error("ExtSomeipClient: null vsomeip app");
    }
}

ExtSomeipClient::~ExtSomeipClient() { stop(); }

void ExtSomeipClient::start() {
    if (started_) return;

    std::set<vsomeip::eventgroup_t> groups;
    groups.insert(g_id_);
    app_->request_event(s_id_, i_id_, e_id_, groups, vsomeip::event_type_e::ET_EVENT);
    app_->register_message_handler(s_id_, i_id_, e_id_,
                                   [this](const std::shared_ptr<vsomeip::message>& msg) { onMessage(msg); });
    app_->subscribe(s_id_, i_id_, g_id_);
    app_->request_service(s_id_, i_id_);

    started_ = true;
}

void ExtSomeipClient::stop() {
    if (!started_) return;
    started_ = false;
    app_->unregister_message_handler(s_id_, i_id_, e_id_);
    app_->release_event(s_id_, i_id_, e_id_);
    app_->unsubscribe(s_id_, i_id_, g_id_);
    app_->release_service(s_id_, i_id_);
}

void ExtSomeipClient::onMessage(const std::shared_ptr<vsomeip::message>& msg) {
    const int64_t t_bytes_ready_ns = bench::now_ns();
    auto payload = msg->get_payload();
    if (!payload) return;
    vsomeip::byte_t* ptr = payload->get_data();
    uint32_t len = payload->get_length();
    if (!ptr || len == 0) return;
    std::vector<uint8_t> data(ptr, ptr + len);

    const uint64_t seq_key = bench::read_slot(data, bench::Slot::SEQ_KEY);
    const int64_t t_bytes_leave = static_cast<int64_t>(bench::read_slot(data, bench::Slot::TS));
    const int64_t sink_mw_lumped_us = (t_bytes_ready_ns - t_bytes_leave) / 1000;
    spdlog::get("SMP_CLI")->trace("[Callback:S{:x}:E{:x}] | Seq: {} | SinkMW: {} us | Size: {} B",
                                  s_id_, e_id_, seq_key, sink_mw_lumped_us, data.size());

    if (on_message_) on_message_(data, t_bytes_ready_ns);
}
