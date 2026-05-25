#include "ext_someip_server.hpp"

#include <set>
#include <stdexcept>
#include <utility>
#include <vector>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"

ExtSomeipServer::ExtSomeipServer(std::shared_ptr<vsomeip::application> app, uint16_t service_id, uint16_t instance_id,
                                 uint16_t eventgroup_id, uint16_t event_id)
    : app_(std::move(app)), s_id_(service_id), i_id_(instance_id), g_id_(eventgroup_id), e_id_(event_id) {
    if (!app_) {
        throw std::runtime_error("ExtSomeipServer: null vsomeip app");
    }
}

ExtSomeipServer::~ExtSomeipServer() { stop(); }

void ExtSomeipServer::start() {
    if (started_) return;

    std::set<vsomeip::eventgroup_t> groups;
    groups.insert(g_id_);
    app_->offer_event(s_id_, i_id_, e_id_, groups);
    app_->offer_service(s_id_, i_id_);

    started_ = true;
}

void ExtSomeipServer::stop() {
    if (!started_) return;
    started_ = false;
    app_->stop_offer_event(s_id_, i_id_, e_id_);
    app_->stop_offer_service(s_id_, i_id_);
}

void ExtSomeipServer::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return;
    std::vector<uint8_t> data(payload_bytes, static_cast<uint8_t>('A'));
    bench::prepend_header(data, seq_key);
    const size_t bench_size = data.size();

    auto payload = vsomeip::runtime::get()->create_payload();
    payload->set_data(std::move(data));
    app_->notify(s_id_, i_id_, e_id_, payload);

    spdlog::get("SMP_SRV")->trace("[Send:S{:x}:E{:x}] | Seq: {} | Size: {} B",
                                  s_id_, e_id_, seq_key, bench_size);
}
