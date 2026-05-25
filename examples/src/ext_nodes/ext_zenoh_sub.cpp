#include "ext_zenoh_sub.hpp"

#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"

ExtZenohSub::ExtZenohSub(std::shared_ptr<zenoh::Session> session, std::string key)
    : session_(std::move(session)), key_(std::move(key)) {
    if (!session_) {
        throw std::runtime_error("ExtZenohSub: null session");
    }
}

ExtZenohSub::~ExtZenohSub() { stop(); }

void ExtZenohSub::start() {
    if (started_) return;

    auto on_sample = [this](const zenoh::Sample& sample) {
        const int64_t t_bytes_ready_ns = bench::now_ns();
        std::vector<uint8_t> payload = sample.get_payload().as_vector();
        if (!payload.empty()) {
            const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
            const int64_t t_bytes_leave = static_cast<int64_t>(bench::read_slot(payload, bench::Slot::TS));
            const int64_t sink_mw_lumped_us = (t_bytes_ready_ns - t_bytes_leave) / 1000;
            spdlog::get("ZEN_SUB")->trace("[Callback:{}] | Seq: {} | SinkMW: {} us | Size: {} B",
                                          key_, seq_key, sink_mw_lumped_us, payload.size());
            if (on_message_) on_message_(payload, t_bytes_ready_ns);
        }
    };
    auto on_drop = []() {};

    subscriber_.emplace(session_->declare_subscriber(key_, std::move(on_sample), std::move(on_drop)));
    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRoleSub, self_zid, key_);
        liveliness_token_.emplace(session_->liveliness_declare_token(token_key));
    } catch (const std::exception& e) {
        spdlog::get("ZEN_SUB")->warn("[ExtZenohSub:{}] Liveliness declare failed: {}", key_, e.what());
    }
    started_ = true;
}

void ExtZenohSub::stop() {
    if (!started_) return;
    started_ = false;
    if (liveliness_token_.has_value()) {
        try {
            std::move(*liveliness_token_).undeclare();
        } catch (const std::exception&) {
        }
        liveliness_token_.reset();
    }
    subscriber_.reset();
}
