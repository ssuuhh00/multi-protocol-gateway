#include "ext_zenoh_sub.hpp"

#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"
#include "zenoh/zenoh_qos_apply.hpp"

ExtZenohSub::ExtZenohSub(std::shared_ptr<zenoh::Session> session, EndpointSpec spec, const QosProfile& profile)
    : ExtSubscriber(std::move(spec)), session_(std::move(session)), profile_(profile) {
    if (!session_) throw std::runtime_error("ExtZenohSub: null session");
}

ExtZenohSub::~ExtZenohSub() { stop(); }

std::string ExtZenohSub::declared() const {
    if (!advanced_) return "default (no policy declared)";
    return zenoh_qos::describeSubscriber(profile_);
}

void ExtZenohSub::start() {
    if (started_) return;

    auto on_sample = [this](const zenoh::Sample& sample) {
        const int64_t t_ready_ns = bench::now_ns();  // raw bytes; no msg-object conversion
        std::vector<uint8_t> payload = sample.get_payload().as_vector();
        if (payload.empty()) return;

        const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
        const int64_t t_leave = static_cast<int64_t>(bench::read_slot(payload, bench::Slot::TS));
        const int64_t sink_mw_us = (t_ready_ns - t_leave) / 1000;
        spdlog::get("ZEN_SUB")->trace("[Callback:{}] | Seq: {} | SinkMW: {} us | Size: {} B", spec_.address, seq_key,
                                      sink_mw_us, payload.size());
        if (on_message_) on_message_(payload, t_ready_ns);
    };

    if (zenoh_qos::needsAdvancedSubscriber(profile_)) {
        advanced_.emplace(session_->ext().declare_advanced_subscriber(spec_.address, std::move(on_sample), []() {},
                                                                      zenoh_qos::buildSubscriberOptions(profile_)));
    } else {
        subscriber_.emplace(session_->declare_subscriber(spec_.address, std::move(on_sample), []() {}));
    }

    // Discovery token, under the reserved "@mpg" namespace. Always declared:
    // it is how the gateway's DiscoveryManager learns this endpoint exists, and
    // without it a Zenoh mapping never leaves the waiting state.
    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRoleSub, self_zid, spec_.address);
        liveliness_token_.emplace(session_->liveliness_declare_token(token_key));
    } catch (const std::exception& e) {
        spdlog::get("ZEN_SUB")->warn("[{}] Discovery token declare failed: {}", spec_.node_name, e.what());
    }

    // The consumer half of the LIVELINESS QoS contract, on the data key
    // expression. Without it the gateway's
    // token is declared into a network nobody is listening to, and a
    // "liveliness" row on the spec sheet would again be a claim with no
    // observer -- the state ext_node.hpp describes.
    if (profile_.liveliness) {
        const std::string name = spec_.node_name;
        zenoh::Session::LivelinessSubscriberOptions live_opts;
        live_opts.history = true;
        liveliness_.emplace(session_->liveliness_declare_subscriber(
            zenoh::KeyExpr(spec_.address),
            [name](const zenoh::Sample& s) {
                auto log = spdlog::get("ZEN_SUB");
                if (s.get_kind() == Z_SAMPLE_KIND_PUT) {
                    log->info("[{}] LIVELINESS: peer alive on '{}'", name, s.get_keyexpr().as_string_view());
                } else {
                    log->warn("[{}] LIVELINESS: peer lost on '{}'", name, s.get_keyexpr().as_string_view());
                }
            },
            []() {}, std::move(live_opts)));
    }
    started_ = true;
    spdlog::get("ZEN_SUB")->debug("[{}] up on '{}' as {}", spec_.node_name, spec_.address, declared());
}

void ExtZenohSub::stop() {
    if (!started_) return;
    started_ = false;
    liveliness_.reset();
    if (liveliness_token_.has_value()) {
        try {
            std::move(*liveliness_token_).undeclare();
        } catch (const std::exception&) {
        }
        liveliness_token_.reset();
    }
    advanced_.reset();
    subscriber_.reset();
}
