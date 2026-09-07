#include "ext_zenoh_pub.hpp"

#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"
#include "zenoh/zenoh_qos_apply.hpp"

ExtZenohPub::ExtZenohPub(std::shared_ptr<zenoh::Session> session, EndpointSpec spec, const QosProfile& profile)
    : ExtPublisher(std::move(spec)), session_(std::move(session)), profile_(profile) {
    if (!session_) throw std::runtime_error("ExtZenohPub: null session");
}

ExtZenohPub::~ExtZenohPub() { stop(); }

std::string ExtZenohPub::declared() const {
    // Still deliberately literal: this reports what was handed to the API, not
    // what the mapping asked for. An undeclared mapping hands declare_publisher
    // no options at all, and saying otherwise is the exact mistake this column
    // exists to prevent.
    if (!advanced_) return "default (no policy declared)";
    return zenoh_qos::describePublisher(profile_);
}

void ExtZenohPub::start() {
    if (started_) return;
    if (zenoh_qos::needsAdvancedPublisher(profile_)) {
        advanced_.emplace(session_->ext().declare_advanced_publisher(spec_.address,
                                                                     zenoh_qos::buildPublisherOptions(profile_)));
    } else {
        publisher_.emplace(session_->declare_publisher(spec_.address));
    }

    // Discovery token, under the reserved "@mpg" namespace. Always declared:
    // it is how the gateway's DiscoveryManager learns this endpoint exists, and
    // without it a Zenoh mapping never leaves the waiting state.
    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRolePub, self_zid, spec_.address);
        liveliness_token_.emplace(session_->liveliness_declare_token(token_key));
    } catch (const std::exception& e) {
        spdlog::get("ZEN_PUB")->warn("[{}] Discovery token declare failed: {}", spec_.node_name, e.what());
    }

    // LIVELINESS QoS token, on the data key expression, only when this endpoint
    // declared the policy. This is the one the gateway watches as the source's
    // assertion.
    if (profile_.liveliness) {
        liveliness_.emplace(session_->liveliness_declare_token(zenoh::KeyExpr(spec_.address)));
    }

    started_ = true;
    spdlog::get("ZEN_PUB")->debug("[{}] up on '{}' as {}", spec_.node_name, spec_.address, declared());
}

void ExtZenohPub::stop() {
    if (!started_) return;
    started_ = false;
    // Undeclare the token first: it is the thing the peer is watching, and the
    // DELETE it produces is the event this endpoint exists to generate.
    liveliness_.reset();
    if (liveliness_token_.has_value()) {
        try {
            std::move(*liveliness_token_).undeclare();
        } catch (const std::exception&) {
        }
        liveliness_token_.reset();
    }
    advanced_.reset();
    publisher_.reset();
}

int64_t ExtZenohPub::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return 0;
    std::vector<uint8_t> payload(payload_bytes, static_cast<uint8_t>('A'));
    const int64_t t_pub_send = bench::prepend_header(payload, seq_key);
    if (stamp_) stamp_(payload, seq_key);
    const size_t bench_size = payload.size();
    if (advanced_) {
        advanced_->put(zenoh::Bytes(std::move(payload)));
    } else {
        publisher_->put(zenoh::Bytes(std::move(payload)));
    }
    // No msg-object conversion on the Zenoh leg, so no Conv field.
    spdlog::get("ZEN_PUB")->trace("[Send:{}] | Seq: {} | Size: {} B", spec_.address, seq_key, bench_size);
    return t_pub_send;
}
