#include "ext_zenoh_pub.hpp"

#include <stdexcept>
#include <utility>

#include <spdlog/spdlog.h>

#include "common/bench_util.hpp"
#include "common/zenoh_liveliness.hpp"

ExtZenohPub::ExtZenohPub(std::shared_ptr<zenoh::Session> session, std::string key)
    : session_(std::move(session)), key_(std::move(key)) {
    if (!session_) {
        throw std::runtime_error("ExtZenohPub: null session");
    }
}

ExtZenohPub::~ExtZenohPub() { stop(); }

void ExtZenohPub::start() {
    if (started_) return;
    publisher_.emplace(session_->declare_publisher(key_));
    try {
        const std::string self_zid = session_->get_zid().to_string();
        const std::string token_key =
            mpg::zenoh_liveliness::make_token_key(mpg::zenoh_liveliness::kRolePub, self_zid, key_);
        liveliness_token_.emplace(session_->liveliness_declare_token(token_key));
    } catch (const std::exception& e) {
        spdlog::get("ZEN_PUB")->warn("[ExtZenohPub:{}] Liveliness declare failed: {}", key_, e.what());
    }
    started_ = true;
}

void ExtZenohPub::stop() {
    if (!started_) return;
    started_ = false;
    if (liveliness_token_.has_value()) {
        try {
            std::move(*liveliness_token_).undeclare();
        } catch (const std::exception&) {
        }
        liveliness_token_.reset();
    }
    publisher_.reset();
}

void ExtZenohPub::send(uint64_t seq_key, size_t payload_bytes) {
    if (!started_) return;
    std::vector<uint8_t> payload(payload_bytes, static_cast<uint8_t>('A'));
    bench::prepend_header(payload, seq_key);
    const size_t bench_size = payload.size();
    publisher_->put(zenoh::Bytes(std::move(payload)));
    spdlog::get("ZEN_PUB")->trace("[Send:{}] | Seq: {} | Size: {} B", key_, seq_key, bench_size);
}
