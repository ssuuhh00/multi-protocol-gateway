// gateway_demo: pair the user-side ends of a gateway mapping JSON.
//
// Reads the same mapping file the gateway loads, instantiates one ExtPublisher
// on the source side and one ExtSubscriber on the sink side of every mapping,
// then sends a stamped bench-header message every N ms and prints what arrives.
//
// Run alongside `multi_protocol_gateway <same.json>` to watch traffic flow
// through the gateway in two terminals.

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <fastdds/dds/domain/DomainParticipantFactory.hpp>
#include <vsomeip/vsomeip.hpp>
#include <zenoh.hxx>

#include "common/command.hpp"
#include "common/logger_config.hpp"
#include "ext_nodes/ext_dds_pub.hpp"
#include "ext_nodes/ext_dds_sub.hpp"
#include "ext_nodes/ext_node.hpp"
#include "ext_nodes/ext_someip_client.hpp"
#include "ext_nodes/ext_someip_server.hpp"
#include "ext_nodes/ext_zenoh_pub.hpp"
#include "ext_nodes/ext_zenoh_sub.hpp"
#include "json/json.hpp"
#include "ros2_msg_type_registry.hpp"

namespace {

volatile sig_atomic_t g_stop = 0;
void signal_handler(int) { g_stop = 1; }

ProtocolType parseProtocol(const std::string& s) {
    if (s == "DDS") return ProtocolType::DDS;
    if (s == "SOMEIP") return ProtocolType::SOMEIP;
    if (s == "ZENOH") return ProtocolType::ZENOH;
    throw std::runtime_error("Unknown protocol: " + s);
}

Qos parseQos(const std::string& s) {
    if (s == "RELIABLE") return Qos::RELIABLE;
    if (s == "BEST_EFFORT") return Qos::BEST_EFFORT;
    throw std::runtime_error("Unknown qos: " + s);
}

MessageType parseMessageType(const std::string& s) {
    MessageType t = DdsTypeRegistry::stringToEnum(s);
    if (t == MessageType::NONE) {
        throw std::runtime_error("Unknown message_type: " + s);
    }
    return t;
}

struct Pair {
    std::string label;
    bool bench_mode = false;
    std::unique_ptr<ExtPublisher> pub;
    std::unique_ptr<ExtSubscriber> sub;
    std::atomic<uint64_t> recv_count{0};
};

std::string routingLabel(ProtocolType src, ProtocolType sink) {
    auto s = [](ProtocolType p) {
        switch (p) {
            case ProtocolType::DDS: return "DDS";
            case ProtocolType::SOMEIP: return "SOMEIP";
            case ProtocolType::ZENOH: return "ZENOH";
            default: return "NONE";
        }
    };
    return std::string(s(src)) + "->" + s(sink);
}

}  // namespace

int main(int argc, char* argv[]) {
    std::signal(SIGINT, signal_handler);
    std::signal(SIGTERM, signal_handler);

    // ext_nodes_demo is compiled with DEFAULT_LOG_PATH="logs/demo.log" so this
    // is just a belt-and-suspenders default; a user-supplied LOG_PATH still wins.
    setenv("LOG_PATH", "logs/demo.log", 0);

    setup_global_logger();
    auto log_ = spdlog::get("DEMO");

    const char* mapping_env = std::getenv("GATEWAY_MAPPING_PATH");
    if (!mapping_env || !*mapping_env) {
        log_->error("Usage: {} [msg_count=10] [rate_hz=2] [payload_bytes=64]", argv[0]);
        log_->error("  Required env: GATEWAY_MAPPING_PATH (path to mappings.json, same as gateway)");
        log_->error("  Required env: ZENOH_CONFIG_PATH");
        log_->error("  Required env (if mapping uses SOMEIP): VSOMEIP_CONFIG_PATH, VSOMEIP_APP_NAME");
        return -1;
    }
    const std::string config_path = mapping_env;
    const uint64_t msg_count = (argc >= 2) ? std::stoull(argv[1]) : 10;
    const double rate_hz = (argc >= 3) ? std::stod(argv[2]) : 2.0;
    const size_t payload_bytes = (argc >= 4) ? std::stoul(argv[3]) : 64;
    const int settle_ms = 2000;

    nlohmann::json cfg;
    try {
        std::ifstream f(config_path);
        if (!f.is_open()) {
            log_->error("Cannot open config: {}", config_path);
            return -1;
        }
        f >> cfg;
    } catch (const std::exception& e) {
        log_->error("JSON parse error: {}", e.what());
        return -1;
    }

    if (!cfg.contains("mappings") || !cfg["mappings"].is_array()) {
        log_->error("Config must contain top-level 'mappings' array");
        return -1;
    }

    log_->info(">>> gateway_demo: config={}, count={}, rate={} Hz, payload={} bytes, mappings={}", config_path,
               msg_count, rate_hz, payload_bytes, cfg["mappings"].size());

    // Detect which backends are needed
    bool uses_dds = false, uses_zenoh = false, uses_someip = false;
    for (const auto& m : cfg["mappings"]) {
        ProtocolType src = parseProtocol(m.at("source").get<std::string>());
        ProtocolType sink = parseProtocol(m.at("sink").get<std::string>());
        if (src == ProtocolType::DDS || sink == ProtocolType::DDS) uses_dds = true;
        if (src == ProtocolType::ZENOH || sink == ProtocolType::ZENOH) uses_zenoh = true;
        if (src == ProtocolType::SOMEIP || sink == ProtocolType::SOMEIP) uses_someip = true;
    }

    // DDS DomainParticipant (shared)
    eprosima::fastdds::dds::DomainParticipant* participant = nullptr;
    auto* pf = eprosima::fastdds::dds::DomainParticipantFactory::get_instance();
    if (uses_dds) {
        int domain_id = 0;
        if (const char* env = std::getenv("ROS_DOMAIN_ID")) {
            try { domain_id = std::stoi(env); } catch (...) {}
        }
        participant = pf->create_participant(domain_id, eprosima::fastdds::dds::PARTICIPANT_QOS_DEFAULT);
        if (!participant) {
            log_->error("Failed to create DomainParticipant (domain={})", domain_id);
            return -1;
        }
        DdsTypeRegistry::registerAllTypes(participant, log_);
    }

    // Zenoh session (shared) — ZENOH_CONFIG_PATH env is required.
    std::shared_ptr<zenoh::Session> zenoh_session;
    if (uses_zenoh) {
        const char* env = std::getenv("ZENOH_CONFIG_PATH");
        if (!env) {
            log_->error("ZENOH_CONFIG_PATH env not set (required by Zenoh mappings)");
            if (participant) pf->delete_participant(participant);
            return -1;
        }
        try {
            auto config = zenoh::Config::from_file(env);
            zenoh_session = std::make_shared<zenoh::Session>(zenoh::Session::open(std::move(config)));
        } catch (const std::exception& e) {
            log_->error("Failed to open Zenoh session: {}", e.what());
            if (participant) pf->delete_participant(participant);
            return -1;
        }
    }

    // SOMEIP app (attaches to gateway routing master) — env required.
    std::shared_ptr<vsomeip::application> someip_app;
    std::thread someip_thread;
    if (uses_someip) {
        const char* cfg_env = std::getenv("VSOMEIP_CONFIG_PATH");
        const char* app_env = std::getenv("VSOMEIP_APP_NAME");
        if (!cfg_env || !app_env) {
            log_->error("VSOMEIP_CONFIG_PATH / VSOMEIP_APP_NAME env not set (required by SOMEIP mappings)");
            zenoh_session.reset();
            if (participant) pf->delete_participant(participant);
            return -1;
        }
        try {
            setenv("VSOMEIP_CONFIGURATION", cfg_env, 1);
            setenv("VSOMEIP_APPLICATION_NAME", app_env, 1);

            someip_app = vsomeip::runtime::get()->create_application(app_env);
            if (!someip_app) throw std::runtime_error("create_application returned nullptr");
            if (!someip_app->init()) throw std::runtime_error("application::init returned false");

            someip_thread = std::thread([&]() { someip_app->start(); });
            std::this_thread::sleep_for(std::chrono::seconds(1));
            log_->info(">>> vSomeIP demo app '{}' attached", app_env);
        } catch (const std::exception& e) {
            log_->error("vSomeIP init failed: {}", e.what());
            zenoh_session.reset();
            if (participant) pf->delete_participant(participant);
            return -1;
        }
    }

    // Build pairs from each mapping
    std::vector<std::unique_ptr<Pair>> pairs;
    try {
        for (const auto& m : cfg["mappings"]) {
            auto pr = std::make_unique<Pair>();
            ProtocolType src = parseProtocol(m.at("source").get<std::string>());
            ProtocolType sink = parseProtocol(m.at("sink").get<std::string>());
            MessageType mt = parseMessageType(m.at("message_type").get<std::string>());
            Qos qos = parseQos(m.at("qos").get<std::string>());
            pr->label = routingLabel(src, sink);
            pr->bench_mode = m.value("bench_mode", false);

            // Source-side publisher (the user's "producer")
            switch (src) {
                case ProtocolType::DDS:
                    pr->pub = std::make_unique<ExtDdsPub>(participant, m.at("topic").get<std::string>(), mt, qos);
                    break;
                case ProtocolType::ZENOH:
                    pr->pub = std::make_unique<ExtZenohPub>(zenoh_session, m.at("zenoh_key").get<std::string>());
                    break;
                case ProtocolType::SOMEIP:
                    pr->pub = std::make_unique<ExtSomeipServer>(
                        someip_app, m.at("service_id").get<uint16_t>(), m.at("instance_id").get<uint16_t>(),
                        m.at("eventgroup_id").get<uint16_t>(), m.at("event_id").get<uint16_t>());
                    break;
                default:
                    throw std::runtime_error("Unsupported source protocol");
            }

            // Sink-side subscriber (the user's "consumer")
            switch (sink) {
                case ProtocolType::DDS:
                    pr->sub = std::make_unique<ExtDdsSub>(participant, m.at("topic").get<std::string>(), mt, qos);
                    break;
                case ProtocolType::ZENOH:
                    pr->sub = std::make_unique<ExtZenohSub>(zenoh_session, m.at("zenoh_key").get<std::string>());
                    break;
                case ProtocolType::SOMEIP:
                    pr->sub = std::make_unique<ExtSomeipClient>(
                        someip_app, m.at("service_id").get<uint16_t>(), m.at("instance_id").get<uint16_t>(),
                        m.at("eventgroup_id").get<uint16_t>(), m.at("event_id").get<uint16_t>());
                    break;
                default:
                    throw std::runtime_error("Unsupported sink protocol");
            }

            Pair* pp = pr.get();
            pr->sub->setOnMessage([pp, log_](const std::vector<uint8_t>& payload, int64_t /*recv_ns*/) {
                pp->recv_count.fetch_add(1);
                const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
                const size_t bytes = payload.size() > bench::HEADER_LEN ? payload.size() - bench::HEADER_LEN : 0;
                log_->info("[{}] <- RECV seq_key=0x{:x} bytes={}", pp->label, seq_key, bytes);
            });

            pairs.push_back(std::move(pr));
        }
    } catch (const std::exception& e) {
        log_->error("Failed to build pairs: {}", e.what());
        return -1;
    }

    log_->info(">>> Starting {} pair(s)", pairs.size());
    for (auto& pr : pairs) {
        pr->sub->start();
        pr->pub->start();
    }

    log_->info(">>> Discovery settle: {} ms (gateway needs both ends visible)", settle_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(settle_ms));

    log_->info(">>> Sending {} message(s) per pair @ {} Hz. Ctrl+C aborts early.", msg_count, rate_hz);
    const auto period = std::chrono::microseconds(static_cast<int64_t>(1'000'000.0 / rate_hz));
    for (uint64_t seq = 0; seq < msg_count && !g_stop; ++seq) {
        for (auto& pr : pairs) {
            log_->info("[{}] -> SEND seq={} bytes={}", pr->label, seq, payload_bytes);
            pr->pub->send(seq, payload_bytes);
        }
        std::this_thread::sleep_for(period);
    }

    // Drain in-flight receives. Give the gateway up to 2s to deliver the last
    // batch before we stop subscribers.
    log_->info(">>> Draining for up to 2s...");
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < drain_deadline && !g_stop) {
        bool all_done = true;
        for (auto& pr : pairs) {
            if (pr->recv_count.load() < msg_count) {
                all_done = false;
                break;
            }
        }
        if (all_done) break;
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
    }

    log_->info(">>> Stopping...");
    for (auto& pr : pairs) {
        pr->pub->stop();
        pr->sub->stop();
        log_->info("[{}] total received: {}", pr->label, pr->recv_count.load());
    }
    pairs.clear();

    zenoh_session.reset();
    if (someip_app) {
        someip_app->stop();
        if (someip_thread.joinable()) someip_thread.join();
    }
    if (participant) {
        participant->delete_contained_entities();
        pf->delete_participant(participant);
    }

    log_->info(">>> gateway_demo exited cleanly.");
    return 0;
}
