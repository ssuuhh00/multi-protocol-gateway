// gateway_demo: stands up the user-side ends of every mapping in a gateway
// mapping file, so the gateway has something real on both sides of each bridge.
//
// Reads the same JSON the gateway loads, builds one ExtPublisher on the source
// side and one ExtSubscriber on the sink side of every mapping, then drives
// traffic and reports what comes back.
//
// Run alongside `multi_protocol_gateway <same.json>`:
//   term 1: multi_protocol_gateway examples/mappings/all_directions.json
//   term 2: gateway_demo --duration inf --rate 100
//
// Two knobs exist for reasons beyond convenience:
//   --duration     the endpoints must outlive a glance. A fixed message count
//                  ends before anything can be observed or captured.
//   --sink-delay-ms  slows the consumer callback. Every bridge is a
//                  single-producer/single-consumer queue drained by a dedicated
//                  blocking worker, so depth sits at 0-1 no matter the rate
//                  unless the consumer is made slower than the producer. This
//                  is the only way to make queue growth visible.
//   --late-join-ms attaches an extra subscriber to each DDS/Zenoh sink this
//                  long into the run. Durability is the one policy whose effect
//                  cannot be seen by an endpoint that was there from the start,
//                  so without a latecomer the gateway's TRANSIENT_LOCAL is a
//                  call that was made rather than a behaviour that was observed.

#include <spdlog/spdlog.h>

#include <algorithm>
#include <atomic>
#include <chrono>
#include <csignal>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <iomanip>
#include <memory>
#include <mutex>
#include <sstream>
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

// The mapping's qos_policies block, parsed the same way the gateway parses it.
// The endpoints need it because a QoS declaration is a contract between two
// peers: if only the gateway declares it, the DDS request/offer rules can leave
// the pair unmatched and the bridge carries nothing -- which looks like a broken
// gateway and is really a demo that was not playing the same game.
QosProfile parseQosPolicies(const nlohmann::json& j) {
    QosProfile p;
    if (!j.is_object()) throw std::runtime_error("qos_policies must be an object");
    for (auto it = j.begin(); it != j.end(); ++it) {
        const std::string& k = it.key();
        if (k == "reliability") {
            const auto v = it->get<std::string>();
            p.reliability = (v == "RELIABLE") ? ReliabilityKind::RELIABLE : ReliabilityKind::BEST_EFFORT;
        } else if (k == "history") {
            HistorySpec h;
            if (it->is_string()) {
                h.kind = it->get<std::string>() == "KEEP_ALL" ? HistoryKind::KEEP_ALL : HistoryKind::KEEP_LAST;
            } else {
                h.kind = (*it).at("kind").get<std::string>() == "KEEP_ALL" ? HistoryKind::KEEP_ALL
                                                                          : HistoryKind::KEEP_LAST;
                h.depth = it->value("depth", 1);
            }
            p.history = h;
        } else if (k == "deadline_ms") {
            p.deadline_ms = it->get<uint32_t>();
        } else if (k == "liveliness") {
            // Same rule as the gateway's parser: a boolean, nothing else. A
            // kind or a lease would be a value nobody enforces.
            if (!it->is_boolean()) throw std::runtime_error("liveliness takes no parameters: write \"liveliness\": true");
            if (it->get<bool>()) p.liveliness = LivelinessSpec{};
        } else if (k == "durability") {
            p.durability = it->get<std::string>() == "TRANSIENT_LOCAL" ? DurabilityKind::TRANSIENT_LOCAL
                                                                      : DurabilityKind::VOLATILE;
        } else {
            throw std::runtime_error("unknown qos policy '" + k + "'");
        }
    }
    return p;
}

MessageType parseMessageType(const std::string& s) {
    MessageType t = DdsTypeRegistry::stringToEnum(s);
    if (t == MessageType::NONE) {
        throw std::runtime_error("Unknown message_type: " + s);
    }
    return t;
}

const char* protoName(ProtocolType p) {
    switch (p) {
        case ProtocolType::DDS: return "DDS";
        case ProtocolType::SOMEIP: return "SOMEIP";
        case ProtocolType::ZENOH: return "ZENOH";
        default: return "NONE";
    }
}

std::string routingLabel(ProtocolType src, ProtocolType sink) {
    return std::string(protoName(src)) + "->" + protoName(sink);
}

// What the endpoint binds to, per protocol. Also what the monitor shows as the
// endpoint's address, so it must read the way the config does.
std::string addressOf(ProtocolType p, const nlohmann::json& m) {
    switch (p) {
        case ProtocolType::DDS:
            return m.at("topic").get<std::string>();
        case ProtocolType::ZENOH:
            return m.at("zenoh_key").get<std::string>();
        case ProtocolType::SOMEIP: {
            std::ostringstream os;
            os << "S" << std::hex << std::setw(4) << std::setfill('0') << m.at("service_id").get<uint16_t>() << ":E"
               << std::setw(4) << std::setfill('0') << m.at("event_id").get<uint16_t>();
            return os.str();
        }
        default:
            return "?";
    }
}

struct Pair {
    std::string label;
    bool bench_mode = false;
    uint32_t map_idx = 0;
    std::unique_ptr<ExtPublisher> pub;
    std::unique_ptr<ExtSubscriber> sub;
    std::atomic<uint64_t> sent{0};
    std::atomic<uint64_t> recv_count{0};
    std::atomic<uint64_t> last_recv_seq{0};

    // Late-join observer: joins after the stream is running, to see whether a
    // declared durability actually retains anything for it.
    //
    // A SECOND subscriber on the same sink address, started only after the
    // bridge has been carrying traffic for a while. It is deliberately not the
    // pair's own consumer: on a DDS or SOME/IP sink the gateway opens the
    // bridge only once the consumer is discovered, so delaying the real one
    // would delay the bridge itself and there would be no past samples for
    // anything to have retained. Adding a third party instead leaves the
    // measured path untouched and asks the only question durability is about --
    // does an endpoint that arrives late get what it missed.
    std::unique_ptr<ExtSubscriber> late;
    std::atomic<uint64_t> late_recv{0};
    std::atomic<uint64_t> late_historical{0};
    std::atomic<uint64_t> late_first_hist_seq{UINT64_MAX};
    std::atomic<uint64_t> late_last_hist_seq{0};
    // Written by the send loop before the observer is started, read from the
    // observer's middleware callback thread. Atomic rather than plain: the
    // ordering is obvious to a reader but not to the compiler.
    std::atomic<uint64_t> late_join_seq{0};  // first seq NOT yet published when it joined

};

struct Args {
    uint64_t msg_count = 10;
    double rate_hz = 2.0;
    size_t payload_bytes = 64;
    int settle_ms = 2000;
    int sink_delay_ms = 0;
    int late_join_ms = 0;      // >0: attach a second sink-side subscriber this late
    // A window in which every source stops publishing and then resumes. Deadline
    // and liveliness are the two policies whose whole point is what happens when
    // data stops coming, and nothing else in this harness can make that happen:
    // a run that never goes quiet cannot tell an armed listener from an absent
    // one. seq does NOT advance across the gap -- the stream pauses, it does not
    // lose messages -- so recv == sent still holds and the misses are the only
    // signal.
    int gap_at_ms = 0;         // >0: silence starts this long after the first send
    int gap_ms = 0;            // how long the silence lasts
    double duration_s = 0.0;   // >0: run for this long; <0: run until Ctrl-C
    bool duration_set = false;
};

// Positional [count] [rate] [payload] is kept for compatibility with the way
// this demo has always been invoked; flags may follow in any order.
Args parseArgs(int argc, char* argv[]) {
    Args a;
    int pos = 0;
    for (int i = 1; i < argc; ++i) {
        std::string t = argv[i];
        auto next = [&](const char* what) -> std::string {
            if (i + 1 >= argc) throw std::runtime_error(std::string(what) + " needs a value");
            return argv[++i];
        };
        if (t == "--duration") {
            std::string v = next("--duration");
            a.duration_set = true;
            a.duration_s = (v == "inf" || v == "forever") ? -1.0 : std::stod(v);
        } else if (t == "--rate") {
            a.rate_hz = std::stod(next("--rate"));
        } else if (t == "--payload") {
            a.payload_bytes = std::stoul(next("--payload"));
        } else if (t == "--count") {
            a.msg_count = std::stoull(next("--count"));
        } else if (t == "--sink-delay-ms") {
            a.sink_delay_ms = std::stoi(next("--sink-delay-ms"));
        } else if (t == "--late-join-ms") {
            a.late_join_ms = std::stoi(next("--late-join-ms"));
        } else if (t == "--gap-at-ms") {
            a.gap_at_ms = std::stoi(next("--gap-at-ms"));
        } else if (t == "--gap-ms") {
            a.gap_ms = std::stoi(next("--gap-ms"));
        } else if (t == "--settle-ms") {
            a.settle_ms = std::stoi(next("--settle-ms"));
        } else if (t.rfind("--", 0) == 0) {
            throw std::runtime_error("Unknown flag: " + t);
        } else {
            if (pos == 0) a.msg_count = std::stoull(t);
            else if (pos == 1) a.rate_hz = std::stod(t);
            else if (pos == 2) a.payload_bytes = std::stoul(t);
            else throw std::runtime_error("Too many positional arguments: " + t);
            ++pos;
        }
    }
    return a;
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

    Args args;
    try {
        args = parseArgs(argc, argv);
    } catch (const std::exception& e) {
        log_->error("{}", e.what());
        log_->error("Usage: {} [count] [rate_hz] [payload_bytes]", argv[0]);
        log_->error("       [--count N] [--rate HZ] [--payload BYTES]");
        log_->error("       [--duration SEC|inf] [--sink-delay-ms N] [--settle-ms N] [--late-join-ms N]");
        log_->error("       [--gap-at-ms N --gap-ms N]   sources go silent for a window, then resume");
        return -1;
    }

    const char* mapping_env = std::getenv("GATEWAY_MAPPING_PATH");
    if (!mapping_env || !*mapping_env) {
        log_->error("Required env: GATEWAY_MAPPING_PATH (path to mappings.json, same file the gateway loads)");
        log_->error("Required env: ZENOH_CONFIG_PATH");
        log_->error("Required env (if any mapping uses SOMEIP): VSOMEIP_CONFIG_PATH, VSOMEIP_APP_NAME");
        return -1;
    }
    const std::string config_path = mapping_env;

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

    // The mapping path on its own line: it is absolute, and putting it in front
    // of the numbers pushes them off the right edge of a normal terminal.
    log_->info(">>> gateway_demo: {} mapping(s) from {}", cfg["mappings"].size(), config_path);
    if (args.duration_set) {
        log_->info(">>> duration={}, rate={} Hz, payload={} B",
                   args.duration_s < 0 ? std::string("until Ctrl-C") : std::to_string(args.duration_s) + " s",
                   args.rate_hz, args.payload_bytes);
    } else {
        log_->info(">>> count={} per pair, rate={} Hz, payload={} B", args.msg_count, args.rate_hz,
                   args.payload_bytes);
    }
    if (args.sink_delay_ms > 0) {
        log_->warn(">>> sink-delay {} ms is ON: consumers are deliberately slower than producers.", args.sink_delay_ms);
    }

    // Which backends the mapping file actually needs.
    bool uses_dds = false, uses_zenoh = false, uses_someip = false;
    for (const auto& m : cfg["mappings"]) {
        ProtocolType src = parseProtocol(m.at("source").get<std::string>());
        ProtocolType sink = parseProtocol(m.at("sink").get<std::string>());
        if (src == ProtocolType::DDS || sink == ProtocolType::DDS) uses_dds = true;
        if (src == ProtocolType::ZENOH || sink == ProtocolType::ZENOH) uses_zenoh = true;
        if (src == ProtocolType::SOMEIP || sink == ProtocolType::SOMEIP) uses_someip = true;
    }

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

    // One producer + one consumer per mapping.
    std::vector<std::unique_ptr<Pair>> pairs;
    try {
        uint32_t idx = 0;
        for (const auto& m : cfg["mappings"]) {
            auto pr = std::make_unique<Pair>();
            ProtocolType src = parseProtocol(m.at("source").get<std::string>());
            ProtocolType sink = parseProtocol(m.at("sink").get<std::string>());
            MessageType mt = parseMessageType(m.at("message_type").get<std::string>());
            Qos qos = parseQos(m.at("qos").get<std::string>());
            QosProfile profile;
            if (m.contains("qos_policies")) profile = parseQosPolicies(m["qos_policies"]);
            pr->label = routingLabel(src, sink);
            pr->bench_mode = m.value("bench_mode", false);
            pr->map_idx = idx;

            // Names are the endpoint's own; the gateway has no field to carry
            // them. A mapping may override, otherwise they are derived so every
            // endpoint still has a stable identity to report.
            EndpointSpec pspec;
            pspec.role = ExtRole::PRODUCER;
            pspec.mapping_index = idx;
            pspec.address = addressOf(src, m);
            pspec.node_name = m.value("producer_name", std::string("ext_") + protoName(src) + "_pub_" +
                                                           std::to_string(idx));

            EndpointSpec cspec;
            cspec.role = ExtRole::CONSUMER;
            cspec.mapping_index = idx;
            cspec.address = addressOf(sink, m);
            cspec.node_name = m.value("consumer_name", std::string("ext_") + protoName(sink) + "_sub_" +
                                                           std::to_string(idx));

            switch (src) {
                case ProtocolType::DDS:
                    pr->pub = std::make_unique<ExtDdsPub>(participant, std::move(pspec), mt, qos, profile);
                    break;
                case ProtocolType::ZENOH:
                    pr->pub = std::make_unique<ExtZenohPub>(zenoh_session, std::move(pspec), profile);
                    break;
                case ProtocolType::SOMEIP:
                    pr->pub = std::make_unique<ExtSomeipServer>(
                        someip_app, std::move(pspec), m.at("service_id").get<uint16_t>(),
                        m.at("instance_id").get<uint16_t>(), m.at("eventgroup_id").get<uint16_t>(),
                        m.at("event_id").get<uint16_t>(), profile);
                    break;
                default:
                    throw std::runtime_error("Unsupported source protocol");
            }

            switch (sink) {
                case ProtocolType::DDS:
                    pr->sub = std::make_unique<ExtDdsSub>(participant, std::move(cspec), mt, qos, profile);
                    break;
                case ProtocolType::ZENOH:
                    pr->sub = std::make_unique<ExtZenohSub>(zenoh_session, std::move(cspec), profile);
                    break;
                case ProtocolType::SOMEIP:
                    pr->sub = std::make_unique<ExtSomeipClient>(
                        someip_app, std::move(cspec), m.at("service_id").get<uint16_t>(),
                        m.at("instance_id").get<uint16_t>(), m.at("eventgroup_id").get<uint16_t>(),
                        m.at("event_id").get<uint16_t>(), profile);
                    break;
                default:
                    throw std::runtime_error("Unsupported sink protocol");
            }

            Pair* pp = pr.get();
            const int delay_ms = args.sink_delay_ms;
            pr->sub->setOnMessage([pp, log_, delay_ms](const std::vector<uint8_t>& payload, int64_t /*recv_ns*/) {
                pp->recv_count.fetch_add(1, std::memory_order_relaxed);
                const uint64_t seq_key = bench::read_slot(payload, bench::Slot::SEQ_KEY);
                pp->last_recv_seq.store(bench::decode_local_seq(seq_key), std::memory_order_relaxed);
                const size_t bytes = payload.size() > bench::HEADER_LEN ? payload.size() - bench::HEADER_LEN : 0;
                // trace, not info: at 100 Hz across six pairs this is 1200
                // lines/s into an async queue whose overflow policy is block,
                // which would make the logger part of what is being measured.
                log_->trace("[{}] <- RECV seq_key=0x{:x} bytes={}", pp->label, seq_key, bytes);
                if (delay_ms > 0) std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            });

            // The late-join observer, built now and started later.
            //
            // SOME/IP sinks are excluded, and not for convenience: one vsomeip
            // application registers an event once per (service, instance,
            // event), so a second client on the same event inside this process
            // is not a late joiner at all -- it is the same registration. The
            // SOME/IP retention question is answered instead by the ET_FIELD
            // cells, where the subscriber genuinely arrives after the offer.
            if (args.late_join_ms > 0 && (sink == ProtocolType::DDS || sink == ProtocolType::ZENOH)) {
                EndpointSpec lspec;
                lspec.role = ExtRole::CONSUMER;
                lspec.mapping_index = idx;
                lspec.address = addressOf(sink, m);
                lspec.node_name = std::string("late_") + protoName(sink) + "_sub_" + std::to_string(idx);
                if (sink == ProtocolType::DDS) {
                    pr->late = std::make_unique<ExtDdsSub>(participant, std::move(lspec), mt, qos, profile);
                } else {
                    pr->late = std::make_unique<ExtZenohSub>(zenoh_session, std::move(lspec), profile);
                }
                pr->late->setOnMessage([pp](const std::vector<uint8_t>& payload, int64_t) {
                    pp->late_recv.fetch_add(1, std::memory_order_relaxed);
                    const uint64_t s = bench::decode_local_seq(bench::read_slot(payload, bench::Slot::SEQ_KEY));
                    // Anything numbered below the seq that was still unsent
                    // when this endpoint joined was published before it
                    // existed. That is the whole measurement.
                    if (s < pp->late_join_seq.load(std::memory_order_relaxed)) {
                        pp->late_historical.fetch_add(1, std::memory_order_relaxed);
                        uint64_t lo = pp->late_first_hist_seq.load(std::memory_order_relaxed);
                        while (s < lo && !pp->late_first_hist_seq.compare_exchange_weak(lo, s)) {
                        }
                        uint64_t hi = pp->late_last_hist_seq.load(std::memory_order_relaxed);
                        while (s > hi && !pp->late_last_hist_seq.compare_exchange_weak(hi, s)) {
                        }
                    }
                });
            }

            pairs.push_back(std::move(pr));
            ++idx;
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

    log_->info(">>> Discovery settle: {} ms (the gateway opens a bridge only once both ends are visible)",
               args.settle_ms);
    std::this_thread::sleep_for(std::chrono::milliseconds(args.settle_ms));

    // Printed only after the settle window. declared() folds in live discovery
    // state (SOME/IP service availability, subscriber presence), and before
    // discovery converges those fields read DOWN/NO on a perfectly healthy
    // link -- a summary that lies at startup is worse than no summary.
    log_->info(">>> External endpoints (post-discovery):");
    for (size_t i = 0; i < pairs.size(); ++i) {
        const auto& pr = pairs[i];
        // Width 47 is the longest declared() any endpoint produces (a Zenoh
        // publisher with all four policies). Fixed, so the columns line up
        // whatever the mapping declares.
        log_->info("  [{}] {:<13} | {:>6} {:<17} {:<47} -> {:>6} {:<17} {}", i, pr->label, pr->pub->protocol(),
                   pr->pub->spec().node_name, pr->pub->declared(), pr->sub->protocol(), pr->sub->spec().node_name,
                   pr->sub->declared());
    }


    const auto period = std::chrono::microseconds(static_cast<int64_t>(1'000'000.0 / args.rate_hz));
    const auto t_start = std::chrono::steady_clock::now();
    const bool timed = args.duration_set;
    const bool forever = timed && args.duration_s < 0;

    if (timed) {
        log_->info(">>> Sending @ {} Hz for {}. Ctrl+C stops.", args.rate_hz,
                   forever ? std::string("as long as it runs") : std::to_string(args.duration_s) + " s");
    } else {
        log_->info(">>> Sending {} message(s) per pair @ {} Hz. Ctrl+C aborts early.", args.msg_count, args.rate_hz);
    }

    auto next_report = t_start + std::chrono::seconds(1);
    uint64_t seq = 0;
    bool late_started = false;
    const auto late_at = t_start + std::chrono::milliseconds(args.late_join_ms);

    // Both ends of the silence are fixed before the loop for the same reason the
    // late joiner is attached inside it: the seq either side of the gap has to be
    // the one the send loop actually used, not one a timer thread guessed at.
    const bool gap_wanted = args.gap_at_ms > 0 && args.gap_ms > 0;
    const auto gap_from = t_start + std::chrono::milliseconds(args.gap_at_ms);
    const auto gap_to = gap_from + std::chrono::milliseconds(args.gap_ms);
    bool in_gap = false;
    while (!g_stop) {
        if (timed) {
            if (!forever) {
                const double elapsed = std::chrono::duration<double>(std::chrono::steady_clock::now() - t_start).count();
                if (elapsed >= args.duration_s) break;
            }
        } else if (seq >= args.msg_count) {
            break;
        }

        const auto tick = std::chrono::steady_clock::now();
        const bool silent = gap_wanted && tick >= gap_from && tick < gap_to;
        if (silent != in_gap) {
            in_gap = silent;
            // Printed either side so the judgment script has an anchor: every
            // deadline miss and liveliness transition worth counting falls
            // between these two lines.
            log_->info(">>> source {} at seq {}", silent ? "silent" : "resumed", seq);
        }

        if (!silent) {
            for (auto& pr : pairs) {
                log_->trace("[{}] -> SEND seq={} bytes={}", pr->label, seq, args.payload_bytes);
                pr->pub->send(seq, args.payload_bytes);
                pr->sent.fetch_add(1, std::memory_order_relaxed);
            }
            ++seq;
        }

        // One status line per second: enough to watch, quiet enough not to
        // become the load. A pair whose recv lags its sent is the visible sign
        // that something is holding messages -- which is what the queue work
        // will explain.
        const auto now = std::chrono::steady_clock::now();

        // Attach the late joiners. Done here rather than on a timer thread so
        // that the seq recorded as the join point is exactly the one the send
        // loop is about to use -- a separate thread would have to guess.
        if (args.late_join_ms > 0 && !late_started && now >= late_at) {
            late_started = true;
            for (auto& pr : pairs) {
                if (!pr->late) continue;
                pr->late_join_seq.store(seq, std::memory_order_relaxed);
                try {
                    pr->late->start();
                    log_->info(">>> late join [{}] {} attached after seq {} was published", pr->map_idx,
                               pr->late->spec().node_name, seq == 0 ? 0 : seq - 1);
                } catch (const std::exception& e) {
                    log_->error(">>> late join [{}] failed: {}", pr->map_idx, e.what());
                    pr->late.reset();
                }
            }
        }

        if (now >= next_report) {
            std::ostringstream os;
            for (size_t i = 0; i < pairs.size(); ++i) {
                const uint64_t s = pairs[i]->sent.load(std::memory_order_relaxed);
                const uint64_t r = pairs[i]->recv_count.load(std::memory_order_relaxed);
                os << "[" << i << "] " << pairs[i]->label << " " << r << "/" << s << "   ";
            }
            log_->info("recv/sent  {}", os.str());
            next_report = now + std::chrono::seconds(1);
        }
        std::this_thread::sleep_for(period);
    }

    // Let in-flight messages land before tearing the consumers down.
    log_->info(">>> Draining for up to 2s...");
    const auto drain_deadline = std::chrono::steady_clock::now() + std::chrono::seconds(2);
    while (std::chrono::steady_clock::now() < drain_deadline) {
        bool all_done = true;
        for (auto& pr : pairs) {
            if (pr->recv_count.load() < pr->sent.load()) {
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
        const uint64_t s = pr->sent.load(), r = pr->recv_count.load();
        log_->info("[{}] sent {} / received {}{}", pr->label, s, r,
                   r < s ? "   <-- " + std::to_string(s - r) + " never arrived" : "");
    }

    // Late-join result. Printed apart from the delivery totals because it
    // answers a different question: not "did the bridge carry everything" but
    // "did an endpoint arriving mid-stream get what it had missed".
    if (late_started) {
        log_->info(">>> Late-join (durability seen from outside the gateway):");
        for (auto& pr : pairs) {
            if (!pr->late) continue;
            pr->late->stop();
            const uint64_t join = pr->late_join_seq.load();
            const uint64_t hist = pr->late_historical.load();
            const uint64_t tot = pr->late_recv.load();
            if (hist == 0) {
                log_->info("  [{}] {:<13} {:>6}  joined after seq {}: {} received, none of them older",
                           pr->map_idx, pr->label, pr->late->protocol(), join == 0 ? 0 : join - 1, tot);
            } else {
                log_->info("  [{}] {:<13} {:>6}  joined after seq {}: {} received, {} of them published "
                           "before it existed (seq {}..{})",
                           pr->map_idx, pr->label, pr->late->protocol(), join == 0 ? 0 : join - 1, tot, hist,
                           pr->late_first_hist_seq.load(), pr->late_last_hist_seq.load());
            }
        }
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
