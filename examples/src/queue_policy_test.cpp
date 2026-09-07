/**
 * @file queue_policy_test.cpp
 * @brief Deterministic tests for the bounded bridge queue and the QoS spec
 *        sheet, plus the printer that produces the sheet itself.
 *
 * Why this exists as a separate binary rather than as a live scenario.
 *
 * The bridge queue only grows when the sink leg is slower than the source leg,
 * and on one host that condition is not reachable: examples/README.md records
 * the measurement -- with `--sink-delay-ms` the external consumer falls hundreds
 * of messages behind while the queue sits at `depth 0, enq == deq, peak 1`,
 * because the sink worker hands each sample to the middleware and returns. The
 * backlog forms in the middleware's own buffers, downstream of us. Pushing 256
 * KB at 47 MB/s does not change it either.
 *
 * So a live demo cannot show a single drop, and a live demo that shows no drops
 * is not evidence that the bound works -- it is evidence of nothing. What
 * saturates the queue is a genuinely slower sink under sustained load, which is
 * what a loaded bridge measures end to end. Here the queue is driven directly,
 * where the overflow is exact rather than hoped for, and the properties that
 * the response letter will state are the assertions below.
 *
 * The spec sheet is tested the same way: the assertions are the sentences the
 * letter makes about it (deadline is a DDS-sink policy; history is settled by
 * the sink and a SOME/IP sink never offers a depth; ...), and `--matrix` prints
 * the sheet from the same function the gateway runs at startup, so the table in
 * the letter is produced by code rather than by hand.
 *
 * Run: build/examples/queue_policy_test              (exit 0 = all passed)
 *      build/examples/queue_policy_test --matrix     the spec sheet, 5 x 6
 *      build/examples/queue_policy_test --matrix long  + what each leg builds, per cell
 *      build/examples/queue_policy_test --matrix csv   the same, for Exp8_QoS_Mapping.csv
 */

#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <filesystem>
#include <fstream>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

#include "common/command.hpp"
#include "common/connection_queue.hpp"
#include "common/qos_profile.hpp"
#include "common/qos_spec.hpp"
#include "discovery/discovery_mapping.hpp"

namespace {

int g_pass = 0;
int g_fail = 0;
std::string g_case;

void begin(const std::string& name) {
    g_case = name;
    std::printf("\n-- %s\n", name.c_str());
}

void check(bool ok, const std::string& what) {
    if (ok) {
        ++g_pass;
        std::printf("   ok    %s\n", what.c_str());
    } else {
        ++g_fail;
        std::printf("   FAIL  %s\n", what.c_str());
    }
}

template <typename T, typename U>
void eq(const T& got, const U& want, const std::string& what) {
    const bool ok = (got == static_cast<T>(want));
    if (ok) {
        ++g_pass;
        std::printf("   ok    %s = %lld\n", what.c_str(), static_cast<long long>(got));
    } else {
        ++g_fail;
        std::printf("   FAIL  %s = %lld, expected %lld\n", what.c_str(), static_cast<long long>(got),
                    static_cast<long long>(want));
    }
}

std::vector<uint8_t> payload(uint8_t marker, size_t bytes = 8) {
    return std::vector<uint8_t>(bytes, marker);
}

// enq - deq - depth == drop. True of the queue at any instant, and checked here
// only at rest: an observer reads these as five independent relaxed
// loads, so a live row can be off by one without anything being wrong.
void checkIdentity(const ConnectionQueue& q) {
    const uint64_t enq = q.enq_total.load();
    const uint64_t deq = q.deq_total.load();
    const uint64_t depth = q.depth.load();
    const uint64_t drop = q.drop_total.load();
    check(enq - deq - depth == drop, "accounting identity enq - deq - depth == drop at rest");
}

// --------------------------------------------------------------------------
// HISTORY: the depth bound and its drop rule
// --------------------------------------------------------------------------

void testUndeclaredIsUnbounded() {
    begin("an unconfigured queue is the submitted queue");
    ConnectionQueue q;
    eq(q.max_depth(), 0u, "max_depth (0 = no cap)");
    eq(q.policy_code(), Q_UNBOUNDED, "policy code");
    check(!q.bounded(), "bounded() is false");
    check(!q.configured(), "configured() is false: nothing was declared");

    for (int i = 0; i < 1000; ++i) q.push(payload(static_cast<uint8_t>(i)));
    eq(q.depth.load(), 1000u, "depth after 1000 pushes with no consumer");
    eq(q.drop_total.load(), 0u, "drops");
    checkIdentity(q);
}

void testKeepLastBounds() {
    begin("KEEP_LAST(4) holds at four and counts what it discarded");
    ConnectionQueue q;
    q.configure(4, Q_KEEP_LAST);

    for (int i = 0; i < 100; ++i) q.push(payload(static_cast<uint8_t>(i)));
    eq(q.depth.load(), 4u, "depth");
    eq(q.peak.load(), 4u, "peak never exceeds the cap");
    eq(q.drop_total.load(), 96u, "drops");
    eq(q.enq_total.load(), 100u, "enqueued");
    checkIdentity(q);
}

void testKeepLastKeepsNewest() {
    begin("KEEP_LAST(4) keeps the newest four, in order");
    ConnectionQueue q;
    q.configure(4, Q_KEEP_LAST);
    for (int i = 0; i < 100; ++i) q.push(payload(static_cast<uint8_t>(i)));

    // DDS KEEP_LAST(n) retains the n most recent samples, so the sample that
    // goes is the oldest. Getting this backwards would turn latest-data-first
    // into oldest-data-first and still pass every count-based assertion.
    for (int expect = 96; expect < 100; ++expect) {
        DataType d = q.pop();
        eq(static_cast<int>(d.data[0]), expect, "front payload marker");
    }
    eq(q.depth.load(), 0u, "drained");
    checkIdentity(q);
}

void testKeepLastOne() {
    begin("KEEP_LAST(1) is always the latest value");
    ConnectionQueue q;
    q.configure(1, Q_KEEP_LAST);
    for (int i = 0; i < 50; ++i) q.push(payload(static_cast<uint8_t>(i)));
    eq(q.depth.load(), 1u, "depth");
    eq(q.drop_total.load(), 49u, "drops");
    DataType d = q.pop();
    eq(static_cast<int>(d.data[0]), 49, "the value held is the most recent");
}

void testKeepAllIsUnboundedButLabelled() {
    begin("declared KEEP_ALL behaves as the unconfigured path and says so");
    ConnectionQueue q;
    q.configure(0, Q_KEEP_ALL);
    for (int i = 0; i < 500; ++i) q.push(payload(static_cast<uint8_t>(i)));
    eq(q.depth.load(), 500u, "depth (nothing discarded)");
    eq(q.drop_total.load(), 0u, "drops");
    eq(q.max_depth(), 0u, "max_depth reports no cap");
    eq(q.policy_code(), Q_KEEP_ALL, "policy code distinguishes it from UNBOUNDED");
    check(!q.bounded(), "bounded() is false: KEEP_ALL is satisfied by doing nothing");
}

void testBytesAccounting() {
    begin("retention is tracked in bytes as well as entries");
    ConnectionQueue q;
    q.configure(3, Q_KEEP_LAST);

    q.push(payload(1, 100));
    q.push(payload(2, 200));
    q.push(payload(3, 300));
    eq(q.bytes_cur.load(), 600u, "bytes held");
    eq(q.bytes_peak.load(), 600u, "bytes high-water");

    q.push(payload(4, 400));  // evicts the 100-byte entry
    eq(q.depth.load(), 3u, "depth still at the cap");
    eq(q.bytes_cur.load(), 900u, "bytes held after drop-oldest (200+300+400)");
    eq(q.bytes_peak.load(), 900u, "bytes high-water rose");

    q.pop();  // removes 200
    eq(q.bytes_cur.load(), 700u, "bytes held after a pop");
    eq(q.bytes_peak.load(), 900u, "high-water does not fall");
}

void testWakeupBypassesTheCap() {
    begin("the shutdown sentinel is not a policy drop");
    ConnectionQueue q;
    q.configure(2, Q_KEEP_LAST);
    q.push(payload(1));
    q.push(payload(2));
    eq(q.depth.load(), 2u, "queue is at its cap");
    const uint64_t before = q.drop_total.load();

    q.push_wakeup();
    eq(q.drop_total.load(), before, "drop count unchanged by teardown");
    eq(q.peak.load(), 2u, "peak unchanged by teardown");
    eq(q.depth.load(), 3u, "the sentinel was admitted above the cap");

    // ...and it is still reachable, which is what the worker's exit depends on.
    q.pop();
    q.pop();
    DataType sentinel = q.pop();
    check(sentinel.data.empty(), "the sentinel survived to the front");
    checkIdentity(q);
}

void testConcurrentBound() {
    begin("the bound holds under a concurrent producer and consumer");
    ConnectionQueue q;
    q.configure(8, Q_KEEP_LAST);
    std::atomic<bool> stop{false};
    std::atomic<uint32_t> observed_max{0};

    std::thread consumer([&] {
        while (!stop.load()) {
            DataType d = q.pop();
            if (d.data.empty()) break;
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        }
    });

    for (int i = 0; i < 20000; ++i) {
        q.push(payload(static_cast<uint8_t>(i)));
        const uint32_t d = q.depth.load();
        if (d > observed_max.load()) observed_max.store(d);
    }
    stop.store(true);
    q.push_wakeup();
    consumer.join();

    check(observed_max.load() <= 8, "observed depth never exceeded the cap (saw " +
                                        std::to_string(observed_max.load()) + ")");
    check(q.drop_total.load() > 0, "the producer outran the consumer, so drops were counted (" +
                                       std::to_string(q.drop_total.load()) + ")");
    checkIdentity(q);
}

// --------------------------------------------------------------------------
// queueQosFor: only HISTORY reaches the queue
// --------------------------------------------------------------------------

void testQueueMirrorsHistoryOnly() {
    begin("the queue is configured from HISTORY and from nothing else");
    QosProfile p;
    p.reliability = ReliabilityKind::RELIABLE;
    p.deadline_ms = 100;
    p.liveliness = LivelinessSpec{};
    p.durability = DurabilityKind::TRANSIENT_LOCAL;

    QueueQos q = queueQosFor(p);
    eq(q.max_depth, 0u, "four policies declared, none of them history");
    eq(q.policy, Q_UNBOUNDED, "policy");
    check(!q.configured(), "the queue stays as the submitted gateway had it");

    p.history = HistorySpec{HistoryKind::KEEP_LAST, 4};
    q = queueQosFor(p);
    eq(q.max_depth, 4u, "KEEP_LAST(4) bounds it");
    eq(q.policy, Q_KEEP_LAST, "policy");

    p.history = HistorySpec{HistoryKind::KEEP_ALL, 1};
    q = queueQosFor(p);
    eq(q.max_depth, 0u, "KEEP_ALL leaves it unbounded");
    eq(q.policy, Q_KEEP_ALL, "but labelled, so the observer can tell");
    check(q.configured(), "and reported as configured");
}

void testConfiguredMapPublishesConfigured() {
    begin("a queue is configured before it is reachable through the map");
    ConnectionQueueMap m;
    auto q = m.create("k", 5, Q_KEEP_LAST);
    eq(q->max_depth(), 5u, "max_depth on the returned queue");
    auto again = m.get("k");
    eq(again->max_depth(), 5u, "and on the one a second reader obtains");

    // An existing key is returned unchanged: the fields are read without a
    // lock, which is sound only while they are written once, before publication.
    auto third = m.create("k", 99, Q_KEEP_LAST);
    eq(third->max_depth(), 5u, "an already-published queue is not reconfigured");
}

// --------------------------------------------------------------------------
// The spec sheet: the sentences the letter makes about it, asserted
// --------------------------------------------------------------------------

struct Direction {
    ProtocolType source;
    ProtocolType sink;
};

const Direction kDirections[] = {
    {ProtocolType::DDS, ProtocolType::ZENOH},    {ProtocolType::DDS, ProtocolType::SOMEIP},
    {ProtocolType::ZENOH, ProtocolType::DDS},    {ProtocolType::ZENOH, ProtocolType::SOMEIP},
    {ProtocolType::SOMEIP, ProtocolType::DDS},   {ProtocolType::SOMEIP, ProtocolType::ZENOH},
};

const QosPolicyId kPolicies[] = {QosPolicyId::RELIABILITY, QosPolicyId::DEADLINE, QosPolicyId::HISTORY,
                                 QosPolicyId::LIVELINESS, QosPolicyId::DURABILITY};

std::string dirName(const Direction& d) { return directionLabel(d.source, d.sink); }

void testSpecDeadlineIsADdsSinkPolicy() {
    begin("deadline: built on a DDS sink and nowhere else; never on a source");
    QosProfile p;
    p.deadline_ms = 100;
    for (const Direction& d : kDirections) {
        const QosSpecCell c = qosSpec(d.source, d.sink, QosPolicyId::DEADLINE, p);
        const bool want = d.sink == ProtocolType::DDS;
        check(c.carried == want, dirName(d) + (want ? ": carried (the sink writer offers it)"
                                                    : ": not carried (no channel notifies the consumer)"));
        check(std::string(c.source).empty(), dirName(d) + ": nothing is built on the source leg");
        if (!want) check(!std::string(c.note).empty(), dirName(d) + ": the missing mechanism is named");
    }
}

void testSpecHistoryIsSettledByTheSink() {
    begin("history: the sink decides; a SOME/IP sink never offers a depth");
    QosProfile one, sixteen, all;
    one.history = HistorySpec{HistoryKind::KEEP_LAST, 1};
    sixteen.history = HistorySpec{HistoryKind::KEEP_LAST, 16};
    all.history = HistorySpec{HistoryKind::KEEP_ALL, 0};
    for (const Direction& d : kDirections) {
        const QosSpecCell c1 = qosSpec(d.source, d.sink, QosPolicyId::HISTORY, one);
        const QosSpecCell c16 = qosSpec(d.source, d.sink, QosPolicyId::HISTORY, sixteen);
        const QosSpecCell ca = qosSpec(d.source, d.sink, QosPolicyId::HISTORY, all);
        switch (d.sink) {
            case ProtocolType::DDS:
                check(c1.carried && c16.carried && ca.carried, dirName(d) + ": KEEP_LAST(n) and KEEP_ALL");
                break;
            case ProtocolType::ZENOH:
                check(c1.carried && c16.carried, dirName(d) + ": KEEP_LAST(n) via the AdvancedPublisher cache");
                check(!ca.carried, dirName(d) + ": KEEP_ALL builds no cache (fixed max_samples only)");
                check(std::string(c16.note).find("KEEP_ALL") != std::string::npos,
                      dirName(d) + ": the note says KEEP_ALL is the exception");
                break;
            case ProtocolType::SOMEIP:
                // The old depth-one exception is gone on purpose: a field is
                // durability, not history, and calling it history on one role
                // counted the same slot twice.
                check(!c1.carried && !c16.carried && !ca.carried,
                      dirName(d) + ": not carried at any depth, depth one included");
                break;
            default:
                break;
        }
        check(c1.carried == c16.carried, dirName(d) + ": depth 1 and depth 16 are the same cell");
    }
}

void testSpecDurabilitySomeipSinkIsDepthOne() {
    begin("durability: carried in all six; a SOME/IP sink keeps one value (ET_FIELD) and says so");
    QosProfile vol, tl;
    vol.durability = DurabilityKind::VOLATILE;
    tl.durability = DurabilityKind::TRANSIENT_LOCAL;
    for (const Direction& d : kDirections) {
        const QosSpecCell cv = qosSpec(d.source, d.sink, QosPolicyId::DURABILITY, vol);
        const QosSpecCell ct = qosSpec(d.source, d.sink, QosPolicyId::DURABILITY, tl);
        check(cv.carried, dirName(d) + ": VOLATILE is carried by building nothing for it");
        check(ct.carried, dirName(d) + ": TRANSIENT_LOCAL is carried");
        check(!std::string(ct.sink).empty(), dirName(d) + ": the sink names what it builds");
        if (d.sink == ProtocolType::SOMEIP) {
            check(std::string(ct.note).find("depth one") != std::string::npos, dirName(d) + ": the note says depth one");
            check(std::string(ct.sink).find("ET_FIELD") != std::string::npos, dirName(d) + ": built as ET_FIELD");
        } else {
            check(std::string(ct.note).empty(), dirName(d) + ": nothing to note -- the declared depth holds");
        }
    }
}

void testSpecLivelinessIsOneShapeOnEveryLeg() {
    begin("liveliness: carried end to end in all six directions; the sink exists while the source's assertion does");
    QosProfile p;
    p.liveliness = LivelinessSpec{};
    for (const Direction& d : kDirections) {
        const QosSpecCell c = qosSpec(d.source, d.sink, QosPolicyId::LIVELINESS, p);
        check(c.carried, dirName(d) + ": carried");
        check(std::string(c.source).find("assertion") != std::string::npos,
              dirName(d) + ": the source string names the source's assertion");
        check(std::string(c.sink).find("with the bridge") != std::string::npos,
              dirName(d) + ": the sink string ties the endpoint to the bridge");
        check(std::string(c.note).find("exists exactly while the source's assertion does") != std::string::npos,
              dirName(d) + ": the note states the end-to-end contract");
        check(std::string(c.note).find("no time bound") != std::string::npos,
              dirName(d) + ": the note says there is no time bound of the policy's own");
    }
}

void testSpecReliabilityIsEachLegSeparately() {
    begin("reliability: carried in all six at both values; best effort is the plain endpoint");
    QosProfile rel, be;
    rel.reliability = ReliabilityKind::RELIABLE;
    be.reliability = ReliabilityKind::BEST_EFFORT;
    for (const Direction& d : kDirections) {
        const QosSpecCell cr = qosSpec(d.source, d.sink, QosPolicyId::RELIABILITY, rel);
        const QosSpecCell cb = qosSpec(d.source, d.sink, QosPolicyId::RELIABILITY, be);
        check(cr.carried && cb.carried, dirName(d) + ": both values carried");
        check(!std::string(cr.source).empty() && !std::string(cr.sink).empty(),
              dirName(d) + ": both legs name their mechanism");
        if (d.sink == ProtocolType::ZENOH) {
            check(std::string(cb.sink).find("plain") != std::string::npos,
                  dirName(d) + ": BEST_EFFORT keeps the plain publisher");
            check(std::string(cr.sink).find("cache") != std::string::npos,
                  dirName(d) + ": RELIABLE names the cache, not z_publisher_options_t::reliability");
        }
    }
}

void testSpecRowsFollowTheDeclaration() {
    begin("a mapping selects one row per declared policy, in policy order, and none when nothing is declared");
    Command c;
    c.source = ProtocolType::DDS;
    c.sink = ProtocolType::SOMEIP;
    c.message_type = MessageType::STD_MSGS_MSG_STRING;
    c.topic = "rt/t";
    c.service_id = 0x1234;
    c.instance_id = 1;
    c.eventgroup_id = 1;
    c.event_id = 0x8001;
    c.qos_policies.deadline_ms = 100;
    c.qos_policies.durability = DurabilityKind::TRANSIENT_LOCAL;

    const auto spec = qosSpecFor(c);
    eq(spec.size(), 2u, "two policies declared, two rows");
    check(spec[0].policy == QosPolicyId::DEADLINE && !spec[0].cell.carried, "d2s DEADLINE: not carried");
    check(spec[1].policy == QosPolicyId::DURABILITY && spec[1].cell.carried &&
              std::string(spec[1].cell.note).find("depth one") != std::string::npos,
          "d2s DURABILITY: carried, noted as depth one");
    eq(countNotCarried(spec), 1u, "one of the two is not carried in this direction");
    check(spec[0].value == "100ms" && spec[1].value == "TRANSIENT_LOCAL", "the declared values are printed");

    Command bare = c;
    bare.qos_policies = QosProfile{};
    check(qosSpecFor(bare).empty(), "an undeclared mapping selects no rows at all (the submitted path)");
}

// The mapping parser refuses what the table cannot express, instead of
// reading it and building something else. This is the assertion behind
// "liveliness has no lease": not that a lease is ignored, but that it is not
// accepted.
void testParserDoesNotReadALivelinessLease() {
    begin("the mapping parser: liveliness is a boolean; a kind or a lease is refused, not read");
    const auto dir = std::filesystem::temp_directory_path() / "queue_policy_test";
    std::filesystem::create_directories(dir);

    auto write = [&](const char* name, const std::string& liveliness) {
        const auto path = dir / name;
        std::ofstream f(path);
        f << "{ \"mappings\": [ { \"source\": \"DDS\", \"sink\": \"ZENOH\", \"message_type\": "
             "\"STD_MSGS_MSG_STRING\", \"qos\": \"RELIABLE\", \"topic\": \"rt/t\", \"zenoh_key\": \"t\", "
             "\"qos_policies\": { \"liveliness\": "
          << liveliness << " } } ] }";
        return path.string();
    };
    auto loads = [](const std::string& path, bool* declared) {
        try {
            const auto m = loadDiscoveryMappingsFromJson(path);
            *declared = m.size() == 1 && m[0].cmd_template.qos_policies.liveliness.has_value();
            return true;
        } catch (const std::exception&) {
            return false;
        }
    };

    bool declared = false;
    check(loads(write("true.json", "true"), &declared) && declared, "\"liveliness\": true declares it");
    check(loads(write("false.json", "false"), &declared) && !declared, "\"liveliness\": false leaves it undeclared");
    check(!loads(write("lease.json", "{ \"kind\": \"AUTOMATIC\", \"lease_ms\": 2000 }"), &declared),
          "a lease is refused at load time");
    check(!loads(write("kind.json", "\"MANUAL_BY_TOPIC\""), &declared), "a kind is refused at load time");
    check(!loads(write("zero.json", "{ \"kind\": \"AUTOMATIC\", \"lease_ms\": 0 }"), &declared),
          "even lease 0 is refused: the parameter does not exist, whatever its value");

    std::filesystem::remove_all(dir);
}

// ---------------------------------------------------------------------------
// The spec sheet, printed: 5 policies x 6 directions.
//
// Derived, not written down. Every cell comes from qosSpec(), the function
// the gateway runs at startup, so the table in the response letter is produced
// by the same code and stays true when the code changes. A cell can depend on
// the declared VALUE (Zenoh carries KEEP_LAST(n) and not KEEP_ALL), so each
// policy is evaluated over the values known to branch and the cell reports the
// form that is carried.
// ---------------------------------------------------------------------------

struct Variant {
    std::string label;
    QosProfile p;
};

std::vector<Variant> variantsOf(QosPolicyId policy) {
    std::vector<Variant> v;
    auto add = [&v](const std::string& label, QosProfile p) { v.push_back({label, std::move(p)}); };
    QosProfile p;
    switch (policy) {
        case QosPolicyId::RELIABILITY:
            p.reliability = ReliabilityKind::RELIABLE;    add("RELIABLE", p);
            p.reliability = ReliabilityKind::BEST_EFFORT; add("BEST_EFFORT", p);
            break;
        case QosPolicyId::DEADLINE:
            p.deadline_ms = 100; add("any period", p);
            break;
        case QosPolicyId::HISTORY:
            p.history = HistorySpec{HistoryKind::KEEP_LAST, 1};  add("KEEP_LAST(1)", p);
            p.history = HistorySpec{HistoryKind::KEEP_LAST, 16}; add("KEEP_LAST(16)", p);
            p.history = HistorySpec{HistoryKind::KEEP_ALL, 0};   add("KEEP_ALL", p);
            break;
        case QosPolicyId::LIVELINESS:
            p.liveliness = LivelinessSpec{}; add("declared", p);
            break;
        case QosPolicyId::DURABILITY:
            p.durability = DurabilityKind::VOLATILE;        add("VOLATILE", p);
            p.durability = DurabilityKind::TRANSIENT_LOCAL; add("TRANSIENT_LOCAL", p);
            break;
    }
    return v;
}

void addUnique(std::vector<std::string>& v, const std::string& s) {
    for (const std::string& e : v) {
        if (e == s) return;
    }
    v.push_back(s);
}

std::string join(const std::vector<std::string>& v, const char* sep) {
    std::string s;
    for (size_t i = 0; i < v.size(); ++i) {
        if (i) s += sep;
        s += v[i];
    }
    return s;
}

// One cell of the 5 x 6 table: "ok" or "-". A cell is ok when the policy is
// built in this direction; a value the direction does not build (KEEP_ALL on
// a Zenoh sink) and a depth it fixes (a SOME/IP field is one value) are notes
// under the table, not cells -- depth is history's business, and a cache that
// keeps n IS history.
struct CellSummary {
    std::string text;
    int carried = 0;   // variants carried
    int total = 0;
};

CellSummary summarize(const Direction& d, QosPolicyId policy) {
    CellSummary s;
    for (const Variant& v : variantsOf(policy)) {
        const QosSpecCell c = qosSpec(d.source, d.sink, policy, v.p);
        ++s.total;
        if (c.carried) ++s.carried;
    }
    s.text = s.carried == 0 ? "-" : "ok";
    return s;
}

void printHeader() {
    std::printf("\n-- QoS spec sheet: 5 policies x 6 conversion directions\n");
    std::printf("   What the gateway builds for a declared policy, per direction. The three\n");
    std::printf("   protocols do not express QoS at the same layer, so the gateway does not say\n");
    std::printf("   \"supported\" or \"not supported\": each cell fixes the form built for that\n");
    std::printf("   combination, and the external node is configured to match it. A cell is\n");
    std::printf("   marked carried only where both middlewares can verify the guarantee from\n");
    std::printf("   outside the gateway -- an imitation inside the gateway is not a contract\n");
    std::printf("   the far endpoint can request or check.\n\n");
}

void printMatrix(bool long_form) {
    printHeader();

    std::printf("   %-13s", "policy");
    for (const Direction& d : kDirections) std::printf(" %-13s", dirName(d).c_str());
    std::printf("\n");

    int n_ok = 0, n_none = 0;
    for (QosPolicyId policy : kPolicies) {
        std::printf("   %-13s", to_string(policy));
        for (const Direction& d : kDirections) {
            const CellSummary s = summarize(d, policy);
            std::printf(" %-13s", s.text.c_str());
            if (s.text == "-") ++n_none;
            else ++n_ok;
        }
        std::printf("\n");
    }
    std::printf("\n   -> %d cells built, %d build nothing (of 30)\n", n_ok, n_none);
    std::printf("\n   ok            built in this direction; checkable from both ends\n");
    std::printf("   -             nothing is built for it in this direction; the reason is below\n");
    std::printf("   d2z = DDS source -> Zenoh sink, and so on. source = the leg the gateway reads\n");
    std::printf("   from, sink = the leg it writes to. The bridge comes up in every case.\n");
    std::printf("   Depth is history's business: a SOME/IP field keeps one value, a Zenoh cache\n");
    std::printf("   keeps KEEP_LAST(n) and has no KEEP_ALL. Both are notes below, not cells.\n");

    std::printf("\n-- Why a cell is '-', and what to know about a cell that is ok\n");
    for (QosPolicyId policy : kPolicies) {
        for (const Direction& d : kDirections) {
            for (const Variant& v : variantsOf(policy)) {
                const QosSpecCell c = qosSpec(d.source, d.sink, policy, v.p);
                if (!*c.note) continue;
                std::printf("   %-11s %-4s %-15s %s\n", to_string(policy), dirName(d).c_str(), v.label.c_str(),
                            c.note);
            }
        }
    }

    if (!long_form) return;

    std::printf("\n-- What each leg builds, per cell\n");
    for (QosPolicyId policy : kPolicies) {
        for (const Direction& d : kDirections) {
            for (const Variant& v : variantsOf(policy)) {
                const QosSpecCell c = qosSpec(d.source, d.sink, policy, v.p);
                std::printf("   %-11s %-4s %-15s %s\n", to_string(policy), dirName(d).c_str(), v.label.c_str(),
                            c.carried ? "carried" : "not carried");
                if (*c.source) std::printf("      source: %s\n", c.source);
                if (*c.sink) std::printf("      sink:   %s\n", c.sink);
            }
        }
    }
}

std::string csvQuote(const char* s) {
    std::string out = "\"";
    for (const char* p = s; *p; ++p) {
        if (*p == '"') out += '"';
        out += *p;
    }
    return out + "\"";
}

void printCsv() {
    std::printf("policy,direction,value,carried,source,sink,note\n");
    for (QosPolicyId policy : kPolicies) {
        for (const Direction& d : kDirections) {
            for (const Variant& v : variantsOf(policy)) {
                const QosSpecCell c = qosSpec(d.source, d.sink, policy, v.p);
                std::printf("%s,%s,%s,%s,%s,%s,%s\n", to_string(policy), dirName(d).c_str(),
                            csvQuote(v.label.c_str()).c_str(), c.carried ? "yes" : "no",
                            csvQuote(c.source).c_str(), csvQuote(c.sink).c_str(), csvQuote(c.note).c_str());
            }
        }
    }
}

}  // namespace

int main(int argc, char* argv[]) {
    const bool matrix = argc > 1 && std::string(argv[1]) == "--matrix";
    const std::string mode = matrix && argc > 2 ? argv[2] : "";

    if (matrix && mode == "csv") {
        printCsv();
        return 0;
    }

    std::printf("queue_policy_test -- bounded bridge queue + QoS spec sheet\n");

    testUndeclaredIsUnbounded();
    testKeepLastBounds();
    testKeepLastKeepsNewest();
    testKeepLastOne();
    testKeepAllIsUnboundedButLabelled();
    testBytesAccounting();
    testWakeupBypassesTheCap();
    testConcurrentBound();
    testQueueMirrorsHistoryOnly();
    testConfiguredMapPublishesConfigured();
    testSpecDeadlineIsADdsSinkPolicy();
    testSpecHistoryIsSettledByTheSink();
    testSpecDurabilitySomeipSinkIsDepthOne();
    testSpecLivelinessIsOneShapeOnEveryLeg();
    testSpecReliabilityIsEachLegSeparately();
    testSpecRowsFollowTheDeclaration();
    testParserDoesNotReadALivelinessLease();

    if (matrix) printMatrix(mode == "long");

    std::printf("\n%d passed, %d failed\n", g_pass, g_fail);
    return g_fail == 0 ? 0 : 1;
}
