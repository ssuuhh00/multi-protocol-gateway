#include "common/connection_queue.hpp"
#include "common/qos_spec.hpp"


namespace {

// ---------------------------------------------------------------------------
// What each leg is built with, per protocol. These are the facts the cells are
// assembled from; the cell functions below decide which leg settles the policy.
//
// Fast DDS 2.14.5 -- every policy is a QoS object on the reader/writer.
// Zenoh 1.8.0 with Z_FEATURE_UNSTABLE_API -- four of the five come from the
//   Advanced Pub/Sub extension; liveliness is a stable-API token.
// vSomeIP 3.5.8 -- two optional arguments on offer_event()/request_event()
//   (event type, reliability) and the service-discovery machinery.
// ---------------------------------------------------------------------------

bool reliable(const QosProfile& p) {
    return p.reliability && *p.reliability == ReliabilityKind::RELIABLE;
}
bool keepLast(const QosProfile& p) {
    return p.history && p.history->kind == HistoryKind::KEEP_LAST;
}
bool transientLocal(const QosProfile& p) {
    return p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL;
}

// --- RELIABILITY: each leg separately ---------------------------------------

const char* reliabilitySource(ProtocolType proto, const QosProfile& p) {
    switch (proto) {
        case ProtocolType::DDS:
            return "DataReader ReliabilityQosPolicy at the declared kind";
        case ProtocolType::ZENOH:
            // z_publisher_options_t::reliability is deliberately not the
            // mechanism: the upstream header says it "does not trigger any
            // data retransmission on the wire". Retransmission is the
            // Advanced extension's cache + recovery, and only that.
            return reliable(p) ? "AdvancedSubscriber recovery, heartbeat-triggered: a gap in the publisher's "
                                 "sequence numbers is re-queried from its cache"
                               : "plain subscriber: no recovery, which is what best effort is";
        case ProtocolType::SOMEIP:
            return "request_event reliability_type_e: RT_RELIABLE selects the service's TCP endpoint, "
                   "RT_UNRELIABLE its UDP endpoint";
        default:
            return "";
    }
}

const char* reliabilitySink(ProtocolType proto, const QosProfile& p) {
    switch (proto) {
        case ProtocolType::DDS:
            return "DataWriter ReliabilityQosPolicy at the declared kind";
        case ProtocolType::ZENOH:
            return reliable(p) ? "AdvancedPublisher cache + sample-miss detection with a periodic heartbeat, so a "
                                 "gap at the end of a burst is visible too"
                               : "plain publisher: no cache and no retransmission";
        case ProtocolType::SOMEIP:
            // vsomeip prefers a reliability configured for the event in
            // vsomeip.json over the API argument (routing_manager_base::
            // determine_event_reliability); the gateway's configs set none, so
            // the argument is what decides.
            return reliable(p) ? "offer_event reliability_type_e RT_RELIABLE: the service's TCP endpoint, which "
                                 "requires a reliable port declared for the service in vsomeip.json"
                               : "offer_event reliability_type_e RT_UNRELIABLE: the service's UDP endpoint";
        default:
            return "";
    }
}

// --- DEADLINE: the sink only -------------------------------------------------
//
// A deadline is the receiving end's contract with its own middleware: "tell me
// when the interval between samples exceeds N". Nothing about it crosses the
// bridge. So the only thing the gateway can build is the OFFER on its own DDS
// writer, without which an external reader that requests the period never
// matches (RxO: offered <= requested) -- and once offered, DDS measures it at
// both ends by itself.
//
// The gateway's DDS source reader does not request a deadline. It could, and
// it would then be told of a stalled external writer in its own log -- but
// that is visible nowhere outside the gateway, and it would make the external
// writer's offer a matching condition for nothing in return.
//
// On a Zenoh or SOME/IP sink the problem is not detection but notification:
// the gateway could time the gap itself, and there is no mechanism in either
// middleware by which "the interval was exceeded" reaches the consumer as a
// contract event.

QosSpecCell deadlineCell(ProtocolType sink) {
    QosSpecCell c;
    switch (sink) {
        case ProtocolType::DDS:
            c.carried = true;
            c.sink = "DataWriter DeadlineQosPolicy offers the declared period; a reader that requests it is told "
                     "of a missed period by its own middleware (on_requested_deadline_missed), and this writer by "
                     "on_offered_deadline_missed";
            return c;
        case ProtocolType::ZENOH:
            c.brief = "Zenoh has no mechanism that reports a missed interval to a subscriber";
            c.note = "no Zenoh mechanism tells a subscriber that an expected interval between samples was exceeded: "
                     "the gateway could time the gap, but nothing it measures reaches the consumer as a contract "
                     "event (AdvancedSubscriber sample-miss detection reports sequence-number gaps and fires on a "
                     "later arrival, not on silence). Nothing is built";
            return c;
        case ProtocolType::SOMEIP:
            c.brief = "SOME/IP has no mechanism that reports a missed interval to a subscriber";
            c.note = "no SOME/IP mechanism tells a subscriber that an expected interval was exceeded: offer_event's "
                     "_cycle re-sends the current value on a period and never checks one, and request_event and "
                     "subscribe take no period at all. Nothing is built";
            return c;
        default:
            c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
            return c;
    }
}

// --- HISTORY: the sink -------------------------------------------------------
//
// Retention that a late or slow consumer can observe is the sink endpoint's.
// The source leg's receive-side depth (a DDS reader's history) sits inside the
// gateway and is not part of the contract; the bridge queue mirrors the
// declared depth on the gateway's own path whatever the sink offers.

QosSpecCell historyCell(ProtocolType source, ProtocolType sink, const QosProfile& p) {
    QosSpecCell c;
    if (source == ProtocolType::DDS) {
        c.source = "DataReader HistoryQosPolicy at the declared depth, on the gateway's side of the bridge "
                   "(not observable from outside)";
    }
    switch (sink) {
        case ProtocolType::DDS:
            c.carried = true;
            c.sink = keepLast(p) ? "DataWriter HistoryQosPolicy KEEP_LAST at the declared depth, ResourceLimits "
                                   "widened so the declared number is the number that holds"
                                 : "DataWriter HistoryQosPolicy KEEP_ALL";
            return c;
        case ProtocolType::ZENOH:
            if (keepLast(p)) {
                c.carried = true;
                c.sink = "AdvancedPublisher cache.max_samples at the declared depth; the cache serves recovery and a "
                         "late joiner's history query (samples carry a sequence number the cache is indexed by; a "
                         "heartbeat only when RELIABLE is also declared)";
                c.note = "KEEP_LAST(n) only: the AdvancedPublisher cache is a fixed max_samples with no unbounded "
                         "setting, so KEEP_ALL builds no cache";
                return c;
            }
            c.brief = "a Zenoh cache has no unbounded setting, so KEEP_ALL builds none; the bridge queue stays unbounded";
            c.note = "the AdvancedPublisher cache is a fixed max_samples with no unbounded setting. Nothing is built "
                     "for KEEP_ALL; the bridge queue stays unbounded, which is what KEEP_ALL asks of the gateway's "
                     "own path";
            return c;
        case ProtocolType::SOMEIP:
            c.brief = "a vSomeIP event holds one value, not a depth; the declared depth bounds the gateway queue only";
            c.note = "a vSomeIP event holds one current payload and one staging slot (event.hpp: current_/update_; "
                     "the staging slot assembles the next value, it is not history), so there is no depth to offer at "
                     "any layer and no configuration key for one. Nothing is built; the declared depth bounds the "
                     "gateway's own queue only";
            return c;
        default:
            c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
            return c;
    }
}

// --- DURABILITY: the sink ----------------------------------------------------
//
// VOLATILE builds nothing and is carried by that: a late joiner is served
// nothing, which is what VOLATILE means. TRANSIENT_LOCAL is where the three
// differ, and SOME/IP's form is fixed at depth one.

QosSpecCell durabilityCell(ProtocolType source, ProtocolType sink, const QosProfile& p) {
    QosSpecCell c;
    c.carried = true;
    if (!transientLocal(p)) {
        // VOLATILE builds nothing of its own. On a Zenoh sink that is not the
        // same as "nothing is retained": a cache built for HISTORY KEEP_LAST(n)
        // or RELIABLE still exists and answers any subscriber that queries
        // history. What VOLATILE fixes is that the gateway's own endpoints do
        // not ask.
        c.sink = sink == ProtocolType::ZENOH
                     ? "nothing is built for durability itself; a cache built for HISTORY KEEP_LAST(n) or RELIABLE "
                       "still answers a subscriber that queries history, so VOLATILE here means the subscriber "
                       "built from this mapping does not ask"
                     : "nothing is retained for a late joiner, which is what VOLATILE means";
        return c;
    }
    switch (source) {
        case ProtocolType::DDS:
            // History is handed to a newly matched reader by the stateful
            // (RELIABLE) writer only (StatefulWriter::matched_reader_add); a
            // best-effort pair matches but the late joiner receives nothing.
            c.source = "DataReader DurabilityQosPolicy TRANSIENT_LOCAL: a gateway that starts after the external "
                       "writer receives that writer's retained samples (on a RELIABLE leg -- Fast DDS hands history "
                       "to reliable readers only)";
            break;
        case ProtocolType::ZENOH:
            c.source = "AdvancedSubscriber history query with detect_late_publishers: a gateway that starts after "
                       "the external publisher fetches its cache";
            break;
        case ProtocolType::SOMEIP:
            // The initial value is sent by the provider because the PROVIDER
            // offered a field (eventgroupinfo::send_initial_events keys on the
            // offered type); the client's type has to match it because vsomeip
            // keeps one registration per event.
            c.source = "request_event ET_FIELD, matching the provider's offer: the provider sends its current value "
                       "on subscribe";
            break;
        default:
            break;
    }
    switch (sink) {
        case ProtocolType::DDS:
            c.sink = "DataWriter DurabilityQosPolicy TRANSIENT_LOCAL: a late-joining RELIABLE reader receives the "
                     "writer's history, i.e. the declared HISTORY depth (10 when none is declared); Fast DDS hands "
                     "history to reliable readers only";
            return c;
        case ProtocolType::ZENOH:
            c.sink = "AdvancedPublisher cache answers a late subscriber's history query; its depth is the declared "
                     "HISTORY depth (1 when no depth is declared, KEEP_ALL included); samples carry a sequence number "
                     "the cache is indexed by, with a heartbeat only when RELIABLE is also declared";
            return c;
        case ProtocolType::SOMEIP:
            c.sink = "offer_event ET_FIELD: the current value is retained and sent to every new subscriber "
                     "(eventgroupinfo::send_initial_events)";
            c.note = "depth one, fixed: a field is exactly one value. vsomeip keeps one event type per (service, "
                     "instance, event) and refuses a registration that disagrees, leaving the first one in "
                     "force, so the subscriber has to request it as a field too";
            return c;
        default:
            c.carried = false;
            c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
            return c;
    }
}

// --- LIVELINESS: both legs, joined by the bridge's lifecycle -------------------
//
// One shape on every leg: an assertion with no time bound of its own, which is
// what a Zenoh token and a SOME/IP service offer are and what a DDS writer is
// at AUTOMATIC with the default lease. A finite lease is a bound only DDS could
// enforce, so the mapping does not declare one (qos_profile.hpp).
//
// What carries the source's loss to the sink is not a leg mechanism but the
// bridge itself: DiscoveryManager creates the bridge when the source's
// assertion appears and deletes it when the assertion is withdrawn, and the
// sink endpoint -- writer, token, offer -- goes with the bridge. The far side
// therefore sees exactly the event its own middleware has for "the publisher
// is gone". For DDS and SOME/IP sources this is what the submitted gateway
// already did; a Zenoh source joins it through its token when the mapping
// declares liveliness (discovery_manager.hpp).
//
// The three middlewares report loss differently, and it is a consequence of
// their designs rather than a ranking: DDS has no broker, so each reader
// judges a writer by discovery (and by a lease only if it asked for one);
// Zenoh has a session layer that pushes a DELETE; SOME/IP has service
// discovery, a dedicated message. A crashed peer, as opposed to one that
// exited, is noticed within that middleware's own session lease -- the DDS
// participant lease, the Zenoh link lease, the SD TTL -- not within a lease of
// the policy's.

QosSpecCell livelinessCell(ProtocolType source, ProtocolType sink) {
    QosSpecCell c;
    c.carried = true;
    switch (source) {
        case ProtocolType::DDS:
            c.source = "participant discovery: a DataWriter on the topic is the source's assertion. The bridge is "
                       "created when one is discovered and deleted when it is removed (writer deleted, or its "
                       "participant's lease expired). The gateway's reader requests no lease, so on_liveliness_changed "
                       "is not the signal; removal is";
            break;
        case ProtocolType::ZENOH:
            c.source = "liveliness subscriber (history=true) on the mapping's key expression: the peer's token is the "
                       "source's assertion. The bridge is created when the token arrives and deleted when its DELETE "
                       "arrives (token undeclared, or the session dropped)";
            break;
        case ProtocolType::SOMEIP:
            c.source = "availability handler: the service's SD offer is the source's assertion. The bridge is created "
                       "when the service becomes available and deleted when it is withdrawn (stop_offer, or the "
                       "offer's TTL expired); scope is the service";
            break;
        default:
            c.carried = false;
            c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
            return c;
    }
    switch (sink) {
        case ProtocolType::DDS:
            c.sink = "DataWriter (AUTOMATIC, no lease) created with the bridge and deleted with it: a matched reader "
                     "sees on_subscription_matched -1 when the source is lost and +1 when it returns; a reader that "
                     "requested a finite lease also sees its alive count drop";
            break;
        case ProtocolType::ZENOH:
            c.sink = "liveliness token on the mapping's key expression, declared with the bridge and undeclared with "
                     "it: a watching peer sees a PUT while the source is alive and a DELETE when it is lost";
            break;
        case ProtocolType::SOMEIP:
            c.sink = "offer_event/offer_service made with the bridge and withdrawn with it: a subscriber sees "
                     "on_availability(false) when the source is lost and (true) when it returns; scope is the "
                     "service instance, so this holds when the mapping is the instance's only one";
            break;
        default:
            c.carried = false;
            c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
            return c;
    }
    c.note = "the sink endpoint exists exactly while the source's assertion does: the bridge is created when the "
             "source appears and deleted when its assertion is withdrawn, on every leg with no time bound of its own "
             "(a crashed peer is noticed within its middleware's own session lease). A retained history at the sink "
             "lives as long as the bridge, which is how DDS itself scopes TRANSIENT_LOCAL to the writer";
    return c;
}

QosSpecCell reliabilityCell(ProtocolType source, ProtocolType sink, const QosProfile& p) {
    QosSpecCell c;
    if (source == ProtocolType::NONE || sink == ProtocolType::NONE) {
        c.brief = "this mapping has no endpoint on that side";
            c.note = "this mapping has no endpoint on that side";
        return c;
    }
    c.carried = true;
    c.source = reliabilitySource(source, p);
    c.sink = reliabilitySink(sink, p);
    return c;
}

}  // namespace

const char* directionLabel(ProtocolType source, ProtocolType sink) {
    switch (source) {
        case ProtocolType::DDS:
            if (sink == ProtocolType::ZENOH) return "d2z";
            if (sink == ProtocolType::SOMEIP) return "d2s";
            break;
        case ProtocolType::ZENOH:
            if (sink == ProtocolType::DDS) return "z2d";
            if (sink == ProtocolType::SOMEIP) return "z2s";
            break;
        case ProtocolType::SOMEIP:
            if (sink == ProtocolType::DDS) return "s2d";
            if (sink == ProtocolType::ZENOH) return "s2z";
            break;
        default:
            break;
    }
    return "?";
}

QosSpecCell qosSpec(ProtocolType source, ProtocolType sink, QosPolicyId policy, const QosProfile& p) {
    switch (policy) {
        case QosPolicyId::RELIABILITY: return reliabilityCell(source, sink, p);
        case QosPolicyId::DEADLINE:    return deadlineCell(sink);
        case QosPolicyId::HISTORY:     return historyCell(source, sink, p);
        case QosPolicyId::DURABILITY:  return durabilityCell(source, sink, p);
        case QosPolicyId::LIVELINESS:  return livelinessCell(source, sink);
    }
    return QosSpecCell{};
}

std::string describeValue(QosPolicyId policy, const QosProfile& p) {
    switch (policy) {
        case QosPolicyId::RELIABILITY:
            return p.reliability ? to_string(*p.reliability) : "";
        case QosPolicyId::DEADLINE:
            return p.deadline_ms ? std::to_string(*p.deadline_ms) + "ms" : "";
        case QosPolicyId::HISTORY:
            if (!p.history) return "";
            return p.history->kind == HistoryKind::KEEP_ALL
                       ? std::string("KEEP_ALL")
                       : "KEEP_LAST(" + std::to_string(p.history->depth) + ")";
        case QosPolicyId::LIVELINESS:
            return p.liveliness ? "declared" : "";
        case QosPolicyId::DURABILITY:
            return p.durability ? to_string(*p.durability) : "";
    }
    return "";
}

std::vector<QosSpecEntry> qosSpecFor(const Command& cmd) {
    std::vector<QosSpecEntry> out;
    const QosProfile& p = cmd.qos_policies;
    if (p.empty()) {
        return out;  // submitted-gateway path; nothing declared, nothing to do
    }
    static const QosPolicyId kAll[] = {QosPolicyId::RELIABILITY, QosPolicyId::DEADLINE, QosPolicyId::HISTORY,
                                       QosPolicyId::LIVELINESS, QosPolicyId::DURABILITY};
    for (QosPolicyId policy : kAll) {
        if (!declares(p, policy)) {
            continue;
        }
        out.push_back({policy, describeValue(policy, p), qosSpec(cmd.source, cmd.sink, policy, p)});
    }
    return out;
}

size_t countNotCarried(const std::vector<QosSpecEntry>& spec) {
    size_t n = 0;
    for (const auto& e : spec) {
        if (!e.cell.carried) ++n;
    }
    return n;
}

QueueQos queueQosFor(const QosProfile& p) {
    QueueQos q;
    if (!p.history) {
        return q;  // nothing declared about retention: the submitted unbounded queue
    }
    if (p.history->kind == HistoryKind::KEEP_ALL) {
        q.policy = Q_KEEP_ALL;  // unbounded, and labelled so the observer can tell
        return q;
    }
    q.max_depth = static_cast<size_t>(p.history->depth);
    q.policy = Q_KEEP_LAST;
    return q;
}
