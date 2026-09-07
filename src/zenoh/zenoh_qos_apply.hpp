#ifndef ZENOH_ZENOH_QOS_APPLY_HPP
#define ZENOH_ZENOH_QOS_APPLY_HPP

#include <zenoh.hxx>

#include <string>

#include "common/qos_profile.hpp"

/**
 * @file zenoh_qos_apply.hpp
 * @brief Translate a declared QosProfile onto Zenoh's Advanced Pub/Sub extension.
 *
 * Zenoh has no QoS object. What it has, in the 1.8.0 build with
 * Z_FEATURE_UNSTABLE_API, is an Advanced Publisher and Advanced Subscriber
 * whose options cover four of the five policies, and a liveliness token in the
 * stable API:
 *
 *   RELIABILITY   publisher cache + sample_miss_detection, subscriber recovery.
 *                 This is real retransmission -- "Retransmission can only be
 *                 done if cache is enabled" (zenoh_commons.h) -- and not the
 *                 same thing as z_publisher_options_t::reliability, which is a
 *                 sample attribute that triggers no retransmission at all.
 *   HISTORY       publisher cache.max_samples. A sink policy: retention that
 *                 a late or slow consumer can observe is the publisher's.
 *   DURABILITY    publisher cache serving a subscriber's history query, with
 *                 detect_late_publishers for the late-joiner case.
 *                 Either cache, when RELIABLE is not also declared, carries
 *                 sequence numbers without a heartbeat -- zenoh-ext will not
 *                 declare a cache sequenced by timestamp unless the session
 *                 config enables timestamping (buildPublisherOptions).
 *   LIVELINESS    a liveliness token on the sink (zenoh_publisher.cpp), a
 *                 liveliness subscriber on the source (discovery_manager.cpp,
 *                 where the peer's token decides whether the bridge exists),
 *                 both stable API; plus publisher_detection /
 *                 subscriber_detection so the peers can find each other.
 *   DEADLINE      nothing is built. The gateway could time the interval, but
 *                 there is no Zenoh mechanism by which "the interval was
 *                 exceeded" reaches the subscriber as a contract event.
 *                 Stated as a mechanism, deliberately: the *word* does appear,
 *                 in DEFAULT_CONFIG.json5, on an unrelated transmit-queue
 *                 fragment timeout, so a claim phrased as "the word is absent"
 *                 is refuted by one grep. The nearest real mechanism is
 *                 AdvancedSubscriber sample-miss detection, which reports
 *                 sequence-number gaps and fires on a LATER arrival -- so a
 *                 publisher that simply stops produces no notification, which
 *                 is the case a deadline exists for.
 *
 * Two of these are approximations and the code should not pretend otherwise.
 *
 * Recovery is query-based: the subscriber notices a gap in sequence numbers and
 * asks the publisher's cache for what it missed. DDS RELIABLE is NACK-based
 * retransmission from the writer's own history. Both end with the sample
 * delivered; the mechanism, and therefore the latency of a recovery, differ.
 *
 * The subscriber's `history` option reads like DDS HISTORY and is not: it
 * queries a publisher's cache for past samples when the subscriber joins, which
 * is DURABILITY. A callback subscriber keeps no receive-side history of its
 * own -- each sample goes straight to the bridge queue -- and that is fine,
 * because HISTORY is settled at the sink (common/qos_spec.hpp): what the
 * declared depth means on the gateway's own path is the bridge queue's bound
 * layer, and what it means to the outside is the sink publisher's cache.
 *
 * Enabling any of this changes the wire behaviour, so it happens only when the
 * mapping declared something. An undeclared leg still calls the plain
 * declare_publisher / declare_subscriber the submitted gateway called.
 */

namespace zenoh_qos {

namespace zext = zenoh::ext;
using PubOptions = zenoh::ext::SessionExt::AdvancedPublisherOptions;
using SubOptions = zenoh::ext::SessionExt::AdvancedSubscriberOptions;

/// Cache depth to use when a policy needs a cache but no history depth was given.
/// One sample is what TRANSIENT_LOCAL means at its DDS default depth.
inline constexpr size_t kDefaultCacheSamples = 1;

/// Heartbeat period for sample-miss detection. Short enough that a gap at the
/// end of a burst is noticed promptly, long enough not to be a second traffic
/// source in its own right.
inline constexpr uint64_t kHeartbeatPeriodMs = 100;

/**
 * @brief Does this leg need the Advanced API, or will the plain one do?
 *
 * Deadline alone does not, and nothing else picks it up either: nothing is
 * built for it on a Zenoh leg (file comment), and the bridge queue does not
 * stand in for it. A leg declaring only a deadline keeps the plain publisher
 * and the submitted code path; the spec sheet the gateway prints at startup
 * says so.
 *
 * Liveliness IS in the list below, but only for publisher_detection -- which
 * makes this publisher visible to a subscriber that is looking, and is not an
 * assertion of liveness. The assertion is a liveliness token, a stable-API
 * object declared alongside the publisher and independent of Advanced Pub/Sub
 * (zenoh_publisher.cpp, and the matching endpoint in
 * examples/src/ext_nodes/ext_zenoh_pub.cpp). Reading this list as "what carries
 * the policy" would miss it. A declared liveliness always gets the token: it
 * has no parameters (qos_profile.hpp), so there is no value to check.
 */
inline bool needsAdvancedPublisher(const QosProfile& p) {
    if (p.reliability && *p.reliability == ReliabilityKind::RELIABLE) return true;
    if (p.history && p.history->kind == HistoryKind::KEEP_LAST) return true;
    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) return true;
    if (p.liveliness) return true;
    return false;
}

inline bool needsAdvancedSubscriber(const QosProfile& p) {
    if (p.reliability && *p.reliability == ReliabilityKind::RELIABLE) return true;
    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) return true;
    if (p.liveliness) return true;
    return false;
}

/**
 * @brief Build the publisher options a declared profile asks for.
 *
 * The cache is shared machinery: history wants it for depth, durability wants
 * it to serve late joiners, and reliability wants it because retransmission
 * reads from it. Whichever asked for the deepest cache wins. KEEP_ALL builds
 * no cache: the cache is a fixed max_samples with no unbounded setting, and a
 * number invented here would be a depth the mapping never declared.
 */
inline PubOptions buildPublisherOptions(const QosProfile& p) {
    PubOptions opts = PubOptions::create_default();

    size_t cache_samples = 0;
    if (p.history && p.history->kind == HistoryKind::KEEP_LAST) {
        cache_samples = static_cast<size_t>(p.history->depth);
    }
    const bool wants_durability = p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL;
    const bool wants_reliable = p.reliability && *p.reliability == ReliabilityKind::RELIABLE;
    if ((wants_durability || wants_reliable) && cache_samples < kDefaultCacheSamples) {
        cache_samples = kDefaultCacheSamples;
    }

    if (cache_samples > 0) {
        PubOptions::CacheOptions cache = PubOptions::CacheOptions::create_default();
        cache.max_samples = cache_samples;
        opts.cache = cache;
    }

    if (wants_reliable) {
        // Sequence numbers let a subscriber see a gap; the heartbeat is what
        // makes a gap at the *end* of a burst visible, since there is no later
        // sample to reveal it.
        PubOptions::SampleMissDetectionOptions miss = PubOptions::SampleMissDetectionOptions::create_default();
        miss.heartbeat = PubOptions::SampleMissDetectionOptions::HeartbeatPeriodic{kHeartbeatPeriodMs};
        opts.sample_miss_detection = miss;
    } else if (cache_samples > 0) {
        // A cache with no sample-miss detection does not declare. zenoh-ext
        // sequences a cached publisher by TIMESTAMP unless sample-miss
        // detection switches it to sequence numbers, and timestamp sequencing
        // bails unless the session config enables `timestamping`
        // (advanced_publisher.rs: "the 'timestamping' setting must be enabled
        // in the Zenoh configuration"; zenoh-c surfaces it as -128). Found
        // 2026-09-05 by the crossing harness: a HISTORY- or DURABILITY-only
        // Zenoh sink had never been built before, and it aborted. Sequence
        // numbers with no heartbeat fix it in code rather than in deployment
        // config: the samples carry a number the cache is indexed by, nothing
        // is sent on a timer, and a subscriber not asking for recovery
        // ignores it. What this leg builds stays what the spec sheet says.
        opts.sample_miss_detection = PubOptions::SampleMissDetectionOptions::create_default();
    }

    if (p.liveliness) {
        opts.publisher_detection = true;
    }
    return opts;
}

/**
 * @brief Build the subscriber options a declared profile asks for.
 */
inline SubOptions buildSubscriberOptions(const QosProfile& p) {
    SubOptions opts = SubOptions::create_default();

    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) {
        SubOptions::HistoryOptions hist = SubOptions::HistoryOptions::create_default();
        hist.detect_late_publishers = true;
        // 0 means "no limit" on the subscriber side. The declared depth is the
        // sink's business as far as the spec sheet is concerned, but if the
        // mapping gave one it is still the best statement of how much past
        // data is wanted when this leg joins late.
        hist.max_samples = (p.history && p.history->kind == HistoryKind::KEEP_LAST)
                               ? static_cast<size_t>(p.history->depth)
                               : 0;
        opts.history = hist;
    }

    if (p.reliability && *p.reliability == ReliabilityKind::RELIABLE) {
        SubOptions::RecoveryOptions rec = SubOptions::RecoveryOptions::create_default();
        // Match the publisher: it heartbeats, so listen for heartbeats rather
        // than polling with periodic queries.
        rec.last_sample_miss_detection = SubOptions::RecoveryOptions::Heartbeat{};
        opts.recovery = rec;
    }

    if (p.liveliness) {
        opts.subscriber_detection = true;
    }
    return opts;
}

/// One-line summary of what the Advanced API was actually asked for.
inline std::string describePublisher(const QosProfile& p) {
    std::string s;
    auto add = [&s](const std::string& part) {
        if (!s.empty()) s += " ";
        s += part;
    };
    if (p.history && p.history->kind == HistoryKind::KEEP_LAST) {
        add("cache=" + std::to_string(p.history->depth));
    }
    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) add("late-join");
    if (p.reliability && *p.reliability == ReliabilityKind::RELIABLE) {
        add("recovery/hb");
    } else if (!s.empty()) {
        // A cache was built (history or durability) without RELIABLE: the
        // sequence numbers are there so the cache can be declared at all
        // (buildPublisherOptions), and there is no heartbeat.
        add("seqnum/no-hb");
    }
    // Both, and they are not the same thing: publisher-detection is an
    // Advanced option that makes this publisher findable, the token is the
    // stable-API object whose disappearance is the reportable event.
    if (p.liveliness) add("liveliness-token");
    return s.empty() ? "plain publisher" : s;
}

inline std::string describeSubscriber(const QosProfile& p) {
    std::string s;
    auto add = [&s](const std::string& part) {
        if (!s.empty()) s += " ";
        s += part;
    };
    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) {
        // Print the max_samples that was actually handed to Zenoh, not just
        // that a history block was set: the declared depth lands here as the
        // late-join fetch bound, and the log should say so.
        const size_t fetch = (p.history && p.history->kind == HistoryKind::KEEP_LAST)
                                 ? static_cast<size_t>(p.history->depth)
                                 : 0;
        add("fetch=" + (fetch == 0 ? std::string("all") : std::to_string(fetch)));
    }
    if (p.reliability && *p.reliability == ReliabilityKind::RELIABLE) add("recovery/hb");
    if (p.liveliness) add("liveliness-watch");
    return s.empty() ? "plain subscriber" : s;
}

}  // namespace zenoh_qos

#endif  // ZENOH_ZENOH_QOS_APPLY_HPP
