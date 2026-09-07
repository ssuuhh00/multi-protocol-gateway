#ifndef DDS_DDS_QOS_APPLY_HPP
#define DDS_DDS_QOS_APPLY_HPP

#include <spdlog/spdlog.h>

#include <fastdds/dds/publisher/qos/DataWriterQos.hpp>
#include <fastdds/dds/subscriber/qos/DataReaderQos.hpp>
#include <memory>
#include <string>

#include "common/qos_profile.hpp"

/**
 * @file dds_qos_apply.hpp
 * @brief Translate a declared QosProfile onto Fast DDS reader/writer QoS.
 *
 * Fast DDS 2.14.5 carries all five policies in both roles, so every one of
 * these is a direct assignment rather than an approximation. The two policies
 * Fast DDS does refuse -- PERSISTENT durability and BY_SOURCE_TIMESTAMP
 * destination order -- are outside the five and unreachable from the
 * declaration surface.
 *
 * Only declared fields are touched. An absent optional leaves the DDS default
 * in place, which is what keeps a mapping without a qos_policies block on the
 * submitted gateway's exact path.
 *
 * Two things are fixed here rather than read from the mapping, and both are
 * spec-sheet decisions (common/qos_spec.hpp):
 *
 *   LIVELINESS  is built as AUTOMATIC with no lease on every leg, because
 *               that is the one shape all three middlewares have. A finite
 *               lease is a bound only DDS could enforce, so the mapping does
 *               not declare one and nothing here reads one.
 *   DEADLINE    is a sink policy. The gateway's DDS writer OFFERS the declared
 *               period, and DDS itself then measures it at both ends -- the
 *               external reader is told by on_requested_deadline_missed, the
 *               writer by on_offered_deadline_missed. The gateway's own source
 *               reader does not request one: sourceLeg() strips it. A request
 *               there would be seen only in the gateway's log while making the
 *               external writer's offer a matching condition (RxO: offered <=
 *               requested) for nothing in return.
 *
 * On RxO more generally: a reader that requests a contract the writer does
 * not offer simply never matches, and the bridge then never carries data.
 * Every such rejection is made loud -- the incompatible-QoS callbacks name the
 * offending policy -- rather than avoided by quietly not applying something.
 * A declaration that is honoured but unmatched, and says so, is the behaviour
 * being asked for; one silently dropped to keep the bridge up is the
 * behaviour being complained about.
 */

namespace dds_qos {

namespace efd = eprosima::fastdds::dds;

inline efd::ReliabilityQosPolicyKind toDds(ReliabilityKind k) {
    return k == ReliabilityKind::RELIABLE ? efd::RELIABLE_RELIABILITY_QOS : efd::BEST_EFFORT_RELIABILITY_QOS;
}

inline efd::HistoryQosPolicyKind toDds(HistoryKind k) {
    return k == HistoryKind::KEEP_LAST ? efd::KEEP_LAST_HISTORY_QOS : efd::KEEP_ALL_HISTORY_QOS;
}

inline efd::DurabilityQosPolicyKind toDds(DurabilityKind k) {
    return k == DurabilityKind::VOLATILE ? efd::VOLATILE_DURABILITY_QOS : efd::TRANSIENT_LOCAL_DURABILITY_QOS;
}

inline eprosima::fastrtps::Duration_t fromMs(uint32_t ms) {
    return eprosima::fastrtps::Duration_t(static_cast<int32_t>(ms / 1000),
                                          static_cast<uint32_t>((ms % 1000) * 1000000ULL));
}

/**
 * A KEEP_LAST depth deeper than ResourceLimits allows is not rejected by
 * Fast DDS, only warned about, and the history then silently caps at the
 * resource limit instead of the declared depth. Raise the limits so the
 * declared number is the number that holds.
 */
template <typename QosT>
inline void widenResourceLimits(QosT& qos, int32_t depth) {
    auto& rl = qos.resource_limits();
    if (rl.max_samples_per_instance > 0 && rl.max_samples_per_instance < depth) {
        rl.max_samples_per_instance = depth;
    }
    const int32_t instances = rl.max_instances > 0 ? rl.max_instances : 1;
    const int64_t needed = static_cast<int64_t>(instances) * rl.max_samples_per_instance;
    if (rl.max_samples > 0 && rl.max_samples < needed) {
        rl.max_samples = static_cast<int32_t>(needed);
    }
}

/**
 * The one liveliness the gateway builds: AUTOMATIC, no lease. Both are the
 * Fast DDS defaults, and they are set explicitly anyway so that "what was
 * built" is literally in the code rather than inherited.
 */
template <typename QosT>
inline void applyLiveliness(QosT& qos) {
    qos.liveliness().kind = efd::AUTOMATIC_LIVELINESS_QOS;
    qos.liveliness().lease_duration = eprosima::fastrtps::c_TimeInfinite;
}

/**
 * @brief The profile as the gateway's own DDS source reader applies it.
 *
 * Identical to the declaration except that DEADLINE is removed -- see the file
 * comment. The external endpoints do not use this: an external reader at the
 * gateway's DDS sink is exactly the endpoint the offered deadline is for, and
 * it applies the full profile.
 */
inline QosProfile sourceLeg(const QosProfile& p) {
    QosProfile leg = p;
    leg.deadline_ms.reset();
    return leg;
}

/// Apply the declared policies to a DataReader's QoS. Undeclared ones are left alone.
inline void apply(efd::DataReaderQos& qos, const QosProfile& p) {
    if (p.reliability) {
        qos.reliability().kind = toDds(*p.reliability);
    }
    if (p.history) {
        qos.history().kind = toDds(p.history->kind);
        if (p.history->kind == HistoryKind::KEEP_LAST) {
            qos.history().depth = p.history->depth;
            widenResourceLimits(qos, p.history->depth);
        }
    }
    if (p.deadline_ms) {
        qos.deadline().period = fromMs(*p.deadline_ms);
    }
    if (p.liveliness) {
        applyLiveliness(qos);
    }
    if (p.durability) {
        qos.durability().kind = toDds(*p.durability);
    }
}

/// Apply the declared policies to a DataWriter's QoS. Undeclared ones are left alone.
inline void apply(efd::DataWriterQos& qos, const QosProfile& p) {
    if (p.reliability) {
        qos.reliability().kind = toDds(*p.reliability);
    }
    if (p.history) {
        qos.history().kind = toDds(p.history->kind);
        if (p.history->kind == HistoryKind::KEEP_LAST) {
            qos.history().depth = p.history->depth;
            widenResourceLimits(qos, p.history->depth);
        }
    }
    if (p.deadline_ms) {
        qos.deadline().period = fromMs(*p.deadline_ms);
    }
    if (p.liveliness) {
        applyLiveliness(qos);
    }
    if (p.durability) {
        qos.durability().kind = toDds(*p.durability);
    }
}

/**
 * @brief Name the policy an RxO rejection blamed.
 *
 * Fast DDS reports the offending policy as a QosPolicyId_t on the incompatible
 * status. Turning it back into a name is what makes the log line usable as
 * evidence rather than a number.
 */
inline const char* policyName(uint32_t policy_id) {
    switch (policy_id) {
        case efd::DURABILITY_QOS_POLICY_ID:     return "DURABILITY";
        case efd::DEADLINE_QOS_POLICY_ID:       return "DEADLINE";
        case efd::LATENCYBUDGET_QOS_POLICY_ID:  return "LATENCY_BUDGET";
        case efd::LIVELINESS_QOS_POLICY_ID:     return "LIVELINESS";
        case efd::RELIABILITY_QOS_POLICY_ID:    return "RELIABILITY";
        case efd::DESTINATIONORDER_QOS_POLICY_ID: return "DESTINATION_ORDER";
        case efd::HISTORY_QOS_POLICY_ID:        return "HISTORY";
        case efd::RESOURCELIMITS_QOS_POLICY_ID: return "RESOURCE_LIMITS";
        case efd::OWNERSHIP_QOS_POLICY_ID:      return "OWNERSHIP";
        case efd::PRESENTATION_QOS_POLICY_ID:   return "PRESENTATION";
        case efd::PARTITION_QOS_POLICY_ID:      return "PARTITION";
        case efd::TOPICDATA_QOS_POLICY_ID:      return "TOPIC_DATA";
        case efd::LIFESPAN_QOS_POLICY_ID:       return "LIFESPAN";
        default:                                return "UNKNOWN";
    }
}

/// One-line summary of what was actually set, for the creation log.
inline std::string describe(const QosProfile& p) {
    if (p.empty()) {
        return "none declared";
    }
    std::string s;
    auto add = [&s](const std::string& part) {
        if (!s.empty()) s += " ";
        s += part;
    };
    if (p.reliability) add(to_string(*p.reliability));
    if (p.history) {
        add(p.history->kind == HistoryKind::KEEP_ALL ? std::string("KEEP_ALL")
                                                     : "KEEP_LAST(" + std::to_string(p.history->depth) + ")");
    }
    if (p.deadline_ms) add("deadline=" + std::to_string(*p.deadline_ms) + "ms");
    if (p.liveliness) add("liveliness=AUTOMATIC/no-lease");
    if (p.durability) add(to_string(*p.durability));
    return s;
}

}  // namespace dds_qos

#endif  // DDS_DDS_QOS_APPLY_HPP
