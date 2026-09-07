#ifndef COMMON_QOS_PROFILE_HPP
#define COMMON_QOS_PROFILE_HPP

#include <cstdint>
#include <optional>

/**
 * @file qos_profile.hpp
 * @brief Fine-grained QoS declaration, one per mapping.
 *
 * The submitted gateway carried exactly one QoS value -- Qos::RELIABLE or
 * Qos::BEST_EFFORT (common/command.hpp) -- shared by both legs of a mapping.
 * The five policies here are the three the reviewers named (Deadline,
 * Liveliness, History), plus the Reliability that already existed, plus the
 * Durability the digital-twin use case needs.
 *
 * Every field is std::optional and every one starts out unset. That is
 * load-bearing rather than stylistic: an undeclared policy must leave the
 * middleware knob untouched, so a mapping with no "qos_policies" block takes
 * the same code path the submitted gateway took. empty() is what the rest of
 * the gateway tests before it does anything at all.
 *
 * A declaration is a request for a FORM, not a negotiation. For every
 * (policy, conversion direction) the gateway builds one fixed thing -- the
 * spec sheet in qos_spec.hpp says which -- and the operator matches the
 * external node to it. That is why the surface below is as narrow as it is:
 * a value the gateway would have to approximate is not accepted in the first
 * place. Liveliness is the visible case. It has no kind and no lease, because
 * the gateway builds it the same way on every leg (an AUTOMATIC assertion with
 * no time bound: a DDS writer at its default, a Zenoh liveliness token, a
 * SOME/IP service-discovery offer) and a lease is a bound only one of the three
 * middlewares could enforce. Reading a lease and then not enforcing it would
 * be a declaration the gateway accepted and silently dropped.
 */

enum class ReliabilityKind { RELIABLE, BEST_EFFORT };
enum class HistoryKind { KEEP_LAST, KEEP_ALL };
enum class DurabilityKind { VOLATILE, TRANSIENT_LOCAL };

struct HistorySpec {
    HistoryKind kind = HistoryKind::KEEP_LAST;
    int32_t depth = 1;  // ignored when kind == KEEP_ALL
};

/**
 * @struct LivelinessSpec
 * @brief Declared or not; nothing else. See the file comment for why.
 *
 * Kept as a struct rather than a bool so that `p.liveliness` reads like the
 * other four optionals and so that a parameter can be added later without
 * touching every call site -- but there is deliberately none.
 */
struct LivelinessSpec {};

/**
 * @struct QosProfile
 * @brief What one mapping declared. Unset == not declared.
 */
struct QosProfile {
    std::optional<ReliabilityKind> reliability;
    std::optional<HistorySpec> history;
    std::optional<uint32_t> deadline_ms;
    std::optional<LivelinessSpec> liveliness;
    std::optional<DurabilityKind> durability;

    bool empty() const {
        return !reliability && !history && !deadline_ms && !liveliness && !durability;
    }
};

/**
 * @enum QosPolicyId
 * @brief Iteration handle for the five policies. Used by the spec sheet.
 */
enum class QosPolicyId { RELIABILITY, DEADLINE, HISTORY, LIVELINESS, DURABILITY };

inline const char* to_string(QosPolicyId p) {
    switch (p) {
        case QosPolicyId::RELIABILITY: return "RELIABILITY";
        case QosPolicyId::DEADLINE:    return "DEADLINE";
        case QosPolicyId::HISTORY:     return "HISTORY";
        case QosPolicyId::LIVELINESS:  return "LIVELINESS";
        case QosPolicyId::DURABILITY:  return "DURABILITY";
    }
    return "?";
}

inline const char* to_string(ReliabilityKind k) {
    return k == ReliabilityKind::RELIABLE ? "RELIABLE" : "BEST_EFFORT";
}

inline const char* to_string(HistoryKind k) {
    return k == HistoryKind::KEEP_LAST ? "KEEP_LAST" : "KEEP_ALL";
}

inline const char* to_string(DurabilityKind k) {
    return k == DurabilityKind::VOLATILE ? "VOLATILE" : "TRANSIENT_LOCAL";
}

/// True when the profile declared @p policy. Lets callers loop over the five.
inline bool declares(const QosProfile& p, QosPolicyId policy) {
    switch (policy) {
        case QosPolicyId::RELIABILITY: return p.reliability.has_value();
        case QosPolicyId::DEADLINE:    return p.deadline_ms.has_value();
        case QosPolicyId::HISTORY:     return p.history.has_value();
        case QosPolicyId::LIVELINESS:  return p.liveliness.has_value();
        case QosPolicyId::DURABILITY:  return p.durability.has_value();
    }
    return false;
}

#endif  // COMMON_QOS_PROFILE_HPP
