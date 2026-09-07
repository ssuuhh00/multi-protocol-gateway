#ifndef SOMEIP_SOMEIP_QOS_APPLY_HPP
#define SOMEIP_SOMEIP_QOS_APPLY_HPP

#include <vsomeip/vsomeip.hpp>

#include <string>

#include "common/qos_profile.hpp"

/**
 * @file someip_qos_apply.hpp
 * @brief Translate a declared QosProfile onto vSomeIP's event arguments.
 *
 * vSomeIP has no QoS object either. What it has is a handful of optional
 * arguments on offer_event() and request_event() that the submitted gateway
 * left at their defaults -- all six of them on the offering side, both on the
 * requesting side. So most of this leg is not new capability, it is calling
 * what was already there.
 *
 *   offer_event(svc, inst, notifier, groups,
 *               event_type_e       _type                = ET_EVENT,
 *               milliseconds       _cycle               = 0,
 *               bool               _change_resets_cycle = false,
 *               bool               _update_on_change    = true,
 *               epsilon_change_func_t _epsilon          = nullptr,
 *               reliability_type_e _reliability         = RT_UNKNOWN);
 *   request_event(svc, inst, event, groups,
 *               event_type_e       _type                = ET_EVENT,
 *               reliability_type_e _reliability         = RT_UNKNOWN);
 *
 * Two knobs, one policy each:
 *
 *   reliability_type_e   RELIABILITY. RT_RELIABLE selects the TCP endpoint.
 *   ET_FIELD             DURABILITY at TRANSIENT_LOCAL, depth one, fixed. A
 *                        field is exactly one value kept and handed to whoever
 *                        subscribes next (routing_manager_base: non-fields
 *                        have their payload unset).
 *
 * ET_FIELD is NOT History. A vSomeIP event holds one current payload and one
 * staging slot (event.hpp: current_/update_), and the staging slot assembles
 * the next value rather than remembering a previous one, so there is no depth
 * at any layer and no configuration key for one. The gateway therefore reads
 * no history depth on a SOME/IP leg: the declared depth bounds the gateway's
 * own queue and the spec sheet says the SOME/IP sink builds nothing
 * for it (common/qos_spec.hpp). Calling depth one "history" on one role and
 * not the other, as an earlier version did, was the same field counted twice.
 *
 * The event type is decided by durability alone and is the same on both
 * roles. That matters beyond tidiness: vsomeip registers one event type per
 * (service, instance, event) and rejects a second registration that
 * disagrees, so an external endpoint derives its type from the same mapping
 * and gets the same answer whichever side of the gateway it sits on.
 *
 * What is deliberately NOT wired: offer_event's `_cycle`. It is real -- it
 * makes vSomeIP re-notify the current value periodically -- but it generates a
 * cadence, it never checks one, so it is not a deadline in the sense the
 * mapping declares. Honouring a deadline declaration by injecting traffic the
 * mapping never asked to send would change throughput measurements, surprise
 * anyone reading the wire, and contradict the spec sheet, which builds nothing
 * for DEADLINE on a SOME/IP leg. Saying "vSomeIP offers a cyclic refresh and we
 * chose not to use it, for these reasons" is also a better answer to a
 * reviewer than quietly using it.
 */

namespace someip_qos {

/**
 * @brief Event type a profile implies: ET_FIELD for TRANSIENT_LOCAL, else ET_EVENT.
 */
inline vsomeip::event_type_e eventType(const QosProfile& p) {
    if (p.durability && *p.durability == DurabilityKind::TRANSIENT_LOCAL) {
        return vsomeip::event_type_e::ET_FIELD;
    }
    return vsomeip::event_type_e::ET_EVENT;
}

/**
 * @brief Reliability the profile asks for.
 *
 * RT_UNKNOWN is vSomeIP's "you decide", which in practice means the endpoint
 * the service discovery configuration offers. Declaring nothing keeps it.
 */
inline vsomeip::reliability_type_e reliability(const QosProfile& p) {
    if (!p.reliability) {
        return vsomeip::reliability_type_e::RT_UNKNOWN;
    }
    return *p.reliability == ReliabilityKind::RELIABLE ? vsomeip::reliability_type_e::RT_RELIABLE
                                                       : vsomeip::reliability_type_e::RT_UNRELIABLE;
}

inline const char* toString(vsomeip::event_type_e t) {
    switch (t) {
        case vsomeip::event_type_e::ET_EVENT:           return "ET_EVENT";
        case vsomeip::event_type_e::ET_SELECTIVE_EVENT: return "ET_SELECTIVE_EVENT";
        case vsomeip::event_type_e::ET_FIELD:           return "ET_FIELD";
        default:                                        return "ET_UNKNOWN";
    }
}

inline const char* toString(vsomeip::reliability_type_e r) {
    switch (r) {
        case vsomeip::reliability_type_e::RT_RELIABLE:   return "RT_RELIABLE";
        case vsomeip::reliability_type_e::RT_UNRELIABLE: return "RT_UNRELIABLE";
        case vsomeip::reliability_type_e::RT_BOTH:       return "RT_BOTH";
        default:                                         return "RT_UNKNOWN";
    }
}

/// One-line summary of what was handed to vSomeIP.
inline std::string describe(const QosProfile& p) {
    std::string s = toString(eventType(p));
    s += " ";
    s += toString(reliability(p));
    if (p.liveliness) {
        s += " availability-reporting";
    }
    return s;
}

}  // namespace someip_qos

#endif  // SOMEIP_SOMEIP_QOS_APPLY_HPP
