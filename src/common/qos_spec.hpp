#ifndef COMMON_QOS_SPEC_HPP
#define COMMON_QOS_SPEC_HPP

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

#include "common/command.hpp"
#include "common/qos_profile.hpp"

/**
 * @file qos_spec.hpp
 * @brief The spec sheet: for each (policy, conversion direction), what the
 *        gateway builds when a mapping declares that policy.
 *
 * This file replaced a negotiator. The negotiator evaluated each declared
 * policy per leg and answered "applied" or "downgraded, and here is the nearest
 * thing instead". That model was wrong in its premise: the five policies are
 * independent options, not rungs of a ladder, so there is no "nearer" value to
 * fall back to, and a gateway that reads a value it cannot express and then
 * builds something else has accepted a declaration and dropped it -- the very
 * behaviour the reviewers asked about.
 *
 * What stands in its place is a table with no runtime decision in it. For
 * every (policy, direction) the gateway builds one fixed thing; the table says
 * which, in words an operator uses to configure the external node, and the
 * gateway prints the relevant rows at startup. A value the table cannot
 * express is not read from the mapping at all (qos_profile.hpp: liveliness has
 * no lease and no kind).
 *
 * The table is per DIRECTION, not per protocol, because the policies are not
 * settled at the same place:
 *
 *   RELIABILITY   each leg separately -- the two hops are independent
 *   DEADLINE      the sink only -- it is a contract at the receiving end, and
 *                 nothing about it is transported
 *   HISTORY       the sink -- retention happens at the endpoint that offers it
 *   DURABILITY    the sink -- same
 *   LIVELINESS    both legs, joined by the bridge's lifecycle -- the source's
 *                 assertion (a discovered writer, an offered service, a
 *                 liveliness token) decides whether the bridge exists, and the
 *                 sink endpoint exists with it (discovery_manager.hpp)
 *
 * A protocol-keyed table got three cells wrong for that reason (a SOME/IP
 * "history" that was the same field on both roles, a Zenoh source "history"
 * nobody outside can observe, and a deadline "no direction carries" when two
 * of the six do). Direction is the key that does not produce those errors.
 *
 * "carried" means one thing: the guarantee is one the two external endpoints
 * can verify through their own middleware. The gateway can imitate a policy
 * in its own application code -- time an interval, keep a buffer -- but an
 * imitation inside the gateway is not a contract the far endpoint can request
 * or check, so it is not in the spec. That is the same line drawn around the
 * bridge queue below.
 *
 * The strings are static and are the words used in the response to reviewers,
 * so they are written to survive a reviewer opening the same headers. In
 * particular they never claim a word is absent from a library ("deadline" is
 * in Zenoh's DEFAULT_CONFIG.json5, on an unrelated fragment-drop timeout) and
 * never say a protocol "does not support" a policy -- partial implementations
 * and workarounds exist for most of them. They say what mechanism the
 * middleware has, and what the gateway therefore builds.
 */

/// Short direction label, the one the experiments use: "d2z", "s2d", ...
const char* directionLabel(ProtocolType source, ProtocolType sink);

/**
 * @struct QosSpecCell
 * @brief One cell of the sheet: what is built for this policy in this direction.
 *
 * `source` and `sink` say what each leg is built with; an empty string means
 * that leg builds nothing for this policy. `note` is, for a cell that is not
 * carried, the mechanism that is missing; for a carried one, anything the
 * operator should know about the form it is built in (a SOME/IP field holds
 * one value; a Zenoh cache has no unbounded setting, so KEEP_ALL builds none).
 * Those are notes, not cells: a field that keeps the current value IS
 * durability, and a cache that keeps n IS history -- depth is history's
 * business, and the note says what depth.
 */
struct QosSpecCell {
    bool carried = false;
    const char* source = "";
    const char* sink = "";
    const char* note = "";

    /// One clause, for the startup log. `note` is written to be read once, by
    /// someone who went looking; a log line is read every time the gateway
    /// starts, by someone who did not. Set only where `carried` is false --
    /// that is the case a log has to surface. The full `note` is still printed,
    /// at debug.
    const char* brief = "";
};

/**
 * @brief Look up the cell for @p policy in the direction @p source -> @p sink.
 *
 * @param p the declared profile. A few cells depend on the declared VALUE, not
 *          only the policy -- Zenoh carries KEEP_LAST(n) and not KEEP_ALL, and
 *          a BEST_EFFORT leg is built as the plain endpoint -- so the profile
 *          is consulted for the value. Nothing else about it is negotiated.
 *
 * Calling this for a policy @p p does not declare is allowed: the cell then
 * describes what would be built had it been declared (the sheet printer uses
 * that).
 */
QosSpecCell qosSpec(ProtocolType source, ProtocolType sink, QosPolicyId policy, const QosProfile& p);

/// Human-readable declared value, for the startup log: "KEEP_LAST(16)", "100ms", ...
std::string describeValue(QosPolicyId policy, const QosProfile& p);

/**
 * @struct QosSpecEntry
 * @brief One declared policy of one mapping, with its cell.
 */
struct QosSpecEntry {
    QosPolicyId policy;
    std::string value;
    QosSpecCell cell;
};

/**
 * @brief The rows of the sheet a mapping's declaration selects, in policy order.
 *
 * Returns an empty vector when nothing was declared, which is the submitted
 * gateway's case and must stay indistinguishable from it.
 */
std::vector<QosSpecEntry> qosSpecFor(const Command& cmd);

/// How many of the declared policies this direction does not carry.
size_t countNotCarried(const std::vector<QosSpecEntry>& spec);

/**
 * @struct QueueQos
 * @brief The bridge queue's own configuration: a depth bound and nothing else.
 *
 * The queue is an ordinary bounded queue, deliberately. It has exactly two
 * behaviours -- unbounded, or bounded with the oldest discarded -- because those
 * two are what the declaration already means and because a third would be a
 * behaviour no middleware on either side asked for. It does not implement any
 * protocol's congestion vocabulary; Zenoh's BLOCK/DROP/BLOCK_FIRST and vSomeIP's
 * byte-limited endpoint queues are those libraries' business, at their own layer.
 *
 * The mapping is one line:
 *
 *   HISTORY KEEP_LAST(n)  ->  bound n, discard the oldest   (latest-data-first)
 *   HISTORY KEEP_ALL      ->  unbounded, discard nothing    (lossless)
 *   HISTORY not declared  ->  unbounded                     (submitted path)
 *
 * Those two behaviours are the ones the submitted conclusion names
 * (05_conclusion.tex): "lossless control of Reliable paths and
 * latest-data-first handling along Best-effort paths".
 *
 * Nothing else reaches the queue. Reliability, deadline, liveliness and
 * durability are contracts between endpoints and are settled at the legs. And
 * the queue is not a stand-in for a leg that builds nothing: it mirrors
 * the declared depth on the gateway's own path whether or not the sink can
 * offer that depth, and the spec sheet never counts it as carrying anything.
 */
struct QueueQos {
    size_t max_depth = 0;  //!< 0 = no cap
    uint32_t policy = 0;   //!< QueuePolicy (connection_queue.hpp); 0 == Q_UNBOUNDED

    bool configured() const { return policy != 0; }
};

/// Derive the queue's configuration straight from the declaration.
QueueQos queueQosFor(const QosProfile& p);

#endif  // COMMON_QOS_SPEC_HPP
