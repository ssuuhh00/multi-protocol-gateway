#ifndef EXAMPLES_EXT_NODES_EXT_FACTORY_HPP
#define EXAMPLES_EXT_NODES_EXT_FACTORY_HPP

#include <iomanip>
#include <memory>
#include <sstream>
#include <stdexcept>
#include <string>

#include <fastdds/dds/domain/DomainParticipant.hpp>
#include <vsomeip/vsomeip.hpp>
#include <zenoh.hxx>

#include "common/command.hpp"
#include "ext_nodes/ext_dds_pub.hpp"
#include "ext_nodes/ext_dds_sub.hpp"
#include "ext_nodes/ext_node.hpp"
#include "ext_nodes/ext_someip_client.hpp"
#include "ext_nodes/ext_someip_server.hpp"
#include "ext_nodes/ext_zenoh_pub.hpp"
#include "ext_nodes/ext_zenoh_sub.hpp"

/**
 * @file ext_factory.hpp
 * @brief Build the two external endpoints of one mapping, from the same Command
 *        the gateway builds its bridge from.
 *
 * Everything here takes a `Command`, never raw JSON. That is the point.
 *
 * A QoS declaration is a contract between two peers: if only the gateway
 * declares it, the DDS request/offer rules can leave the pair unmatched and the
 * bridge carries nothing. So the endpoints must declare *the same thing the
 * gateway declared* -- and "the same thing" only holds if the same parser
 * produced it. Two parsers over one file is how the two ends of a bridge come
 * to disagree while both files look right.
 *
 * loadDiscoveryMappingsFromJson() (discovery/discovery_mapping.hpp) is that one
 * parser. It validates what this file then merely reads: unknown policy names
 * and unknown values are rejected there rather than coerced to a default,
 * because accepting a declaration and then quietly dropping it is exactly the
 * silent failure the fine-grained QoS work exists to remove.
 *
 * Both consumers of this header -- examples/src/demo.cpp and
 * exp/runner/ext_runner.cpp -- therefore build endpoints from a validated
 * Command, and neither owns a copy of the schema.
 */

namespace ext_factory {

inline const char* protoName(ProtocolType p) {
    switch (p) {
        case ProtocolType::DDS:    return "DDS";
        case ProtocolType::SOMEIP: return "SOMEIP";
        case ProtocolType::ZENOH:  return "ZENOH";
        default:                   return "NONE";
    }
}

inline std::string routingLabel(ProtocolType src, ProtocolType sink) {
    return std::string(protoName(src)) + "->" + protoName(sink);
}

/**
 * @brief What an endpoint of protocol @p p binds to in mapping @p cmd.
 *
 * Also what the monitor shows as the endpoint's address, so it reads the way
 * the config does rather than the way the middleware does.
 */
inline std::string addressOf(ProtocolType p, const Command& cmd) {
    switch (p) {
        case ProtocolType::DDS:
            return cmd.topic;
        case ProtocolType::ZENOH:
            return cmd.zenoh_key;
        case ProtocolType::SOMEIP: {
            std::ostringstream os;
            os << "S" << std::hex << std::setw(4) << std::setfill('0') << cmd.service_id.value_or(0) << ":E"
               << std::setw(4) << std::setfill('0') << cmd.event_id.value_or(0);
            return os.str();
        }
        default:
            return "?";
    }
}

/// Default identity for an endpoint, when the mapping does not name one.
inline std::string defaultName(ProtocolType p, ExtRole role, uint32_t idx) {
    return std::string("ext_") + protoName(p) + (role == ExtRole::PRODUCER ? "_pub_" : "_sub_") + std::to_string(idx);
}

inline EndpointSpec specFor(ProtocolType p, ExtRole role, const Command& cmd, uint32_t idx, std::string name = {}) {
    EndpointSpec s;
    s.role = role;
    s.mapping_index = idx;
    s.address = addressOf(p, cmd);
    s.node_name = name.empty() ? defaultName(p, role, idx) : std::move(name);
    return s;
}

/**
 * @brief The endpoint that feeds the gateway's source leg.
 *
 * The SOME/IP producer is a *server*: it offers the event the gateway's SOME/IP
 * client subscribes to. Getting that round the wrong way builds two subscribers
 * and no publisher, and the symptom is an empty bridge rather than an error.
 */
inline std::unique_ptr<ExtPublisher> makeProducer(const Command& cmd, uint32_t idx,
                                                  eprosima::fastdds::dds::DomainParticipant* participant,
                                                  const std::shared_ptr<zenoh::Session>& zenoh_session,
                                                  const std::shared_ptr<vsomeip::application>& someip_app,
                                                  std::string name = {}) {
    EndpointSpec spec = specFor(cmd.source, ExtRole::PRODUCER, cmd, idx, std::move(name));
    switch (cmd.source) {
        case ProtocolType::DDS:
            if (!participant) throw std::runtime_error("makeProducer: DDS source needs a DomainParticipant");
            return std::make_unique<ExtDdsPub>(participant, std::move(spec), cmd.message_type, cmd.qos,
                                               cmd.qos_policies);
        case ProtocolType::ZENOH:
            if (!zenoh_session) throw std::runtime_error("makeProducer: Zenoh source needs a session");
            return std::make_unique<ExtZenohPub>(zenoh_session, std::move(spec), cmd.qos_policies);
        case ProtocolType::SOMEIP:
            if (!someip_app) throw std::runtime_error("makeProducer: SOME/IP source needs an application");
            return std::make_unique<ExtSomeipServer>(someip_app, std::move(spec), cmd.service_id.value(),
                                                     cmd.instance_id.value(), cmd.eventgroup_id.value(),
                                                     cmd.event_id.value(), cmd.qos_policies);
        default:
            throw std::runtime_error("makeProducer: unsupported source protocol");
    }
}

/// The endpoint that drains the gateway's sink leg.
inline std::unique_ptr<ExtSubscriber> makeConsumer(const Command& cmd, uint32_t idx,
                                                   eprosima::fastdds::dds::DomainParticipant* participant,
                                                   const std::shared_ptr<zenoh::Session>& zenoh_session,
                                                   const std::shared_ptr<vsomeip::application>& someip_app,
                                                   std::string name = {}) {
    EndpointSpec spec = specFor(cmd.sink, ExtRole::CONSUMER, cmd, idx, std::move(name));
    switch (cmd.sink) {
        case ProtocolType::DDS:
            if (!participant) throw std::runtime_error("makeConsumer: DDS sink needs a DomainParticipant");
            return std::make_unique<ExtDdsSub>(participant, std::move(spec), cmd.message_type, cmd.qos,
                                               cmd.qos_policies);
        case ProtocolType::ZENOH:
            if (!zenoh_session) throw std::runtime_error("makeConsumer: Zenoh sink needs a session");
            return std::make_unique<ExtZenohSub>(zenoh_session, std::move(spec), cmd.qos_policies);
        case ProtocolType::SOMEIP:
            if (!someip_app) throw std::runtime_error("makeConsumer: SOME/IP sink needs an application");
            return std::make_unique<ExtSomeipClient>(someip_app, std::move(spec), cmd.service_id.value(),
                                                     cmd.instance_id.value(), cmd.eventgroup_id.value(),
                                                     cmd.event_id.value(), cmd.qos_policies);
        default:
            throw std::runtime_error("makeConsumer: unsupported sink protocol");
    }
}

/// True when any mapping in @p cmds puts SOME/IP on either leg.
template <typename Range>
inline bool usesSomeip(const Range& cmds) {
    for (const Command& c : cmds) {
        if (c.source == ProtocolType::SOMEIP || c.sink == ProtocolType::SOMEIP) return true;
    }
    return false;
}

}  // namespace ext_factory

#endif  // EXAMPLES_EXT_NODES_EXT_FACTORY_HPP
