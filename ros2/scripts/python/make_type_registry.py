#!/usr/bin/env python3
"""Generate ros2_msg_type_registry.hpp and ros2_msg_data_converter.hpp

Usage: python3 make_type_registry.py <config_file> <registry_output> <converter_output>

Used in update_registry.sh

Keep package hierarchy order from ros2_msg_config.h:
  builtin_interfaces → std_msgs → geometry_msgs → sensor_msgs → nav_msgs → (Other alphabetical)
"""
import os
import re
import sys
from typing import List, Tuple, Dict, Any

# =============================================================================
# Package hierarchy order (Low level -> High level)
# =============================================================================
PACKAGE_HIERARCHY: List[str] = [
    "builtin_interfaces",
    "std_msgs",
    "geometry_msgs",
    "sensor_msgs",
    "nav_msgs",
]


def get_package_sort_key(pkg_upper: str) -> Tuple[int, int | str]:
    """Return key for sorting packages by hierarchy order (uppercase input)"""
    pkg_lower = pkg_upper.lower()
    if pkg_lower in PACKAGE_HIERARCHY:
        return (0, PACKAGE_HIERARCHY.index(pkg_lower))
    return (1, pkg_lower)


def to_pascal_case(text: str) -> str:
    return ''.join(x.title() for x in text.split('_'))


def parse_config_sorted(config_path: str) -> List[Tuple[str, str]]:
    """Parse config file and return sorted by hierarchy order"""
    if not os.path.exists(config_path):
        print(f"[Error] Config file not found: {config_path}")
        sys.exit(1)

    with open(config_path, 'r') as f:
        content = f.read()

    pattern = re.compile(r'^\s*#define\s+USE_(\w+)_MSG_(\w+)', re.MULTILINE)
    matches = pattern.finditer(content)

    # Grouping by package
    pkg_msgs: Dict[str, List[str]] = {}
    for m in matches:
        pkg_upper = m.group(1)
        msg_upper = m.group(2)
        if pkg_upper not in pkg_msgs:
            pkg_msgs[pkg_upper] = []
        pkg_msgs[pkg_upper].append(msg_upper)

    # Sort packages by hierarchy order, sort messages alphabetically inside packages
    sorted_matches: List[Tuple[str, str]] = []
    for pkg_upper in sorted(pkg_msgs.keys(), key=get_package_sort_key):
        for msg_upper in sorted(pkg_msgs[pkg_upper]):
            sorted_matches.append((pkg_upper, msg_upper))

    return sorted_matches


def get_existing_enums(output_path: str) -> set:
    if not os.path.exists(output_path):
        return set()
    with open(output_path, 'r') as f:
        content = f.read()
    match = re.search(r'enum class MessageType \{(.*?)\};', content, re.DOTALL)
    if not match:
        return set()
    enums = set(x.strip() for x in match.group(1).split(',') if x.strip() and 'NONE' not in x)
    return enums

def generate_registry(config_path: str, output_path: str) -> None:
    print(f"[DdsTypeRegistry] Reading config: {config_path}")

    matches = parse_config_sorted(config_path)
    
    existing_enums = get_existing_enums(output_path)
    current_enums = set(f"{pkg_upper}_MSG_{msg_upper}" for pkg_upper, msg_upper in matches)
    
    added = current_enums - existing_enums
    removed = existing_enums - current_enums
    unchanged = current_enums & existing_enums
    
    print(f"[DdsTypeRegistry] Configuration Analysis:")
    if added:
        print(f"  [+] Added ({len(added)}): {', '.join(sorted(added))}")
    if removed:
        print(f"  [-] Removed ({len(removed)}): {', '.join(sorted(removed))}")
    print(f"  [=] Maintained ({len(unchanged)}): {len(unchanged)} messages referenced.")
    
    includes: List[str] = []
    enum_entries: List[str] = ["NONE = 0"]
    reg_logic: List[str] = []
    name_logic: List[str] = []
    create_logic: List[str] = []

    for pkg_upper, msg_upper in matches:
        pkg_lower = pkg_upper.lower()
        msg_pascal = to_pascal_case(msg_upper)
        enum_name = f"{pkg_upper}_MSG_{msg_upper}"

        enum_entries.append(enum_name)

        # Include (Filename has 's')
        includes.append(f'#include "{pkg_lower}/msg/{msg_pascal}PubSubTypes.h"')

        # Class Name (Class name does not have 's')
        cls_name = f"{pkg_lower}::msg::{msg_pascal}PubSubType"

        # Register Logic
        reg_code = f"""
    try {{
        eprosima::fastdds::dds::TypeSupport type(new {cls_name}());
        type.register_type(participant);
        log->debug("  - Registered: {pkg_lower}/msg/{msg_pascal} ({{}})", type.get_type_name());
        count++;
    }} catch (const std::exception& e) {{
        log->error("  - Failed to register {pkg_lower}/{msg_pascal}: {{}}", e.what());
    }}"""
        reg_logic.append(reg_code)

        # Name Logic
        dds_type_name = f"{pkg_lower}::msg::dds_::{msg_pascal}_"
        name_logic.append(f'        case MessageType::{enum_name}: return "{dds_type_name}";')

        # Create Logic
        create_logic.append(f'        case MessageType::{enum_name}: return eprosima::fastdds::dds::TypeSupport(new {cls_name}());')

    # Create file
    out_dir = os.path.dirname(output_path)
    if not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(output_path, 'w') as f:
        f.write("#ifndef ROS2_MSG_TYPE_REGISTRY_HPP\n")
        f.write("#define ROS2_MSG_TYPE_REGISTRY_HPP\n\n")
        f.write("/**\n")
        f.write(" * @file ros2_msg_type_registry.hpp\n")
        f.write(" * @brief Auto-generated message type registry.\n")
        f.write(" * \n")
        f.write(" * DO NOT EDIT MANUALLY.\n")
        f.write(" * Run 'ros2/scripts/update_with_config.sh' to update.\n")
        f.write(" */\n\n")
        f.write("#include <spdlog/spdlog.h>\n\n")
        f.write("#include <fastdds/dds/domain/DomainParticipant.hpp>\n")
        f.write("#include <fastdds/dds/topic/TypeSupport.hpp>\n\n")
        f.write("// Generated Headers\n")
        for inc in includes:
            f.write(f"{inc}\n")

        f.write("\n// Auto-generated MessageType Enum\n")
        f.write("enum class MessageType {\n    ")
        f.write(",\n    ".join(enum_entries))
        f.write("\n};\n\n")

        f.write("class DdsTypeRegistry {\npublic:\n")

        f.write("    static int registerAllTypes(eprosima::fastdds::dds::DomainParticipant* participant, std::shared_ptr<spdlog::logger> log) {\n")
        f.write("        int count = 0;\n")
        f.write("\n".join(reg_logic))
        f.write("\n        return count;\n    }\n\n")

        f.write("    static std::string getTypeName(MessageType type) {\n")
        f.write("        switch(type) {\n")
        f.write("\n".join(name_logic))
        f.write('\n            default: return "";\n')
        f.write("        }\n    }\n\n")

        f.write("    static eprosima::fastdds::dds::TypeSupport createTypeSupport(MessageType type) {\n")
        f.write("        switch(type) {\n")
        f.write("\n".join(create_logic))
        f.write('\n            default: return eprosima::fastdds::dds::TypeSupport(nullptr);\n')
        f.write("        }\n    }\n\n")

        f.write("    static std::string enumToString(MessageType type) {\n")
        f.write("        switch(type) {\n")
        for enum_name in enum_entries:
            if "NONE" in enum_name:
                continue
            f.write(f'            case MessageType::{enum_name}: return "{enum_name}";\n')
        f.write('            default: return "UNKNOWN";\n')
        f.write("        }\n    }\n\n")

        f.write("    // Returns MessageType::NONE if the string does not match any enabled message.\n")
        f.write("    static MessageType stringToEnum(const std::string& name) {\n")
        for enum_name in enum_entries:
            if "NONE" in enum_name:
                continue
            f.write(f'        if (name == "{enum_name}") return MessageType::{enum_name};\n')
        f.write("        return MessageType::NONE;\n")
        f.write("    }\n")

        f.write("};\n\n")
        f.write("#endif // ROS2_MSG_TYPE_REGISTRY_HPP\n")

    print(f"[DdsTypeRegistry] Generated: {output_path}")


def generate_data_converter(config_path: str, output_path: str) -> None:
    """Generate ros2_msg_data_converter.hpp (declarations only) and a template .cpp file."""

    matches = parse_config_sorted(config_path)

    target_types: List[Dict[str, str]] = []
    for pkg_upper, msg_upper in matches:
        pkg_lower = pkg_upper.lower()
        msg_pascal = to_pascal_case(msg_upper)
        target_types.append({
            'enum_name': f"{pkg_upper}_MSG_{msg_upper}",
            'cls_name': f"{pkg_lower}::msg::{msg_pascal}",
            'func_suffix': f"{pkg_lower}_msg_{msg_pascal}",
            'var_name': msg_upper.lower() + '_msg',
        })

    # Generate Header (.hpp)
    out_dir = os.path.dirname(output_path)
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir)

    with open(output_path, 'w') as f:
        f.write("#ifndef ROS2_MSG_DATA_CONVERTER_HPP\n")
        f.write("#define ROS2_MSG_DATA_CONVERTER_HPP\n\n")
        f.write("/**\n")
        f.write(" * @file ros2_msg_data_converter.hpp\n")
        f.write(" * @brief Logic for converting between SOMEIP/Zenoh payload and ROS 2 msg.\n")
        f.write(" * \n")
        f.write(" * This header contains declarations only. Implement these functions in ros2_msg_data_converter.cpp\n")
        f.write(" * Run 'ros2/scripts/update_with_config.sh' to update this header.\n")
        f.write(" */\n\n")

        f.write("#include <cstring>\n")
        f.write("#include <string>\n")
        f.write("#include <vector>\n\n")
        f.write('#include "ros2_msg_list.h"\n')
        f.write('#include "ros2_msg_type_registry.hpp"\n\n')

        f.write("/**\n")
        f.write(" * @brief payloadToMsg: SOMEIP/Zenoh received payload -> Populate ROS2 msg fields (DDS_PUB path)\n")
        f.write(" * @brief msgToPayload: ROS2 msg fields -> Convert to SOMEIP/Zenoh send payload (DDS_SUB path)\n")
        f.write(" * \n")
        f.write(" * payload = Raw byte data sent via SOMEIP/Zenoh protocol\n")
        f.write(" *           This is the raw payload created by the sender, not an internal gateway format.\n")
        f.write(" */\n\n")

        # (B) Write declarations
        f.write("// --- User Implemented Conversion Functions ---\n\n")
        for t in target_types:
            suf = t['func_suffix']
            f.write(f"bool payloadToMsg_{suf}(const std::vector<uint8_t>& payload, void* msg);\n")
            f.write(f"std::vector<uint8_t> msgToPayload_{suf}(void* msg);\n\n")

        # (C) Dispatcher class
        f.write("/**\n")
        f.write(" * @class DataConverter\n")
        f.write(" * @brief Dispatcher class for mapping payload conversions.\n")
        f.write(" */\n\n")
        f.write("class DataConverter {\npublic:\n")
        f.write("    // DDS_PUB path: SOMEIP/Zenoh payload -> ROS2 msg fields\n")
        f.write("    // Returns false if conversion fails\n")
        f.write("    static bool payloadToMsg(MessageType type, const std::vector<uint8_t>& payload, void* msg) {\n")
        f.write("        switch (type) {\n")
        for t in target_types:
            f.write(f"            case MessageType::{t['enum_name']}: return payloadToMsg_{t['func_suffix']}(payload, msg);\n")
        f.write("            default: return false;\n")
        f.write("        }\n")
        f.write("    }\n\n")
        f.write("    // DDS_SUB path: ROS2 msg fields -> SOMEIP/Zenoh payload\n")
        f.write("    // Returns empty vector if conversion fails\n")
        f.write("    static std::vector<uint8_t> msgToPayload(MessageType type, void* msg) {\n")
        f.write("        switch (type) {\n")
        for t in target_types:
            f.write(f"            case MessageType::{t['enum_name']}: return msgToPayload_{t['func_suffix']}(msg);\n")
        f.write("            default: return {};\n")
        f.write("        }\n")
        f.write("    }\n")
        f.write("};\n\n")
        f.write("#endif // ROS2_MSG_DATA_CONVERTER_HPP\n")

    print(f"[DataConverter] Updated Header: {output_path}")

    # Generate Template (.cpp_template)
    template_path = output_path.replace(".hpp", ".cpp_template")
    with open(template_path, 'w') as f:
        f.write("/**\n")
        f.write(" * @file ros2_msg_data_converter.cpp_template\n")
        f.write(" * @brief Template for user-implemented message logic.\n")
        f.write(" * \n")
        f.write(" * Copy these functions into src/ros2_msg_data_converter.cpp (or a similarly named file)\n")
        f.write(" * to implement your data conversion logic. This file will be overwritten on generation.\n")
        f.write(" */\n\n")
        f.write('#include "ros2_msg_data_converter.hpp"\n\n')
        for t in target_types:
            suf = t['func_suffix']
            var = t['var_name']
            cls = t['cls_name']

            f.write(f"// --- {cls} ---\n\n")
            f.write(f"bool payloadToMsg_{suf}(const std::vector<uint8_t>& payload, void* msg) {{\n")
            f.write(f"    auto* {var} = static_cast<{cls}*>(msg);\n")
            f.write(f"    // [TODO: User Implementation]\n")
            f.write(f"    return true;\n")
            f.write(f"}}\n\n")

            f.write(f"std::vector<uint8_t> msgToPayload_{suf}(void* msg) {{\n")
            f.write(f"    auto* {var} = static_cast<{cls}*>(msg);\n")
            f.write(f"    std::vector<uint8_t> payload;\n")
            f.write(f"    // [TODO: User Implementation]\n")
            f.write(f"    return payload;\n")
            f.write(f"}}\n\n")
            
    print(f"[DataConverter] Generated Template: {template_path}")

    print(f"[DataConverter] Updated: {output_path}")


if __name__ == "__main__":
    generate_registry(sys.argv[1], sys.argv[2])
    generate_data_converter(sys.argv[1], sys.argv[3])
