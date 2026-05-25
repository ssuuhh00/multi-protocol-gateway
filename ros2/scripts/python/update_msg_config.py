#!/usr/bin/env python3
"""Generate/Update ros2_msg_config.h: Scan IDL directory to generate message macros

Usage: python3 update_msg_config.py <idl_directory> <config_file>

Used in generate_messages.sh

Sort packages by dependency hierarchy order:
  builtin_interfaces → std_msgs → geometry_msgs → sensor_msgs → nav_msgs → (Other alphabetical)
Alphabetical sorting within each package
"""
import os
import re
import sys
from typing import Dict, List, Tuple

# =============================================================================
# Package hierarchy order (Low level -> High level)
# =============================================================================
PACKAGE_HIERARCHY = [
    "builtin_interfaces",
    "std_msgs",
    "geometry_msgs",
    "sensor_msgs",
    "nav_msgs",
]


# =============================================================================
# Utility: CamelCase -> SNAKE_CASE conversion
# =============================================================================
def to_snake_case(name):
    # e.g.: CompressedImage -> COMPRESSED_IMAGE
    # e.g.: AccelStamped    -> ACCEL_STAMPED
    # e.g.: TF2Error        -> TF2_ERROR
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()


def get_package_sort_key(pkg_name):
    """Return key to sort packages by hierarchy order"""
    if pkg_name in PACKAGE_HIERARCHY:
        return (0, PACKAGE_HIERARCHY.index(pkg_name))
    return (1, pkg_name)  # Packages not in hierarchy are placed alphabetically at the end


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    root_dir = sys.argv[1]
    config_file = sys.argv[2]

    # ---------------------------------------------------------
    # 1. Scan IDL directory (based on current filesystem)
    # ---------------------------------------------------------
    # Structure: { 'sensor_msgs': ['USE_SENSOR_MSGS_MSG_IMAGE', ...], ... }
    detected_packages: Dict[str, List[str]] = {}

    for root, dirs, files in os.walk(root_dir):
        for f in files:
            if f.endswith(".idl"):
                # Path parsing (e.g.: idls/sensor_msgs/msg/CompressedImage.idl)
                rel_path = os.path.relpath(os.path.join(root, f), root_dir)
                parts = rel_path.split(os.sep)

                # parts[0]: Package name (sensor_msgs)
                # parts[-1]: File name (CompressedImage.idl)
                if len(parts) >= 2:
                    pkg_name = parts[0]
                    msg_raw_name = os.path.splitext(parts[-1])[0]

                    # Generate macro name
                    msg_snake = to_snake_case(msg_raw_name)
                    macro_name = f"USE_{pkg_name.upper()}_MSG_{msg_snake}"

                    if pkg_name not in detected_packages:
                        detected_packages[pkg_name] = []
                    detected_packages[pkg_name].append(macro_name)

    # ---------------------------------------------------------
    # 2. Analyze existing Config file state (Preserve user settings)
    # ---------------------------------------------------------
    # Structure: { 'USE_SENSOR_MSGS_MSG_IMAGE': True(Active) / False(Commented out), ... }
    existing_state: Dict[str, bool] = {}

    if os.path.exists(config_file):
        print(f"[Config] Updating existing file: {config_file}")
        with open(config_file, 'r', encoding='utf-8') as f:
            content = f.read()

        # Capture active/inactive state using regex
        # Pattern: (Comment status) #define (Macro name)
        pattern = re.compile(r'^\s*(//)?\s*#define\s+(USE_\w+)', re.MULTILINE)
        matches = pattern.finditer(content)

        for m in matches:
            comment_mark = m.group(1) or ""
            macro = m.group(2)
            is_disabled = (comment_mark.strip() == '//')
            existing_state[macro] = not is_disabled  # True(Active) if no comment
    else:
        print(f"[Config] Creating new file: {config_file}")

    # ---------------------------------------------------------
    # 3. Write file (Sorted by hierarchy order)
    # ---------------------------------------------------------
    with open(config_file, 'w', encoding='utf-8') as f:
        f.write("#ifndef ROS2_MSG_CONFIG_H_\n")
        f.write("#define ROS2_MSG_CONFIG_H_\n\n")
        f.write("/**\n")
        f.write(" * @file ros2_msg_config.h\n")
        f.write(" * @brief ROS 2 Message Build Configuration.\n")
        f.write(" *\n")
        f.write(" * Auto-generated & Updated by build script.\n")
        f.write(" * Uncomment lines to enable specific messages.\n")
        f.write(" */\n\n")

        # Sort packages by hierarchy order
        for pkg in sorted(detected_packages.keys(), key=get_package_sort_key):
            f.write(f"// [{pkg}]\n")

            # Sort message macros by alphabetical order
            for macro in sorted(detected_packages[pkg]):
                is_enabled = False

                # Follow existing state if any value is already set
                if macro in existing_state:
                    is_enabled = existing_state[macro]

                # Output
                if is_enabled:
                    f.write(f"#define {macro}\n")
                else:
                    f.write(f"// #define {macro}\n")

            f.write("\n")

        f.write("#endif  // ROS2_MSG_CONFIG_H_\n")
