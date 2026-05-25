#!/usr/bin/env python3
"""Generate dependency map (ros2_msg_dependencies.map) and include list (ros2_msg_list.h)

Usage: python3 make_dependency_map.py <idl_directory> <dep_file> <list_h>

Used in generate_messages.sh
"""
import os
import re
import sys
from typing import Dict, List, Set, Tuple, Optional

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


def get_idl_sort_key(idl_path):
    """Return key for sorting IDL paths by package hierarchy order"""
    parts = idl_path.split('/')
    pkg = parts[0] if parts else ""
    if pkg in PACKAGE_HIERARCHY:
        return (0, PACKAGE_HIERARCHY.index(pkg), idl_path)
    return (1, 0, idl_path)


def get_deps(root_dir: str, rel_path: str) -> Set[str]:
    p = os.path.join(root_dir, rel_path)
    if not os.path.exists(p):
        return set()
    with open(p, 'r', encoding='utf-8') as f:
        return set(re.findall(r'#include "(.+?)\.idl"', f.read()))


def get_all_deps(root_dir: str, rel_path: str, memo: Optional[Dict[str, Set[str]]] = None, stack: Optional[List[str]] = None) -> Set[str]:
    if memo is None:
        memo = {}
    if stack is None:
        stack = []

    base = rel_path.replace(".idl", "")

    # Check for circular dependencies
    if base in stack:
        print(f"\n[FATAL] Circular Dependency: {' -> '.join(stack + [base])}")
        sys.exit(1)

    if base in memo:
        return memo[base]

    stack.append(base)
    direct_deps = get_deps(root_dir, rel_path)
    combined_deps = set(direct_deps)

    for d in direct_deps:
        # Must pass stack during recursive call
        combined_deps.update(get_all_deps(root_dir, d + ".idl", memo, stack))

    stack.pop()
    memo[base] = combined_deps
    return combined_deps


def to_macro_case(text):
    # CamelCase to SNAKE_CASE conversion (e.g. CompressedImage -> COMPRESSED_IMAGE)
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', text)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).upper()


if __name__ == "__main__":
    root_dir, dep_file, list_h = sys.argv[1], sys.argv[2], sys.argv[3]

    # Collect all IDLs
    all_idls = []
    for r, _, fs in os.walk(root_dir):
        for f in fs:
            if f.endswith(".idl"):
                all_idls.append(os.path.relpath(os.path.join(r, f), root_dir))

    with open(dep_file, 'w') as fd, open(list_h, 'w') as fl:
        fl.write("/**\n")
        fl.write(" * @file ros2_msg_list.h\n")
        fl.write(" * @brief Auto-generated ROS 2 message include list.\n")
        fl.write(" */\n\n")
        fl.write("#ifndef ROS2_MSG_LIST_H_\n#define ROS2_MSG_LIST_H_\n\n")
        fl.write("#include \"ros2_msg_config.h\"\n\n")

        cache = {}
        for idl in sorted(all_idls, key=get_idl_sort_key):
            base = idl.replace(".idl", "")
            try:
                all_reqs = get_all_deps(root_dir, idl, cache, [])
                fd.write(f"{base}: {' '.join(sorted(list(all_reqs | {base})))}\n")

                p = base.split('/')
                # Apply CamelCase -> SNAKE_CASE conversion when generating macro name
                # p[0]: Package name(sensor_msgs), p[1]: folder(msg), p[-1]: Message name(CompressedImage)
                if len(p) >= 3:
                    msg_upper = to_macro_case(p[-1])
                    m = f"USE_{p[0].upper()}_{p[1].upper()}_{msg_upper}"
                else:
                    m = f"USE_{base.upper().replace('/', '_')}"

                fl.write(f"#ifdef {m}\n")
                fl.write(f"  #include \"{base}.h\"\n")
                fl.write(f"  #include \"{base}TypeObject.h\"\n")
                fl.write(f"  #include \"{base}PubSubTypes.h\"\n")
                fl.write(f"#endif\n\n")
            except SystemExit:
                sys.exit(1)

        fl.write("#endif // ROS2_MSG_LIST_H_\n")
