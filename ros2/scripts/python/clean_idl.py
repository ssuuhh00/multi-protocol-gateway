#!/usr/bin/env python3
"""IDL Preprocessing: Remove comments/annotations, inject include guard, resolve typedef collisions

Usage: python3 clean_idl.py <idl_directory>

Used in generate_messages.sh
"""
import os
import re
import sys
from collections import defaultdict
from typing import DefaultDict, List


# =============================================================================
# (1) Remove comments and unnecessary items
# =============================================================================
def clean_file_by_indent(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    new_lines, skip_mode, trigger_indent = [], False, 0
    for line in lines:
        if not line.strip():
            if not skip_mode:
                new_lines.append(line)
            continue
        curr_indent = len(line) - len(line.lstrip())
        content = line.strip()
        if skip_mode:
            if curr_indent > trigger_indent:
                continue
            else:
                skip_mode = False
        if content.startswith("@verbatim") or content.startswith("@default"):
            skip_mode, trigger_indent = True, curr_indent
            continue
        if content.startswith("@unit") or content.startswith("@key"):
            continue
        if not skip_mode and "@verbatim" not in line:
            new_lines.append(line)
    with open(filepath, 'w', encoding='utf-8') as f:
        f.writelines(new_lines)


# =============================================================================
# (2) Inject IDL Guard
# =============================================================================
def inject_guard(filepath, root_dir):
    rel_path = os.path.relpath(filepath, root_dir)
    guard = rel_path.upper().replace("/", "_").replace(".", "_") + "_GENERATED"
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
    if guard in content:
        return
    with open(filepath, 'w', encoding='utf-8') as f:
        f.write(f"#ifndef {guard}\n#define {guard}\n\n{content}\n\n#endif")


# =============================================================================
# (3) Resolve Typedef collisions
# =============================================================================
def fix_collisions(root_dir):
    pattern = re.compile(r'typedef\s+.*?\s+(\w+)\s*\[\s*\d+\s*\]')
    registry: DefaultDict[str, List[str]] = defaultdict(list)
    for root, _, files in os.walk(root_dir):
        for f in files:
            if f.endswith(".idl"):
                path = os.path.join(root, f)
                with open(path, 'r', errors='ignore') as file:
                    for name in set(pattern.findall(file.read())):
                        registry[name].append(path)
    for name, paths in registry.items():
        if len(paths) > 1:
            for p in paths:
                new_name = f"{name}_{os.path.splitext(os.path.basename(p))[0].lower()}"
                with open(p, 'r') as f:
                    c = f.read()
                with open(p, 'w') as f:
                    f.write(re.sub(r'\b' + name + r'\b', new_name, c))


# =============================================================================
# Main
# =============================================================================
if __name__ == "__main__":
    idl_dir = sys.argv[1]

    # (1) Remove comments and unnecessary items
    for root, _, files in os.walk(idl_dir):
        for f in files:
            if f.endswith(".idl"):
                clean_file_by_indent(os.path.join(root, f))

    # (2) Inject IDL Guard
    for root, _, files in os.walk(idl_dir):
        for f in files:
            if f.endswith(".idl"):
                inject_guard(os.path.join(root, f), idl_dir)

    # (3) Resolve Typedef collisions
    fix_collisions(idl_dir)
