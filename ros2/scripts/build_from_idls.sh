#!/bin/bash

source ~/ws/setup.sh

# ==========================================
# 1. Environment Setup & Path Definitions
# ==========================================
# Current script directory (ros2/scripts)
CURRENT_SCRIPT_DIR=$(dirname $(realpath "$0"))

# BASE_DIR (ros2 folder)
BASE_DIR=$(realpath "$CURRENT_SCRIPT_DIR/..")

INPUT_IDL_DIR=$BASE_DIR/idls            # Original IDL sources
FILTERED_IDL_DIR=$CURRENT_SCRIPT_DIR/build_temp/idls_filtered # Preprocessing directory
TEMP_DUMP_DIR=$CURRENT_SCRIPT_DIR/build_temp/dump # FastDDSGen raw output directory
OUTPUT_MSGS_DIR=$BASE_DIR/msgs          # Final output directory

# Unified naming for result files
ROS2_MSG_CONFIG_H="$OUTPUT_MSGS_DIR/ros2_msg_config.h"
ROS2_MSG_DEPS_MAP="$OUTPUT_MSGS_DIR/ros2_msg_dependencies.map"
ROS2_MSG_LIST_H="$OUTPUT_MSGS_DIR/ros2_msg_list.h"
BUILD_LOG="$CURRENT_SCRIPT_DIR/build_error.log"

# [Core Fix] Fast-DDS-Gen command setup
# Uses the wrapper script registered in system PATH instead of searching for the JAR path.
if ! command -v fastddsgen &> /dev/null; then
    echo "----------------------------------------------------------------------"
    echo "[ERROR] Command 'fastddsgen' not found."
    echo "Solution: Run the following command in your terminal first."
    echo "$ source ~/ws/setup.sh"
    echo "----------------------------------------------------------------------"
    exit 1
fi

FASTDDSGEN="fastddsgen"
# ==========================================
# Debug mode settings (1: Keep temp files, 0: Delete temp files)
KEEP_TEMP_FILES=0

# ==========================================
# 2. Initialization (Workspace Cleanup & Preserve Configs)
# ==========================================
echo "----------------------------------------------------------------------"
echo "[Build System] Step 0: Initialize Workspace"
echo "----------------------------------------------------------------------"

# Define paths for important files to preserve
FILE_CONFIG="$ROS2_MSG_CONFIG_H"
FILE_CONVERTER_HPP="$OUTPUT_MSGS_DIR/ros2_msg_data_converter.hpp"
FILE_CONVERTER_CPP="$OUTPUT_MSGS_DIR/ros2_msg_data_converter.cpp"
FILE_REGISTRY="$OUTPUT_MSGS_DIR/ros2_msg_type_registry.hpp" # Additional preserve if needed

# Temporary backup paths
BACKUP_CONFIG="/tmp/ros2_msg_config.h.bak"
BACKUP_CONVERTER_HPP="/tmp/ros2_msg_data_converter.hpp.bak"
BACKUP_CONVERTER_CPP="/tmp/ros2_msg_data_converter.cpp.bak"
BACKUP_REGISTRY="/tmp/ros2_msg_type_registry.hpp.bak"

# Backup important files if they exist (move to /tmp)
if [ -f "$FILE_CONFIG" ]; then cp "$FILE_CONFIG" "$BACKUP_CONFIG"; fi
if [ -f "$FILE_CONVERTER_HPP" ]; then cp "$FILE_CONVERTER_HPP" "$BACKUP_CONVERTER_HPP"; fi
if [ -f "$FILE_CONVERTER_CPP" ]; then cp "$FILE_CONVERTER_CPP" "$BACKUP_CONVERTER_CPP"; fi
if [ -f "$FILE_REGISTRY" ]; then cp "$FILE_REGISTRY" "$BACKUP_REGISTRY"; fi

# Initialize working directories (delete and recreate)
rm -rf "$FILTERED_IDL_DIR" "$OUTPUT_MSGS_DIR" "$TEMP_DUMP_DIR" "$BUILD_LOG"
mkdir -p "$FILTERED_IDL_DIR" "$OUTPUT_MSGS_DIR" "$TEMP_DUMP_DIR"
touch "$BUILD_LOG"

# Restore backup files (to original location)
if [ -f "$BACKUP_CONFIG" ]; then
    mv "$BACKUP_CONFIG" "$FILE_CONFIG"
    echo " [Restored] ros2_msg_config.h"
fi
if [ -f "$BACKUP_CONVERTER_HPP" ]; then
    mv "$BACKUP_CONVERTER_HPP" "$FILE_CONVERTER_HPP"
    echo " [Restored] ros2_msg_data_converter.hpp"
fi
if [ -f "$BACKUP_CONVERTER_CPP" ]; then
    mv "$BACKUP_CONVERTER_CPP" "$FILE_CONVERTER_CPP"
    echo " [Restored] ros2_msg_data_converter.cpp"
fi
if [ -f "$BACKUP_REGISTRY" ]; then
    mv "$BACKUP_REGISTRY" "$FILE_REGISTRY"
    echo " [Restored] ros2_msg_type_registry.hpp"
fi

cp -r "$INPUT_IDL_DIR"/* "$FILTERED_IDL_DIR"/
echo "Initial cleanup done. IDLs prepared for preprocessing."

# ==========================================
# 3. IDL Preprocessing (Cleaning & Guards & Collisions)
# ==========================================
echo -e "\n----------------------------------------------------------------------"
echo "[Build System] Step 1: IDL Preprocessing"
echo "----------------------------------------------------------------------"

python3 "$CURRENT_SCRIPT_DIR/python/clean_idl.py" "$FILTERED_IDL_DIR"
echo "IDL preprocessing completed."

# ==========================================
# 4. Dependency Analysis & Config Update
# ==========================================
echo -e "\n----------------------------------------------------------------------"
echo "[Build System] Step 2: Dependency Analysis & Config Update"
echo "----------------------------------------------------------------------"

python3 "$CURRENT_SCRIPT_DIR/python/update_msg_config.py" "$FILTERED_IDL_DIR" "$ROS2_MSG_CONFIG_H"
echo "[OK] Config updated."

# ==========================================
# Generate Dependency Map
# ==========================================

python3 "$CURRENT_SCRIPT_DIR/python/make_dependency_map.py" "$FILTERED_IDL_DIR" "$ROS2_MSG_DEPS_MAP" "$ROS2_MSG_LIST_H"
if [ $? -ne 0 ]; then
    exit 1
fi
echo "[OK] Dependency map and main header created."

# ==========================================
# 5. Fast-DDS Code Generation (FASTDDSGEN)
# ==========================================
echo -e "\n----------------------------------------------------------------------"
echo "[Build System] Step 3: Fast-DDS Code Generation"
echo "----------------------------------------------------------------------"

find "$FILTERED_IDL_DIR" -name "*.idl" | while read idl_file; do
    rel_path="${idl_file#$FILTERED_IDL_DIR/}"
    rel_dir=$(dirname "$rel_path")
    base_name=$(basename "$idl_file" .idl)
    final_dir="$OUTPUT_MSGS_DIR/$rel_dir"
    mkdir -p "$final_dir"

    ERR=$($FASTDDSGEN -cs -replace -typeobject -typeros2 -I "$FILTERED_IDL_DIR" -d "$TEMP_DUMP_DIR" "$idl_file" 2>&1)
    if [ $? -ne 0 ]; then
        echo -e "[ERROR] $rel_path\n$ERR\n---" >> "$BUILD_LOG"
        rm -rf "$TEMP_DUMP_DIR"/*; continue
    fi

    find "$TEMP_DUMP_DIR" -name "${base_name}*" -exec mv {} "$final_dir/" \;
    find "$final_dir" -name "*${base_name}.idl*" | while read f; do
        mv "$f" "${f//${base_name}.idl/${base_name}}"
    done
    rm -rf "$TEMP_DUMP_DIR"/*
    echo "[Generated] $rel_path"
done

# ==========================================
# 6. Final Cleanup (Temp folders and logs)
# ==========================================
echo -e "\n----------------------------------------------------------------------"
echo "[Build System] Step 4: Final Cleanup"
echo "----------------------------------------------------------------------"

# Color Definitions
GREEN='\033[0;32m'
CYAN='\033[0;36m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

if [ $KEEP_TEMP_FILES -eq 0 ]; then
    echo "Cleanup mode: Deleting temporary files..."

    # [If build succeeds without errors]
    if [ ! -s "$BUILD_LOG" ]; then
        rm -f "$BUILD_LOG"
        echo " - No errors found. Log file removed."

        # === Success Guide Output (Updated) ===
        echo -e "\n${GREEN}----------------------------------------------------------------------"
        echo -e "[Build System] ROS 2 Message System Build Success!"
        echo -e "----------------------------------------------------------------------${NC}"
        echo -e " [Next Steps]"
        echo -e " 1. Open Configuration File"
        echo -e "    ${CYAN}$ nano $ROS2_MSG_CONFIG_H${NC}"
        echo -e "\n 2. Uncomment Required Messages"
        echo -e "    Ex: // #define USE_SENSOR_MSGS...  =>  #define USE_SENSOR_MSGS..."
        echo -e "\n 3. Apply Configuration to Registry"
        echo -e "    ${CYAN}$ ${CURRENT_SCRIPT_DIR}/update_with_config.sh${NC}"
        echo -e "${GREEN}----------------------------------------------------------------------${NC}\n"
        # === Success Guide Output End ===

    else
        echo -e "\n${CYAN} - [Notice] Errors recorded in build_error.log.${NC}"
    fi

    rm -rf "$CURRENT_SCRIPT_DIR/build_temp"
    echo "Cleanup complete."
else
    echo "Debug mode: Keeping all temporary files."
    echo " - Temp files: $CURRENT_SCRIPT_DIR/build_temp"
    echo " - Build log : $BUILD_LOG"
fi
