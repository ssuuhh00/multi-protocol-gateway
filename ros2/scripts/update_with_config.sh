#!/bin/bash

source ~/ws/setup.sh

# ==========================================
# 1. Path Setup
# ==========================================
# Directory containing the current script (ros2/scripts)
SCRIPT_DIR=$(dirname "$(realpath "$0")")

# Project root directory (two levels up from ros2/scripts)
PROJECT_ROOT=$(realpath "$SCRIPT_DIR/../..")

# Input: Configuration file
CONFIG_FILE="$PROJECT_ROOT/ros2/msgs/ros2_msg_config.h"

# Output: Registry header file (Generated in ros2/msgs folder)
OUTPUT_FILE="$PROJECT_ROOT/ros2/msgs/ros2_msg_type_registry.hpp"

# Output: Data converter header file (Generated in ros2/msgs folder)
CONVERTER_FILE="$PROJECT_ROOT/ros2/msgs/ros2_msg_data_converter.hpp"

# Color Definitions
GREEN='\033[0;32m'
CYAN='\033[0;36m'
NC='\033[0m'

# ==========================================
# 2. Execution and Cleanup
# ==========================================
echo -e "${CYAN}----------------------------------------------------------------------"
echo -e "[Gen Registry] Generating ros2_msg_type_registry.hpp & ros2_msg_data_converter.hpp..."
echo -e "----------------------------------------------------------------------${NC}"
python3 "$SCRIPT_DIR/python/make_type_registry.py" "$CONFIG_FILE" "$OUTPUT_FILE" "$CONVERTER_FILE"
EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
    echo -e "\n${GREEN}----------------------------------------------------------------------"
    echo -e " SUCCESS: ros2_msg_type_registry.hpp & ros2_msg_data_converter.hpp updated!"
    echo -e "----------------------------------------------------------------------${NC}"
    echo -e " [Next Steps]"
    echo -e " 1. Implement Data Converter"
    echo -e "    ${CYAN}$ nano $PROJECT_ROOT/ros2/msgs/ros2_msg_data_converter.cpp${NC}"
    echo -e "\n    (Note: Check template at ros2_msg_data_converter.cpp_template)"
    echo -e "\n 2. Build the Workspace"
    echo -e "    ${CYAN}$ source ~/ws/setup.sh && \\"
    echo -e "      cmake -B ~/ws/build/gateway -S ~/ws/src/multi-protocol-gateway && \\"
    echo -e "      cmake --build ~/ws/build/gateway${NC}"
    echo -e "${GREEN}----------------------------------------------------------------------${NC}\n"
else
    echo -e " ${CYAN}FAILED: Python script error.${NC}"
    exit 1
fi
