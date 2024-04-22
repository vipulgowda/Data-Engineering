#!/bin/bash

# Define the directory containing the Python script
SCRIPT_DIR="/home/vipulp/dataeng-project/"

# Change directory to the script directory
cd "$SCRIPT_DIR" || exit

# Activate the virtual environment
source env/bin/activate

pip install -r requirements.txt
# Define the Python script path
PYTHON_SCRIPT="gather_data.py"

PUBLISHER_SCRIPT="./pub-sub/publisher.py"

# Define the log file path
LOG_FILE="log_file.log"

# Run the Python script and append the output to the log file
python3 "$PYTHON_SCRIPT" >> "$LOG_FILE" 2>&1 && python3 "$PUBLISHER_SCRIPT" >> "$LOG_FILE" 2>&1