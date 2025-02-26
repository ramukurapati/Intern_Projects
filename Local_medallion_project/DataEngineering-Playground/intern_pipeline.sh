#!/bin/bash

LOG_DIR="C:/Users/ramuk/Intern_Project/DataEngineering-Playground/logs"
TODAY=$(date +"%Y%m%d")
LOG_FILE="$LOG_DIR/intern_pipeline.log"

mkdir -p "$LOG_DIR"  # Ensure log directory exists

echo "ETL Job Started at $(date)" >> "$LOG_FILE"

run_job() {
    local job_name=$1
    local script_path=$2
    local job_log="$LOG_DIR/${job_name}_${TODAY}.log"

    echo "Running $job_name..." | tee -a "$LOG_FILE" "$job_log"

    python "$script_path" >> "$job_log" 2>&1
    if [ $? -ne 0 ]; then
        echo "❌ ERROR: $job_name failed! Check logs: $job_log" | tee -a "$LOG_FILE"
        exit 1
    fi

    echo "✅ SUCCESS: $job_name completed." | tee -a "$LOG_FILE"
}

# Run ETL jobs in sequence
run_job "raw_layer" "C:/Users/ramuk/Intern_Project/DataEngineering-Playground/generate_data_with_config.py"
run_job "bronze_layer" "C:/Users/ramuk/Intern_Project/DataEngineering-Playground/bronze_layer.py"
run_job "silver_layer" "C:/Users/ramuk/Intern_Project/DataEngineering-Playground/silver_layer.py"
run_job "gold_layer" "C:/Users/ramuk/Intern_Project/DataEngineering-Playground/gold_layer.py"

echo "ETL Job Completed at $(date)" >> "$LOG_FILE"
