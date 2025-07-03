#!/bin/bash

# ====================================================================
# DATA PIPELINE SCHEDULER SCRIPT
# ====================================================================
# Purpose: Run the complete data pipeline (Bronze -> Silver -> Gold)
# Schedule: Every 10 minutes via cron
# 
# To schedule this script to run every 10 minutes, add to crontab:
# */10 * * * * /path/to/data-pipeline/run_pipeline_scheduled.sh >> /var/log/data-pipeline.log 2>&1
# ====================================================================

# Configuration
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$PROJECT_ROOT/logs"
TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
DATE_PARAM=$(date '+%Y-%m-%d')

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Log file with timestamp
LOG_FILE="$LOG_DIR/pipeline_$(date '+%Y%m%d_%H%M%S').log"

# AWS Credentials (set these as environment variables or modify here)
# Note: In production, use IAM roles or AWS credentials file
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-AKIAWM6L4EDISI3XEOPL}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-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}"
export AWS_DEFAULT_REGION="${AWS_DEFAULT_REGION:-eu-west-3}"

# Pipeline Configuration
JAR_PATH="/tmp/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar"
SPARK_PACKAGES="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.376"
SPARK_CONFIG="--master local[2] --driver-memory 1g --executor-memory 1g --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem"

# ====================================================================
# UTILITY FUNCTIONS
# ====================================================================

log() {
    echo "[$TIMESTAMP] $1" | tee -a "$LOG_FILE"
}

check_prerequisites() {
    log "🔍 Checking prerequisites..."
    
    # Check if Docker is running
    if ! docker ps >/dev/null 2>&1; then
        log "❌ ERROR: Docker is not running. Please start Docker first."
        exit 1
    fi
    
    # Check if Spark container is running
    if ! docker compose ps spark-master | grep -q "running"; then
        log "⚠️  WARNING: Spark master container is not running. Attempting to start..."
        cd "$PROJECT_ROOT" && docker compose up -d spark-master
        sleep 10
    fi
    
    # Check if JAR file exists in container
    if ! docker compose exec spark-master test -f "$JAR_PATH"; then
        log "⚠️  WARNING: JAR file not found in container. Rebuilding and copying..."
        build_and_copy_jar
    fi
    
    log "✅ Prerequisites check completed"
}

build_and_copy_jar() {
    log "🔨 Building JAR file..."
    
    cd "$SCRIPT_DIR/spark"
    if sbt assembly; then
        log "✅ JAR build successful"
        
        # Copy JAR to Spark container
        cd "$PROJECT_ROOT"
        if docker compose cp data-pipeline/spark/target/scala-2.12/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar spark-master:/tmp/; then
            log "✅ JAR copied to Spark container"
        else
            log "❌ ERROR: Failed to copy JAR to container"
            exit 1
        fi
    else
        log "❌ ERROR: JAR build failed"
        exit 1
    fi
}

run_bronze_job() {
    log "🥉 Starting Bronze Job..."
    
    cd "$PROJECT_ROOT"
    docker compose exec spark-master bash -c "
        export AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'
        export AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'
        export AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'
        
        spark-submit \
          --class processing.BronzeJob \
          --packages $SPARK_PACKAGES \
          $SPARK_CONFIG \
          $JAR_PATH \
          $DATE_PARAM
    "
    
    if [ $? -eq 0 ]; then
        log "✅ Bronze Job completed successfully"
        return 0
    else
        log "❌ Bronze Job failed"
        return 1
    fi
}

run_silver_job() {
    log "🥈 Starting Silver Job..."
    
    cd "$PROJECT_ROOT"
    docker compose exec spark-master bash -c "
        export AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'
        export AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'
        export AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'
        
        spark-submit \
          --class processing.SilverJob \
          --packages $SPARK_PACKAGES \
          $SPARK_CONFIG \
          $JAR_PATH \
          $DATE_PARAM
    "
    
    if [ $? -eq 0 ]; then
        log "✅ Silver Job completed successfully"
        return 0
    else
        log "❌ Silver Job failed"
        return 1
    fi
}

run_gold_job() {
    log "🥇 Starting Gold Job..."
    
    cd "$PROJECT_ROOT"
    docker compose exec spark-master bash -c "
        export AWS_ACCESS_KEY_ID='$AWS_ACCESS_KEY_ID'
        export AWS_SECRET_ACCESS_KEY='$AWS_SECRET_ACCESS_KEY'
        export AWS_DEFAULT_REGION='$AWS_DEFAULT_REGION'
        
        spark-submit \
          --class processing.GoldJob \
          --packages $SPARK_PACKAGES \
          $SPARK_CONFIG \
          $JAR_PATH \
          $DATE_PARAM
    "
    
    if [ $? -eq 0 ]; then
        log "✅ Gold Job completed successfully"
        return 0
    else
        log "❌ Gold Job failed"
        return 1
    fi
}

check_data_availability() {
    log "📊 Checking data availability in S3..."
    
    # Check Bronze data
    BRONZE_FILES=$(aws s3 ls s3://inde-aws-datalake/bronze/iot-data/ --recursive | wc -l)
    log "📁 Bronze files available: $BRONZE_FILES"
    
    # Check Silver data
    SILVER_FILES=$(aws s3 ls s3://inde-aws-datalake/silver/iot-data/ --recursive | wc -l)
    log "📁 Silver files available: $SILVER_FILES"
    
    # Check Gold data
    GOLD_FILES=$(aws s3 ls s3://inde-aws-datalake/gold/ --recursive | wc -l)
    log "📁 Gold files available: $GOLD_FILES"
}

cleanup_old_logs() {
    log "🧹 Cleaning up old log files..."
    
    # Keep only last 7 days of logs
    find "$LOG_DIR" -name "pipeline_*.log" -type f -mtime +7 -delete
    
    log "✅ Log cleanup completed"
}

send_notification() {
    local status=$1
    local message=$2
    
    # Simple notification - can be extended to send emails, Slack messages, etc.
    log "📢 NOTIFICATION: $status - $message"
    
    # Example: Send to a webhook or monitoring system
    # curl -X POST "https://your-webhook-url" -d "{\"status\":\"$status\", \"message\":\"$message\", \"timestamp\":\"$TIMESTAMP\"}"
}

# ====================================================================
# MAIN PIPELINE EXECUTION
# ====================================================================

main() {
    log "🚀 Starting Data Pipeline Execution"
    log "📅 Processing date: $DATE_PARAM"
    log "📝 Log file: $LOG_FILE"
    
    # Initialize pipeline
    check_prerequisites
    
    # Track overall success
    PIPELINE_SUCCESS=true
    
    # Run Bronze Job
    if run_bronze_job; then
        log "🎯 Bronze tier processing completed"
    else
        log "💥 Bronze tier processing failed - stopping pipeline"
        PIPELINE_SUCCESS=false
        send_notification "ERROR" "Bronze job failed for date $DATE_PARAM"
        exit 1
    fi
    
    # Run Silver Job (only if Bronze succeeded)
    if run_silver_job; then
        log "🎯 Silver tier processing completed"
    else
        log "💥 Silver tier processing failed - stopping pipeline"
        PIPELINE_SUCCESS=false
        send_notification "ERROR" "Silver job failed for date $DATE_PARAM"
        exit 1
    fi
    
    # Run Gold Job (only if Silver succeeded)
    if run_gold_job; then
        log "🎯 Gold tier processing completed"
    else
        log "💥 Gold tier processing failed"
        PIPELINE_SUCCESS=false
        send_notification "ERROR" "Gold job failed for date $DATE_PARAM"
        exit 1
    fi
    
    # Check final data state
    check_data_availability
    
    # Cleanup
    cleanup_old_logs
    
    # Final status
    if [ "$PIPELINE_SUCCESS" = true ]; then
        log "🎉 PIPELINE COMPLETED SUCCESSFULLY!"
        log "📊 All tiers (Bronze, Silver, Gold) processed successfully for $DATE_PARAM"
        send_notification "SUCCESS" "Pipeline completed successfully for date $DATE_PARAM"
    else
        log "💥 PIPELINE FAILED!"
        send_notification "ERROR" "Pipeline failed for date $DATE_PARAM"
        exit 1
    fi
    
    log "⏰ Pipeline execution finished at $(date '+%Y-%m-%d %H:%M:%S')"
    log "📁 Detailed logs saved to: $LOG_FILE"
}

# ====================================================================
# CRON SCHEDULING HELPER
# ====================================================================

install_cron_job() {
    log "📅 Installing cron job for every 10 minutes..."
    
    # Create cron job entry
    CRON_JOB="*/10 * * * * $SCRIPT_DIR/run_pipeline_scheduled.sh >> $LOG_DIR/pipeline_cron.log 2>&1"
    
    # Add to crontab
    (crontab -l 2>/dev/null; echo "$CRON_JOB") | crontab -
    
    log "✅ Cron job installed. Pipeline will run every 10 minutes."
    log "📝 Cron logs will be saved to: $LOG_DIR/pipeline_cron.log"
    log "🔍 To check current cron jobs: crontab -l"
    log "🗑️  To remove cron job: crontab -e"
}

# ====================================================================
# SCRIPT EXECUTION
# ====================================================================

# Check command line arguments
if [ "$1" = "--install-cron" ]; then
    install_cron_job
    exit 0
elif [ "$1" = "--help" ] || [ "$1" = "-h" ]; then
    echo "Data Pipeline Scheduler"
    echo ""
    echo "Usage:"
    echo "  $0                    Run the pipeline once"
    echo "  $0 --install-cron     Install cron job to run every 10 minutes"
    echo "  $0 --help             Show this help message"
    echo ""
    echo "Environment Variables (required):"
    echo "  AWS_ACCESS_KEY_ID      Your AWS access key"
    echo "  AWS_SECRET_ACCESS_KEY  Your AWS secret key" 
    echo "  AWS_DEFAULT_REGION     AWS region (default: eu-west-3)"
    echo ""
    echo "Examples:"
    echo "  # Run once manually"
    echo "  ./run_pipeline_scheduled.sh"
    echo ""
    echo "  # Install cron job for automatic scheduling"
    echo "  ./run_pipeline_scheduled.sh --install-cron"
    echo ""
    echo "  # Run with specific AWS credentials"
    echo "  AWS_ACCESS_KEY_ID=your_key AWS_SECRET_ACCESS_KEY=your_secret ./run_pipeline_scheduled.sh"
    exit 0
else
    # Run the main pipeline
    main
fi 