# Data Pipeline - Batch Processing Architecture

## Overview

This data pipeline has been successfully migrated from Kafka streaming to a batch processing architecture that reads from raw S3 data and processes it through **Bronze, Silver, and Gold** tiers with **automated scheduling**.

## Architecture

### Current Implementation
- **Raw Tier**: JSON files uploaded to `s3://inde-aws-datalake/raw/iot-data/`
- **Bronze Tier**: Processes raw JSON data, adds metadata, validates format → `s3://inde-aws-datalake/bronze/iot-data/`
- **Silver Tier**: Applies data quality rules, enriches data, filters invalid records → `s3://inde-aws-datalake/silver/iot-data/`
- **Gold Tier**: Creates analytics-ready aggregations and metrics → `s3://inde-aws-datalake/gold/`

### Data Flow
```
Raw S3 JSON Files → Bronze Job → Bronze S3 Parquet → Silver Job → Silver S3 Parquet → Gold Job → Gold S3 Analytics Tables
```

## How to Run the Pipeline

### Prerequisites
1. Docker and Docker Compose running
2. AWS credentials configured
3. Spark cluster running (`docker compose up -d`)

### Automated Scheduling (Recommended)

The pipeline includes an automated scheduler that runs the complete pipeline every 10 minutes:

```bash
# Run pipeline once manually
./data-pipeline/run_pipeline_scheduled.sh

# Install cron job for automatic execution every 10 minutes
./data-pipeline/run_pipeline_scheduled.sh --install-cron

# Check help for more options
./data-pipeline/run_pipeline_scheduled.sh --help
```

**Environment Variables Required:**
```bash
export AWS_ACCESS_KEY_ID="your_access_key"
export AWS_SECRET_ACCESS_KEY="your_secret_key"
export AWS_DEFAULT_REGION="eu-west-3"
```

### Manual Execution (Step by Step)

#### Step 1: Upload Raw Data
Upload JSON files to the raw tier:
```bash
aws s3 cp sample-data/ s3://inde-aws-datalake/raw/iot-data/ --recursive
```

#### Step 2: Build and Deploy JAR
```bash
# Build the JAR
cd data-pipeline/spark && sbt assembly

# Copy JAR to Spark container
cd ../..
docker compose cp data-pipeline/spark/target/scala-2.12/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar spark-master:/tmp/
```

#### Step 3: Run Bronze Job
Process raw data into Bronze tier:
```bash
docker compose exec spark-master bash -c "
export AWS_ACCESS_KEY_ID=<your_key>
export AWS_SECRET_ACCESS_KEY=<your_secret>
export AWS_DEFAULT_REGION=eu-west-3
spark-submit \
  --class processing.BronzeJob \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.376 \
  --master local[2] \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /tmp/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar \
  2025-07-03"
```

#### Step 4: Run Silver Job
Process Bronze data into Silver tier:
```bash
docker compose exec spark-master bash -c "
export AWS_ACCESS_KEY_ID=<your_key>
export AWS_SECRET_ACCESS_KEY=<your_secret>
export AWS_DEFAULT_REGION=eu-west-3
spark-submit \
  --class processing.SilverJob \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.376 \
  --master local[2] \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /tmp/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar \
  2025-07-03"
```

#### Step 5: Run Gold Job
Create analytics tables from Silver data:
```bash
docker compose exec spark-master bash -c "
export AWS_ACCESS_KEY_ID=<your_key>
export AWS_SECRET_ACCESS_KEY=<your_secret>
export AWS_DEFAULT_REGION=eu-west-3
spark-submit \
  --class processing.GoldJob \
  --packages org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.376 \
  --master local[2] \
  --driver-memory 1g \
  --executor-memory 1g \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  /tmp/data-pipeline-scala-assembly-0.1.0-SNAPSHOT.jar \
  2025-07-03"
```

## Data Quality Results

### Latest Processing Results:
- **Bronze Tier**: 650 records successfully ingested and partitioned
- **Silver Tier**: 650 valid records (100% quality score) with enrichment and validation
- **Gold Tier**: 6 analytics tables created for business intelligence

## Data Structure

### Bronze Tier Schema:
- Original sensor data (deviceId, deviceType, temperature, humidity, pressure, etc.)
- Bronze metadata (bronze_ingestion_time, bronze_date, data_source, etc.)
- Processing status tracking

### Silver Tier Schema:
- Standardized sensor data with data quality validations
- Enriched fields (temperature categories, comfort indices, device health status)
- Business logic applied (temperature conversions, categorical classifications)
- Partitioned by: year, month, day, deviceType

### Gold Tier Analytics Tables:
1. **Hourly Device Metrics**: Aggregated sensor readings by hour, device, and location
2. **Daily Location Summary**: Business intelligence summaries by location and season
3. **Device Performance Analytics**: Device health, availability, and stability metrics
4. **Environmental Trends**: Time-series analytics for environmental conditions
5. **Data Quality Dashboard**: Monitoring and alerting metrics for data quality
6. **Real-time KPIs**: Executive dashboard metrics and system health indicators

## Key Features

1. **Complete Data Pipeline**: Bronze → Silver → Gold tier processing
2. **Automated Scheduling**: Configurable cron-based execution (every 10 minutes by default)
3. **Data Quality Validation**: Comprehensive validation rules for sensor data
4. **Data Enrichment**: Business logic adds value to raw sensor readings
5. **Analytics Ready**: Gold tier provides business intelligence tables
6. **Partitioning**: Efficient data organization for analytics queries
7. **Metadata Tracking**: Full lineage from raw data to analytics tables
8. **Error Handling**: Robust error handling and logging with detailed logs
9. **Scalability**: Spark-based processing for large datasets
10. **Monitoring**: Automated pipeline status monitoring and notifications

## Automated Scheduling

### Cron Job Setup
The pipeline can be automated to run every 10 minutes:

```bash
# Install the cron job
./data-pipeline/run_pipeline_scheduled.sh --install-cron

# Check installed cron jobs
crontab -l

# Remove cron job (edit crontab manually)
crontab -e
```

### Scheduling Features
- **Automatic JAR building** and deployment
- **Prerequisites checking** (Docker, Spark, dependencies)
- **Error handling** with pipeline failure notifications
- **Log management** with automatic cleanup (keeps 7 days)
- **Data availability monitoring** across all tiers
- **Configurable scheduling** (modify cron expression as needed)

### Log Files
- Pipeline logs: `logs/pipeline_YYYYMMDD_HHMMSS.log`
- Cron logs: `logs/pipeline_cron.log`
- Log retention: 7 days (configurable)

## Migration Notes

### What Changed:
- ❌ Removed: Kafka streaming infrastructure
- ❌ Removed: Real-time processing
- ✅ Added: Batch processing from S3 raw data
- ✅ Added: Gold tier analytics tables
- ✅ Added: Automated scheduling system
- ✅ Added: Enhanced data quality framework
- ✅ Added: Better partitioning strategy
- ✅ Added: Comprehensive metadata tracking
- ✅ Added: Business intelligence ready datasets

### Benefits of Batch Processing:
- Simpler infrastructure (no Kafka needed)
- Better data quality control
- More predictable processing
- Easier troubleshooting and reprocessing
- Cost-effective for this use case
- Analytics-ready data for BI tools
- Automated pipeline management

## Troubleshooting

### Common Issues:
1. **Path not found**: Ensure data exists in the expected S3 partitions
2. **AWS credentials**: Verify AWS access keys are correct and have S3 permissions
3. **Schema mismatches**: Check that Bronze, Silver, and Gold jobs are compatible
4. **Memory issues**: Adjust Spark memory settings if processing large datasets
5. **Docker issues**: Ensure Docker daemon is running and containers are healthy
6. **Cron issues**: Check cron service status and log files for scheduling problems

### Verification Commands:
```bash
# Check Bronze data
aws s3 ls s3://inde-aws-datalake/bronze/iot-data/ --recursive

# Check Silver data
aws s3 ls s3://inde-aws-datalake/silver/iot-data/ --recursive

# Check Gold analytics tables
aws s3 ls s3://inde-aws-datalake/gold/ --recursive

# Check pipeline logs
tail -f logs/pipeline_cron.log

# Test pipeline manually
./data-pipeline/run_pipeline_scheduled.sh

# Download sample for inspection
aws s3 cp s3://inde-aws-datalake/gold/hourly_device_metrics/year=2025/month=7/day=3/part-00000-*.parquet /tmp/sample_analytics.parquet
```

## Analytics and BI Integration

The Gold tier provides analytics-ready datasets that can be easily integrated with:
- **Grafana** for real-time dashboards
- **Tableau** for advanced visualizations
- **Power BI** for business intelligence reports
- **Jupyter Notebooks** for data science analysis
- **Custom applications** via direct S3/Parquet access

### Sample Analytics Queries
The Gold tier enables queries like:
- Device performance trends over time
- Location-based environmental analysis
- Data quality monitoring and alerting
- Comfort index tracking and optimization
- Device health and maintenance scheduling 