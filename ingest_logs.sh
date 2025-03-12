#!/bin/bash

# Ensure a date argument is provided
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 YYYY-MM-DD"
    exit 1
fi

# Extract year, month, and day from the input date
DATE=$1
YEAR=$(date -d "$DATE" '+%Y')
MONTH=$(date -d "$DATE" '+%m')
DAY=$(date -d "$DATE" '+%d')

# Define local and HDFS paths
LOCAL_LOG_DIR="./data/logs/$DATE"
LOCAL_METADATA_FILE="./data/content_metadata.csv"

HDFS_BASE_DIR="/raw"
HDFS_LOG_DIR="$HDFS_BASE_DIR/logs/$YEAR/$MONTH/$DAY"
HDFS_METADATA_DIR="$HDFS_BASE_DIR/metadata/$YEAR/$MONTH/$DAY"

# Create necessary directories in HDFS
hdfs dfs -mkdir -p $HDFS_LOG_DIR
hdfs dfs -mkdir -p $HDFS_METADATA_DIR

# Copy logs to HDFS
if hdfs dfs -test -e "$LOCAL_LOG_DIR"; then
    hdfs dfs -put "$LOCAL_LOG_DIR"/*.csv "$HDFS_LOG_DIR/"
    echo "User logs for $DATE ingested successfully."
else
    echo "Error: Log directory $LOCAL_LOG_DIR does not exist."
fi

# Copy metadata to HDFS (only once, not per date)
if hdfs dfs -test -e "$LOCAL_METADATA_FILE"; then
    hdfs dfs -put "$LOCAL_METADATA_FILE" "$HDFS_METADATA_DIR/"
    echo "Metadata ingested successfully."
else
    echo "Error: Metadata file not found."
fi

echo "Data ingestion completed for $DATE."
