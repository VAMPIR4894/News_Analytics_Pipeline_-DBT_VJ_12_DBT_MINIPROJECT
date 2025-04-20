from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, explode, split, length, 
                                 count, sum as _sum, udf)
from pyspark.sql.types import StringType
from textblob import TextBlob
import json
import time
from datetime import datetime
from db_config import DB_CONFIG
import mysql.connector

print("Starting Batch Processing Job...")

# Create SparkSession for batch processing with optimized configurations
spark = SparkSession.builder \
    .appName("NewsAnalyticsBatchProcessor") \
    .config("spark.driver.host", "localhost") \
    .config("spark.driver.bindAddress", "127.0.0.1") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "2g") \
    .config("spark.network.timeout", "600s") \
    .config("spark.executor.heartbeatInterval", "60s") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.default.parallelism", "4") \
    .config("spark.memory.fraction", "0.8") \
    .config("spark.memory.storageFraction", "0.5") \
    .config("spark.local.dir", "/tmp/spark-temp") \
    .getOrCreate()

# Set log level to ERROR to reduce noise
spark.sparkContext.setLogLevel("ERROR")

def get_sentiment(text):
    if not text:
        return "neutral"
    analysis = TextBlob(str(text))
    polarity = analysis.sentiment.polarity
    if polarity > 0:
        return "positive"
    elif polarity < 0:
        return "negative"
    else:
        return "neutral"

def print_sentiment_analysis(sentiment_counts):
    """Print sentiment analysis results"""
    total = sum(row["count"] for row in sentiment_counts)
    for row in sentiment_counts:
        percentage = (row["count"] / total) * 100 if total > 0 else 0
        print(f"{row['sentiment']}: {row['count']} ({percentage:.1f}%)")

def print_word_frequency(word_counts):
    """Print word frequency results"""
    for row in word_counts[:5]:  # Only show top 5 words
        print(f"{row['word']}: {row['count']}")

def print_source_statistics(source_stats):
    """Print source statistics"""
    for row in source_stats:
        avg_length = row["total_length"] / row["article_count"] if row["article_count"] > 0 else 0
        print(f"{row['source']}: {row['article_count']} articles, avg length: {avg_length:.0f} chars")

def read_from_mysql():
    """Read all processed articles from MySQL"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    query = "SELECT source, title, content FROM processed_articles"
    cursor.execute(query)
    
    # Fetch all records
    records = cursor.fetchall()
    
    cursor.close()
    conn.close()
    
    # Convert to Spark DataFrame
    return spark.createDataFrame(records, ["source", "title", "content"])

def save_batch_analytics(analytics_data):
    """Save batch analytics results to MySQL"""
    conn = mysql.connector.connect(**DB_CONFIG)
    cursor = conn.cursor()
    
    current_time = datetime.now()
    
    insert_query = """
    INSERT INTO analytics_results 
    (window_start, window_end, metric_name, metric_value, processing_type) 
    VALUES (%s, %s, %s, %s, %s)
    """
    
    cursor.executemany(insert_query, [
        (current_time, current_time, name, json.dumps(value), "batch")
        for name, value in analytics_data
    ])
    
    conn.commit()
    cursor.close()
    conn.close()

def save_timing_metrics(start_time, end_time, num_records, processing_type):
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Create timing_metrics table if it doesn't exist
        create_table_query = """
        CREATE TABLE IF NOT EXISTS timing_metrics (
            id INT AUTO_INCREMENT PRIMARY KEY,
            processing_type VARCHAR(20),
            start_time TIMESTAMP,
            end_time TIMESTAMP,
            processing_duration_seconds FLOAT,
            records_processed INT,
            records_per_second FLOAT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
        """
        cursor.execute(create_table_query)
        
        # Insert timing metrics
        insert_query = """
        INSERT INTO timing_metrics 
        (processing_type, start_time, end_time, processing_duration_seconds, 
         records_processed, records_per_second)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        
        duration = end_time - start_time
        records_per_second = num_records / duration if duration > 0 else 0
        
        cursor.execute(insert_query, (
            processing_type,
            datetime.fromtimestamp(start_time),
            datetime.fromtimestamp(end_time),
            duration,
            num_records,
            records_per_second
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error saving timing metrics: {str(e)}")

def process_batch():
    try:
        overall_start_time = time.time()
        
        # Read data timing
        read_start_time = time.time()
        df = read_from_mysql()
        total_records = df.count()
        read_end_time = time.time()
        read_duration = read_end_time - read_start_time
        
        # Processing timing
        process_start_time = time.time()
        
        # Add sentiment analysis
        sentiment_udf = udf(get_sentiment, StringType())
        df_with_sentiment = df.withColumn("sentiment", sentiment_udf(col("content")))
        
        # Calculate analytics
        word_counts = df_with_sentiment \
            .withColumn("word", explode(split(col("content"), "\\W+"))) \
            .filter(col("word") != "") \
            .groupBy("word") \
            .count() \
            .orderBy(col("count").desc()) \
            .limit(5) \
            .collect()
        
        sentiment_counts = df_with_sentiment \
            .groupBy("sentiment") \
            .count() \
            .collect()
        
        source_stats = df_with_sentiment \
            .groupBy("source") \
            .agg(
                count("*").alias("article_count"),
                _sum(length("content")).alias("total_length")
            ) \
            .collect()
        
        # Print analysis results
        print("\nAnalysis Results:")
        print("================")
        print(f"Total Articles Processed: {total_records}")
        
        # Print sentiment analysis
        print_sentiment_analysis(sentiment_counts)
        
        # Print word frequency
        print_word_frequency(word_counts)
        
        # Print source statistics
        print_source_statistics(source_stats)
        
        process_end_time = time.time()
        process_duration = process_end_time - process_start_time
        
        # Save results timing
        save_start_time = time.time()
        
        analytics_data = [
            ("word_counts_batch", word_counts),
            ("sentiment_distribution_batch", sentiment_counts),
            ("source_statistics_batch", source_stats)
        ]
        save_batch_analytics(analytics_data)
        
        save_end_time = time.time()
        save_duration = save_end_time - save_start_time
        
        # Overall timing
        overall_end_time = time.time()
        overall_duration = overall_end_time - overall_start_time
        
        # Save timing metrics
        save_timing_metrics(overall_start_time, overall_end_time, total_records, "batch")
        
        # Print detailed timing report
        print("\nBatch Processing Performance Report")
        print("=" * 50)
        print(f"Total records processed: {total_records}")
        print(f"Read time: {read_duration:.2f} seconds")
        print(f"Processing time: {process_duration:.2f} seconds")
        print(f"Save time: {save_duration:.2f} seconds")
        print(f"Total time: {overall_duration:.2f} seconds")
        print(f"Overall throughput: {total_records / overall_duration:.2f} records/second")
        
    except Exception as e:
        print(f"Error in batch processing: {str(e)}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()

def compare_performance():
    """Compare batch and streaming performance metrics"""
    try:
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        query = """
        SELECT 
            processing_type,
            AVG(processing_duration_seconds) as avg_duration,
            AVG(records_per_second) as avg_throughput,
            MAX(records_per_second) as max_throughput,
            MIN(records_per_second) as min_throughput,
            COUNT(*) as num_executions
        FROM timing_metrics
        GROUP BY processing_type
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        print("\nPerformance Comparison (Batch vs Streaming)")
        print("=" * 50)
        print("Type\t\tAvg Duration\tAvg Throughput\tMax Throughput\tMin Throughput\tExecutions")
        print("-" * 100)
        
        for row in results:
            proc_type, avg_dur, avg_thru, max_thru, min_thru, num_exec = row
            print(f"{proc_type}\t\t{avg_dur:.2f}s\t\t{avg_thru:.2f} r/s\t{max_thru:.2f} r/s\t{min_thru:.2f} r/s\t{num_exec}")
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error comparing performance: {str(e)}")

if __name__ == "__main__":
    process_batch() 