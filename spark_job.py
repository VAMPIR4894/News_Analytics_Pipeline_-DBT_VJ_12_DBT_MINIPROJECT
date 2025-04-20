from pyspark.sql import SparkSession
from pyspark.sql.functions import (col, from_json, explode, split, length, count as spark_count, 
                                 sum as _sum, udf, window, current_timestamp, lit, lower, trim, when, avg, regexp_replace, collect_list, arrays_zip)
from pyspark.sql.types import StructType, StringType, IntegerType, TimestampType, StructField
from textblob import TextBlob
import time
from datetime import datetime, timedelta
import json
import mysql.connector
from mysql.connector import Error
from db_config import DB_CONFIG
import os

# Define stop words
stop_words = {'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'is', 'are', 'was', 'were'}

def get_sentiment(text):
    """Analyze sentiment of text using TextBlob"""
    if not text:
        return "neutral"
    try:
        analysis = TextBlob(text)
        polarity = analysis.sentiment.polarity
        if polarity > 0.1:
            return "positive"
        elif polarity < -0.1:
            return "negative"
        else:
            return "neutral"
    except Exception as e:
        print(f"Error in sentiment analysis: {str(e)}")
        return "neutral"

def print_sentiment_analysis(sentiment_counts):
    """Print sentiment analysis results"""
    print("\nSentiment Analysis:")
    total = sum(row["count"] for row in sentiment_counts)
    for row in sentiment_counts:
        percentage = (row["count"] / total) * 100 if total > 0 else 0
        print(f"{row['sentiment']}: {row['count']} ({percentage:.1f}%)")

def print_word_frequency(word_counts):
    """Print word frequency results"""
    print("\nTop Words:")
    for row in word_counts:
        print(f"{row['word']}: {row['count']}")

def print_source_statistics(source_stats):
    """Print source statistics"""
    print("\nSource Statistics:")
    for row in source_stats:
        avg_length = row["total_length"] / row["article_count"] if row["article_count"] > 0 else 0
        print(f"{row['source']}: {row['article_count']} articles, avg length: {avg_length:.0f} chars")

def save_analytics_results(analytics_data, window_start, window_end):
    """Save analytics results to database"""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        for metric_name, metric_value in analytics_data:
            cursor.execute("""
                INSERT INTO analytics_results 
                (window_start, window_end, metric_name, metric_value, processing_type)
                VALUES (%s, %s, %s, %s, 'streaming')
            """, (window_start, window_end, metric_name, json.dumps(metric_value)))
        
        connection.commit()
        print("Analytics results saved successfully")
        
    except Error as e:
        print(f"Error saving analytics results: {str(e)}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def save_processed_articles(df):
    """Save processed articles to database"""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        for row in df.collect():
            cursor.execute("""
                INSERT INTO processed_articles 
                (source, title, content, sentiment, word_count)
                VALUES (%s, %s, %s, %s, %s)
            """, (row["source"], row["title"], row["content"], row["sentiment"], len(row["content"].split())))
        
        connection.commit()
        print("Processed articles saved successfully")
        
    except Error as e:
        print(f"Error saving processed articles: {str(e)}")
        if connection:
            connection.rollback()
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

def clean_word(text):
    """Clean and normalize words"""
    if not text:
        return ""
    # Remove special characters, convert to lowercase, and trim
    cleaned = text.strip().lower()
    # Remove common stop words (expand this list as needed)
    if cleaned in stop_words:
        return ""
    return cleaned

def process_window_batch(df, epoch_id):
    if df.isEmpty():
        print(f"Empty batch at epoch {epoch_id}")
        return
    
    try:
        # Initialize window counter if not exists
        if not hasattr(process_window_batch, 'window_counter'):
            process_window_batch.window_counter = 0
        
        # Check if we've reached the limit of 3 windows
        if process_window_batch.window_counter >= 3:
            return
        
        process_window_batch.window_counter += 1
        print(f"\nProcessing window {process_window_batch.window_counter} of 3 (15 minutes each)")
        
        # Calculate window boundaries based on initial time
        window_start = process_window_batch.initial_time + timedelta(minutes=(process_window_batch.window_counter - 1) * 15)
        window_end = window_start + timedelta(minutes=15)
        
        print(f"\nWindow {process_window_batch.window_counter} Analysis:")
        print(f"Time Range: {window_start} to {window_end}")
        print(f"Articles in this window: {df.count()}")
        
        # Word count analysis with better filtering
        word_counts = df \
            .withColumn("word", explode(split(lower(col("content")), "\\W+"))) \
            .filter(length(col("word")) > 3) \
            .filter(col("word").rlike("^[a-zA-Z]+$")) \
            .groupBy("word") \
            .count() \
            .orderBy(col("count").desc()) \
            .limit(10) \
            .collect()
        
        print("\nTop 10 Words in Articles:")
        print("------------------------")
        for row in word_counts:
            print(f"{row['word']}: {row['count']} occurrences")
        
        # Sentiment analysis
        sentiment_counts = df \
            .groupBy("sentiment") \
            .count() \
            .collect()
        
        print("\nSentiment Distribution:")
        print("----------------------")
        total_articles = sum(row['count'] for row in sentiment_counts)
        for row in sentiment_counts:
            percentage = (row['count'] / total_articles) * 100 if total_articles > 0 else 0
            print(f"{row['sentiment']}: {row['count']} articles ({percentage:.1f}%)")
        
        # Source statistics
        source_stats = df \
            .groupBy("source") \
            .agg(
                spark_count("*").alias("article_count"),
                _sum(length("content")).alias("total_length")
            ) \
            .collect()
        
        print("\nSource Statistics:")
        print("-----------------")
        for row in source_stats:
            avg_length = row["total_length"] / row["article_count"] if row["article_count"] > 0 else 0
            print(f"{row['source']}: {row['article_count']} articles, average length: {avg_length:.0f} characters")
        
        # Save to MySQL
        import mysql.connector
        
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        # Insert analytics results
        insert_query = """
        INSERT INTO analytics_results 
        (window_start, window_end, metric_name, metric_value, processing_type) 
        VALUES (%s, %s, %s, %s, %s)
        """
        
        analytics_data = [
            (window_start, window_end, "word_counts", 
             json.dumps([{"word": row["word"], "count": row["count"]} 
                       for row in word_counts])),
            (window_start, window_end, "sentiment_distribution", 
             json.dumps([{"sentiment": row["sentiment"], "count": row["count"]} 
                       for row in sentiment_counts])),
            (window_start, window_end, "source_statistics", 
             json.dumps([{"source": row["source"], 
                        "count": row["article_count"],
                        "total_length": row["total_length"]} 
                       for row in source_stats]))
        ]
        
        cursor.executemany(insert_query, [(start, end, name, value, "streaming") 
                                        for start, end, name, value in analytics_data])
        
        conn.commit()
        print(f"\nSuccessfully saved analytics for window {process_window_batch.window_counter}")
        
        cursor.close()
        conn.close()
        
        # If this is the last window, set a flag to stop gracefully
        if process_window_batch.window_counter >= 3:
            process_window_batch.should_stop = True
        
    except Exception as e:
        print(f"Error processing batch {epoch_id}: {str(e)}")
        import traceback
        traceback.print_exc()

def main():
    try:
        print("Starting Spark Job...")
        
        # Initialize the start time for the entire job
        start_time = datetime.now()
        # Round to the next minute to start fresh
        start_time = start_time.replace(second=0, microsecond=0) + timedelta(minutes=1)
        end_time = start_time + timedelta(minutes=45)
        
        # Set the initial time as a static attribute of process_window_batch
        process_window_batch.initial_time = start_time
        
        print(f"Will process data from {start_time.strftime('%H:%M')} to {end_time.strftime('%H:%M')} (3 windows of 15 minutes each)")

        # Create SparkSession with specific package version
        spark = SparkSession.builder \
            .appName("NewsAnalyticsProcessor") \
            .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoint") \
            .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1") \
            .config("spark.driver.host", "localhost") \
            .config("spark.driver.bindAddress", "127.0.0.1") \
            .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        print("Spark Session created successfully")

        # Schema for incoming Kafka messages
        schema = StructType() \
            .add("raw_id", IntegerType()) \
            .add("source", StringType()) \
            .add("title", StringType()) \
            .add("content", StringType())

        print("Starting to read from Kafka...")
        
        # Read stream from Kafka
        df_raw = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("subscribe", "scraped_articles") \
            .option("startingOffsets", "latest") \
            .load()
        
        # Parse JSON data
        df_parsed = df_raw \
            .selectExpr("CAST(value AS STRING) as json_str", "timestamp") \
            .select(from_json(col("json_str"), schema).alias("data"), "timestamp") \
            .select("data.*", "timestamp")
        
        # Add sentiment analysis
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
        
        sentiment_udf = udf(get_sentiment, StringType())
        df_with_sentiment = df_parsed.withColumn("sentiment", sentiment_udf(col("content")))
        
        # Filter for messages only after our start time
        df_with_sentiment = df_with_sentiment.filter(col("timestamp") >= start_time)
        
        print("Starting streaming query...")
        
        # Start the streaming query with 15-minute trigger
        query = df_with_sentiment \
            .writeStream \
            .foreachBatch(process_window_batch) \
            .trigger(processingTime="15 minutes") \
            .start()
        
        print("Streaming query started. Will process 3 windows of 15 minutes each...")
        
        # Wait for the streaming to finish (after 3 windows)
        while True:
            if hasattr(process_window_batch, 'should_stop') and process_window_batch.should_stop:
                print("\nCompleted processing 3 windows. Stopping streaming query...")
                query.stop()
                break
            time.sleep(1)  # Check every second
        
        print("\nFinal Analytics Summary:")
        print("=======================")
        
        # Query MySQL for final statistics
        conn = mysql.connector.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT metric_name, metric_value, window_start, window_end 
            FROM analytics_results 
            WHERE processing_type = 'streaming'
            ORDER BY window_start DESC 
            LIMIT 3
        """)
        
        results = cursor.fetchall()
        if results:
            # Aggregate statistics across all windows
            all_sentiments = {}
            all_words = {}
            all_sources = {}
            
            for metric_name, metric_value, window_start, window_end in results:
                data = json.loads(metric_value)
                
                if metric_name == "sentiment_distribution":
                    for item in data:
                        sentiment = item['sentiment']
                        count = item['count']
                        all_sentiments[sentiment] = all_sentiments.get(sentiment, 0) + count
                
                elif metric_name == "word_counts":
                    for item in data:
                        word = item['word']
                        count = item['count']
                        all_words[word] = all_words.get(word, 0) + count
                
                elif metric_name == "source_statistics":
                    for item in data:
                        source = item['source']
                        count = item['count']
                        length = item['total_length']
                        if source not in all_sources:
                            all_sources[source] = {'count': 0, 'total_length': 0}
                        all_sources[source]['count'] += count
                        all_sources[source]['total_length'] += length
            
            print("\nOverall Statistics for 45-minute Period:")
            print("--------------------------------------")
            
            # Print sentiment distribution
            print("\nTotal Sentiment Distribution:")
            print("---------------------------")
            total_articles = sum(all_sentiments.values())
            for sentiment, count in all_sentiments.items():
                percentage = (count / total_articles * 100) if total_articles > 0 else 0
                print(f"{sentiment}: {count} articles ({percentage:.1f}%)")
            
            # Print top 10 words
            print("\nTop 10 Most Common Words Overall:")
            print("------------------------------")
            sorted_words = sorted(all_words.items(), key=lambda x: x[1], reverse=True)[:10]
            for word, count in sorted_words:
                print(f"{word}: {count} occurrences")
            
            # Print source statistics
            print("\nSource Statistics Overall:")
            print("------------------------")
            for source, stats in all_sources.items():
                avg_length = stats['total_length'] / stats['count'] if stats['count'] > 0 else 0
                print(f"{source}: {stats['count']} articles, average length: {avg_length:.0f} characters")
        
        cursor.close()
        conn.close()
        
        print("\nStreaming job completed successfully")
        
    except Exception as e:
        print(f"Error in main process: {str(e)}")
        import traceback
        traceback.print_exc()
        if 'spark' in locals():
            spark.stop()

if __name__ == "__main__":
    main()

