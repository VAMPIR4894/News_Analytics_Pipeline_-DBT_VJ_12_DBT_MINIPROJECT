from kafka import KafkaConsumer, KafkaProducer
import json
from extractor import parse
import mysql.connector
from db_config import DB_CONFIG
from datetime import datetime
from textblob import TextBlob

# Kafka configuration
TOPICS = ['bbc_news', 'guardian_news', 'reuters_news']
KAFKA_SERVER = 'localhost:9092'
SCRAPED_TOPIC = 'scraped_articles'

# MySQL connection
def get_mysql_connection():
    return mysql.connector.connect(**DB_CONFIG)

# Kafka Consumer setup
consumer = KafkaConsumer(
    *TOPICS,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset='earliest',
    group_id='news-consumers',
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

# Kafka Producer to send scraped articles
producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def save_raw_article(news_data):
    connection = get_mysql_connection()
    cursor = connection.cursor()
    
    try:
        insert_query = """
        INSERT INTO raw_articles (source, title, url)
        VALUES (%s, %s, %s)
        """
        cursor.execute(insert_query, (news_data['source'], news_data['title'], news_data['url']))
        connection.commit()
        return cursor.lastrowid
    finally:
        cursor.close()
        connection.close()

def save_processed_article(raw_id, title, content, source):
    connection = get_mysql_connection()
    cursor = connection.cursor()
    
    try:
        # Calculate sentiment
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
        
        # Calculate word count
        word_count = len(content.split())
        
        # Get sentiment
        sentiment = get_sentiment(content)
        
        insert_query = """
        INSERT INTO processed_articles 
        (raw_article_id, source, title, content, sentiment, word_count)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (raw_id, source, title, content, sentiment, word_count))
        connection.commit()
    finally:
        cursor.close()
        connection.close()

print("Kafka consumer started. Listening to topics...")

for message in consumer:
    topic = message.topic
    news = message.value
    print(f"[{topic.upper()}] Article Title: {news['title']}")
    
    # Save raw article first
    raw_id = save_raw_article(news)
    
    # Scrape and process the article
    title, article_content = parse(news['url'])
    
    if title and article_content:
        print(f"Scraped Content (Preview): {article_content[:200]}...\n")
        
        # Save processed article
        save_processed_article(raw_id, title, article_content, news['source'])
        
        # Send to Kafka for Spark processing
        scraped_data = {
            "raw_id": raw_id,
            "source": news['source'],
            "title": title,
            "content": article_content
        }
        producer.send(SCRAPED_TOPIC, scraped_data)
    else:
        print(f"[ERROR] Failed to scrape article: {news['url']}")

