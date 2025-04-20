import requests
import json
import time
from kafka import KafkaProducer
import feedparser
from config import (GUARDIAN_API_KEY, BBC_RSS_URL, REUTERS_RSS_URL, 
                   GUARDIAN_API_URL_BASE, KAFKA_SERVER, KAFKA_TOPICS)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_bbc_news():
    feed = feedparser.parse(BBC_RSS_URL)
    if feed.entries:
        for entry in feed.entries:
            if '/videos' not in entry.link:
                news_data = {"source": "BBC", "title": entry.title, "url": entry.link}
                producer.send(KAFKA_TOPICS['BBC'], news_data)
                print(f"[BBC] Sent: {news_data['url']}")

def fetch_guardian_news(page=1):
    api_url = f"{GUARDIAN_API_URL_BASE}?api-key={GUARDIAN_API_KEY}&section=world&page={page}&page-size=10"
    response = requests.get(api_url)
    data = response.json()

    if "response" in data and "results" in data["response"]:
        for article in data["response"]["results"]:
            news_data = {"source": "Guardian", "title": article["webTitle"], "url": article["webUrl"]}
            producer.send(KAFKA_TOPICS['GUARDIAN'], news_data)
            print(f"[Guardian - Page {page}] Sent: {news_data['url']}")

def fetch_reuters_news():
    feed = feedparser.parse(REUTERS_RSS_URL)
    if feed.entries:
        for entry in feed.entries:
            news_data = {"source": "Reuters", "title": entry.title, "url": entry.link}
            producer.send(KAFKA_TOPICS['REUTERS'], news_data)
            print(f"[Reuters] Sent: {news_data['url']}")

if __name__ == "__main__":
    page = 1
    while True:
        print(f"\n--- Fetching news (Guardian page {page}) ---")
        fetch_bbc_news()
        fetch_guardian_news(page)
        fetch_reuters_news()

        # Loop page 1 to 5 then restart
        page = page + 1 if page < 5 else 1

        time.sleep(30)

