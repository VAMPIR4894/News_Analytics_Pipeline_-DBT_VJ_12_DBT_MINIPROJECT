# 📰 News Analytics Pipeline: Streaming & Batch Processing

## 1. Project Overview
This project implements a **real-time news analytics system** that ingests, processes, and analyzes articles from major sources like **BBC, Reuters, and The Guardian**. It uses both **streaming and batch processing** to compare live vs historical analytics using Apache Spark and Kafka.

### Key Features
- Real-time article ingestion from multiple news sources
- Sentiment analysis using TextBlob
- Source-based analytics and statistics
- Historical data processing and comparison
- Real-time dashboard and visualization
- Scalable architecture using Apache Spark and Kafka

## 2. System Architecture
```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  News APIs  │────▶│  Producer   │────▶│   Kafka     │
└─────────────┘     └─────────────┘     └─────────────┘
                                                  │
                                                  ▼
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  MySQL DB   │◀────│  Consumer   │◀────│  Spark      │
└─────────────┘     └─────────────┘     └─────────────┘
```

## 3. Implementation Details
- **Data Ingestion**: Articles are fetched live using APIs and fed into Kafka topics by a producer.
- **Streaming Processing**: Apache Spark Structured Streaming processes data in 1-minute tumbling windows to perform real-time analysis.
- **Batch Processing**: A separate Spark job processes historical data in bulk for comprehensive insights.
- **Storage**: Results and raw articles are stored in a **MySQL** database.
- **Analysis**: TextBlob is used for sentiment analysis, and word frequency & source-based statistics are computed.

## 4. Project Structure
```
News_Analytics_Pipeline_-DBT_VJ_12_DBT_MINIPROJECT/
├── producer.py                 # Fetches articles and sends to Kafka
├── consumer.py                 # Consumes Kafka messages and writes to MySQL
├── spark_job.py               # Performs real-time streaming analytics
├── batch_processor.py         # Performs historical batch analytics
├── extractor.py               # Extracts and cleans article content
├── db_config.py               # Sets up and configures MySQL database
├── config.py                 # Template for API keys and database settings
├── requirements.txt           # Python dependencies
├── .gitignore                 # Git ignore rules
└── README.md                  # Project documentation
```

## 5. Component Details
| File                         | Purpose                                                   |
|------------------------------|-----------------------------------------------------------|
| `producer.py`                | Fetches articles and sends them to Kafka topics           |
| `consumer.py`                | Consumes Kafka messages and writes to MySQL               |
| `spark_job.py`               | Performs **real-time streaming** analytics                |
| `batch_processor.py`         | Performs **historical batch** analytics                   |
| `extractor.py`               | Extracts and cleans article content                       |
| `db_config.py`               | Sets up and configures the MySQL database                 |
| `config.py`         | Template for API keys and database settings               |

## 6. Installation and Setup

### Prerequisites
- Python 3.8+
- Java 21 (OpenJDK)
- Apache Kafka + Zookeeper
- Apache Spark (with Kafka support)
- MySQL Server 8.0+
- Internet access (for live article fetching)
- TextBlob (installed via `pip install -r requirements.txt`)

### Installation Steps
```bash
# Clone the repository
git clone <repo-url>
cd News_Analytics_Pipeline_-DBT_VJ_12_DBT_MINIPROJECT

# Create and activate virtual environment
python -m venv venv
source venv/bin/activate       # On Windows: venv\Scripts\activate

# Install dependencies
pip install -r requirements.txt

# Configure the project
cp config.template.py config.py
# Edit config.py with your settings:
# - Update MySQL credentials in DB_CONFIG
# - Add your Guardian API key
# - Adjust Kafka settings if needed
python db_config.py
```

## 7. Running the System

### Start Application Components
### Start Required Services
```bash
# Terminal 1 - Start Zookeeper
/opt/homebrew/bin/zookeeper-server-start /opt/homebrew/etc/kafka/zookeeper.properties


# Terminal 2 - Start Kafka
/opt/homebrew/bin/kafka-server-start /opt/homebrew/etc/kafka/server.properties

```

### Start Application Components
```bash
# Terminal 3 - Start Producer
python producer.py

# Terminal 4 - Start Consumer
python consumer.py

# Terminal 5 - Start Streaming Job
python spark_job.py

# Terminal 6 - Start Batch Processing
python batch_processor.py
```

## 8. Troubleshooting

### Common Issues
1. **Kafka Connection Issues**
   - Ensure Zookeeper is running
   - Check Kafka server status
   - Verify network connectivity

2. **Database Connection Problems**
   - Verify MySQL credentials in config.py
   - Check MySQL server status
   - Ensure database and tables are created

3. **API Rate Limiting**
   - Check API key validity
   - Monitor rate limits
   - Implement backoff strategy

### Logs and Monitoring
- Check logs in `/var/log/kafka/` for Kafka issues
- Monitor Spark UI at `http://localhost:4040`
- Check MySQL logs for database issues
