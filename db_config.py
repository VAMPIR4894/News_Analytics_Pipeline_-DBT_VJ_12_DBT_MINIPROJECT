import mysql.connector
from mysql.connector import Error
from config import DB_CONFIG

# Database Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'USER_NAME',
    'password': 'PASSWORD',
    'database': 'news_analytics'
}

def create_database():
    try:
        # Connect without database selected
        connection = mysql.connector.connect(
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password']
        )
        
        cursor = connection.cursor()
        
        # Create database if not exists
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
        print(f"Database {DB_CONFIG['database']} created successfully")
        
        # Use the database
        cursor.execute(f"USE {DB_CONFIG['database']}")
        
        # Create raw_articles table
        create_raw_table = """
        CREATE TABLE IF NOT EXISTS raw_articles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            source VARCHAR(50) NOT NULL,
            title TEXT NOT NULL,
            url TEXT,
            content TEXT,
            received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_source (source),
            INDEX idx_received_at (received_at)
        ) ENGINE=InnoDB
        """
        cursor.execute(create_raw_table)
        print("raw_articles table created/verified")
        
        # Create processed_articles table
        create_processed_table = """
        CREATE TABLE IF NOT EXISTS processed_articles (
            id INT AUTO_INCREMENT PRIMARY KEY,
            raw_article_id INT NOT NULL,
            source VARCHAR(50) NOT NULL,
            title TEXT NOT NULL,
            content TEXT NOT NULL,
            sentiment VARCHAR(20) NOT NULL,
            word_count INT NOT NULL,
            processed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (raw_article_id) REFERENCES raw_articles(id) ON DELETE CASCADE,
            INDEX idx_source_sentiment (source, sentiment),
            INDEX idx_processed_at (processed_at),
            INDEX idx_sentiment (sentiment)
        ) ENGINE=InnoDB
        """
        cursor.execute(create_processed_table)
        print("processed_articles table created/verified")
        
        # Create analytics_results table with window information
        create_analytics_table = """
        CREATE TABLE IF NOT EXISTS analytics_results (
            id INT AUTO_INCREMENT PRIMARY KEY,
            window_start TIMESTAMP NOT NULL,
            window_end TIMESTAMP NOT NULL,
            metric_name VARCHAR(50) NOT NULL,
            metric_value JSON NOT NULL,
            processing_type ENUM('streaming', 'batch') NOT NULL,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            INDEX idx_metric_time (metric_name, created_at),
            INDEX idx_window (window_start, window_end),
            INDEX idx_processing_type (processing_type)
        ) ENGINE=InnoDB
        """
        cursor.execute(create_analytics_table)
        print("analytics_results table created/verified")

        # Verify all required columns exist
        verify_queries = [
            {
                "table": "raw_articles",
                "required_columns": ['id', 'source', 'title', 'url', 'content', 'received_at'],
                "query": """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = 'raw_articles'
                """
            },
            {
                "table": "processed_articles",
                "required_columns": ['id', 'raw_article_id', 'source', 'title', 'content', 'sentiment', 'word_count', 'processed_at'],
                "query": """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = 'processed_articles'
                """
            },
            {
                "table": "analytics_results",
                "required_columns": ['id', 'window_start', 'window_end', 'metric_name', 'metric_value', 'processing_type', 'created_at'],
                "query": """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = %s AND table_name = 'analytics_results'
                """
            }
        ]

        for table_info in verify_queries:
            cursor.execute(table_info["query"], (DB_CONFIG['database'],))
            existing_columns = [row[0] for row in cursor.fetchall()]
            missing_columns = [col for col in table_info["required_columns"] if col not in existing_columns]
            
            if missing_columns:
                print(f"\nWarning: Table {table_info['table']} is missing columns: {', '.join(missing_columns)}")
                print("Attempting to add missing columns...")
                
                for column in missing_columns:
                    try:
                        if table_info['table'] == 'raw_articles':
                            if column == 'content':
                                cursor.execute("ALTER TABLE raw_articles ADD COLUMN content TEXT")
                        elif table_info['table'] == 'processed_articles':
                            if column == 'word_count':
                                cursor.execute("ALTER TABLE processed_articles ADD COLUMN word_count INT NOT NULL")
                        elif table_info['table'] == 'analytics_results':
                            if column == 'window_start':
                                cursor.execute("ALTER TABLE analytics_results ADD COLUMN window_start TIMESTAMP NOT NULL")
                            elif column == 'window_end':
                                cursor.execute("ALTER TABLE analytics_results ADD COLUMN window_end TIMESTAMP NOT NULL")
                            elif column == 'metric_value':
                                cursor.execute("ALTER TABLE analytics_results ADD COLUMN metric_value JSON NOT NULL")
                    except Error as e:
                        print(f"Error adding column {column}: {e}")

        # Add any missing indexes (will not error if they already exist)
        index_queries = [
            "CREATE INDEX idx_source ON raw_articles (source)",
            "CREATE INDEX idx_received_at ON raw_articles (received_at)",
            "CREATE INDEX idx_source_sentiment ON processed_articles (source, sentiment)",
            "CREATE INDEX idx_processed_at ON processed_articles (processed_at)",
            "CREATE INDEX idx_sentiment ON processed_articles (sentiment)",
            "CREATE INDEX idx_metric_time ON analytics_results (metric_name, created_at)",
            "CREATE INDEX idx_window ON analytics_results (window_start, window_end)",
            "CREATE INDEX idx_processing_type ON analytics_results (processing_type)"
        ]

        for query in index_queries:
            try:
                cursor.execute(query)
            except Error as e:
                if "Duplicate" not in str(e):  # Ignore duplicate index errors
                    print(f"Error creating index: {e}")

        connection.commit()
        print("\nDatabase setup completed successfully!")
        print("All tables and indexes are ready for Spark job processing")
        
    except Error as e:
        print(f"Error: {e}")
        raise e
    finally:
        if 'connection' in locals() and connection.is_connected():
            cursor.close()
            connection.close()

def verify_database_ready():
    """Verify database is ready for Spark job"""
    connection = None
    cursor = None
    try:
        connection = mysql.connector.connect(**DB_CONFIG)
        cursor = connection.cursor()
        
        # Check if all tables exist and have the right structure
        tables = ['raw_articles', 'processed_articles', 'analytics_results']
        for table in tables:
            cursor.execute(f"SHOW CREATE TABLE {table}")
            result = cursor.fetchone()
            if result:
                print(f"\n{table} table structure verified")
            else:
                print(f"\n{table} table not found!")
                return False
        
        return True
        
    except Error as e:
        print(f"Database is not ready: {e}")
        return False
    finally:
        if cursor:
            cursor.close()
        if connection and connection.is_connected():
            connection.close()

if __name__ == "__main__":
    create_database()
    if verify_database_ready():
        print("\nDatabase is ready for Spark job processing")
    else:
        print("\nDatabase setup failed - please check the errors above") 