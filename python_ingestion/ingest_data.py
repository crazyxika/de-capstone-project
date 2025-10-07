 
import requests
import json
import snowflake.connector
from dotenv import load_dotenv
import os
from time import sleep

# Load environment variables
load_dotenv()

# --- Snowflake Connection Details ---
SNOWFLAKE_USER = os.getenv('SNOWFLAKE_USER')
SNOWFLAKE_PASSWORD = os.getenv('SNOWFLAKE_PASSWORD')
SNOWFLAKE_ACCOUNT = os.getenv('SNOWFLAKE_ACCOUNT')
SNOWFLAKE_WAREHOUSE = os.getenv('SNOWFLAKE_WAREHOUSE')
SNOWFLAKE_DATABASE = os.getenv('SNOWFLAKE_DATABASE')
SNOWFLAKE_SCHEMA = os.getenv('SNOWFLAKE_SCHEMA')

# --- Configuration ---
POSTS_URL = "https://jsonplaceholder.typicode.com/posts"
COMMENTS_URL = "https://jsonplaceholder.typicode.com/comments"
POSTS_TABLE = "POSTS"
COMMENTS_TABLE = "COMMENTS"

# --- Utility Functions ---

def fetch_data(url):
    """Fetches JSON data from a public API endpoint."""
    print(f"\n📡 Fetching data from: {url}")
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        data = response.json()
        print(f" Retrieved {len(data)} records from {url}")
        return data
    except requests.exceptions.RequestException as e:
        print(f" ERROR: Could not fetch data from {url}: {e}")
        return None

def connect_to_snowflake():
    """Establishes connection to Snowflake using environment variables."""
    try:
        conn = snowflake.connector.connect(
            user=SNOWFLAKE_USER,
            password=SNOWFLAKE_PASSWORD,
            account=SNOWFLAKE_ACCOUNT,
            warehouse=SNOWFLAKE_WAREHOUSE,
            database=SNOWFLAKE_DATABASE,
            schema=SNOWFLAKE_SCHEMA
        )
        print("\n Successfully connected to Snowflake.")
        return conn
    except Exception as e:
        print(f" ERROR: Could not connect to Snowflake. Check credentials and network.\n{e}")
        return None

def load_data_to_snowflake(data, table_name, conn):
    """Loads JSON objects into a Snowflake VARIANT table using single inserts."""
    if not data:
        print(f" No data to load for table {table_name}. Skipping.")
        return

    cursor = conn.cursor()
    sql = f"INSERT INTO {table_name} (RAW_JSON) SELECT PARSE_JSON(%s)"
    print(f"\n⬆ Loading {len(data)} records into {conn.database}.{conn.schema}.{table_name} ...")

    try:
        cursor.execute(f"TRUNCATE TABLE {table_name}")
        for record in data:
            cursor.execute(sql, (json.dumps(record),))
        conn.commit()
        print(f" Successfully loaded {len(data)} records into {table_name}.")
    except Exception as e:
        print(f" ERROR loading data into Snowflake table {table_name}: {e}")
        conn.rollback()
    finally:
        cursor.close()

# --- Main Execution ---

def main():
    conn = connect_to_snowflake()
    if not conn:
        return

    try:
        # 1. Fetch and load POSTS
        posts_data = fetch_data(POSTS_URL)
        load_data_to_snowflake(posts_data, POSTS_TABLE, conn)

        sleep(2)  # avoid API overload

        # 2. Fetch and load COMMENTS
        comments_data = fetch_data(COMMENTS_URL)
        load_data_to_snowflake(comments_data, COMMENTS_TABLE, conn)

    finally:
        conn.close()
        print("\n Snowflake connection closed. Ingestion complete.\n")

if __name__ == "__main__":
    main()