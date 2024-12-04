import os
import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import time
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Environment configurations
API_TOKEN = os.getenv("API_TOKEN")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "Energy_Data"
TABLE_ID = "sweden_daily_avg"  # Correct table name with proper structure

# API URL template
url = "https://www.elprisetjustnu.se/api/v1/prices/{}/{:02d}-{:02d}_{}.json"
zones = ["SE1", "SE2", "SE3", "SE4"]  # Swedish zones

def get_latest_date_from_bigquery():
    """Fetch the latest date from the BigQuery table."""
    client = bigquery.Client()
    query = f"""
        SELECT MAX(date) AS latest_date
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    try:
        query_job = client.query(query)
        result = query_job.result()
        latest_date = result.to_dataframe().iloc[0]['latest_date']
        print(f"Latest date in the table: {latest_date}")
        return latest_date
    except Exception as e:
        print(f"Error fetching latest date from BigQuery: {str(e)}")
        return None

def fetch_data():
    """Fetch data from the Elpriser API."""
    latest_date = get_latest_date_from_bigquery()
    if latest_date:
        start_date = datetime.combine(latest_date, datetime.min.time()) + timedelta(days=1)
    else:
        print("No data found in the database, starting from 10 days ago.")
        start_date = datetime.utcnow() - timedelta(days=10)

    end_date = datetime.utcnow()
    print(f"Fetching data from {start_date.date()} to {end_date.date()}")

    all_data = []

    for zone in zones:
        current_date = start_date
        while current_date <= end_date:
            api_url = url.format(current_date.year, current_date.month, current_date.day, zone)

            print(f"Hämtar data för zon: {zone}")
            print(f"API-url: {api_url}")

            num_retries = 3
            for retry in range(num_retries):
                try:
                    response = requests.get(api_url)
                    if response.status_code == 200:
                        try:
                            data = response.json()
                            # Calculate the daily average price for the zone
                            avg_price = sum(item['SEK_per_kWh'] for item in data) / len(data)
                            all_data.append([current_date.date(), zone, avg_price])
                        except Exception as e:
                            print(f"Error processing API response: {str(e)}")
                        break
                    else:
                        print(f"Failed to fetch data for {zone}: {response.status_code}")
                except Exception as e:
                    print(f"Error fetching data for {zone}: {str(e)}")
                time.sleep(5)
            current_date += timedelta(days=1)

    # Convert all_data to DataFrame
    if all_data:
        df = pd.DataFrame(all_data, columns=["date", "zone", "avg_price"])
        save_to_bigquery(df)

def save_to_bigquery(df):
    """Save the data to the existing BigQuery table."""
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # Define BigQuery schema
    schema = [
        bigquery.SchemaField("date", "DATE"),
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("avg_price", "FLOAT"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP")
    ]

    # Add a load timestamp column
    df['load_timestamp'] = datetime.utcnow()

    try:
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()
        print(f"Loaded {len(df)} rows to {table_id}")
    except Exception as e:
        print(f"Error saving to BigQuery: {str(e)}")

if __name__ == "__main__":
    fetch_data()
