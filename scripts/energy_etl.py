import os
import pyarrow
import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import time
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Configure credentials and API settings from .env
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
API_TOKEN = os.getenv("API_TOKEN")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "Energy_Data"
TABLE_ID = "load_data"

# Set credentials path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(GOOGLE_APPLICATION_CREDENTIALS)

# ENTSO-E API URL
url = "https://web-api.tp.entsoe.eu/api"

# Timezones and date formats
now = datetime.utcnow()
start_date = (now - timedelta(days=1)).strftime("%Y%m%d%H00")  # 24 hours back
end_date = now.strftime("%Y%m%d%H00")

# Zone identifier for Sweden - SE1 (BZN1SE1)
zone = "10YSE-1--------K"

def save_to_bigquery(df):
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Configure table schema
    schema = [
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("position", "INTEGER"),
        bigquery.SchemaField("quantity", "FLOAT"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP")
    ]
    
    # Add load timestamp
    df['load_timestamp'] = datetime.utcnow()
    
    # Convert timestamp to correct format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    try:
        # Create or get the table
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        
        # Load data
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Wait for the job to complete
        
        print(f"Loaded {len(df)} rows to {table_id}")
    except Exception as e:
        print(f"Error saving to BigQuery: {str(e)}")

def fetch_data():
    """Fetch data from the ENTSO-E API"""
    params = {
        "documentType": "A65",          # Total load
        "processType": "A16",           # Realised
        "outBiddingZone_Domain": zone,
        "periodStart": start_date,
        "periodEnd": end_date,
        "securityToken": API_TOKEN
    }

    print(f"Fetching data for period: {start_date} to {end_date}")
    print(f"API parameters: {params}")

    try:
        response = requests.get(url, params=params)
        print(f"Response Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Check if we got an error message
            if 'Acknowledgement_MarketDocument' in response.text:
                root = ET.fromstring(response.text)
                reason = root.find('.//Reason/text').text
                print(f"API Message: {reason}")
                return
            
            # Parse XML
            root = ET.fromstring(response.text)
            
            # Define namespace
            ns = {'ns': 'urn:iec62325.351:tc57wg16:451-6:generationloaddocument:3:0'}
            
            # Extract data from XML
            data_filtered = []
            
            # Process time series data
            for timeseries in root.findall('.//ns:TimeSeries', ns):
                for period in timeseries.findall('.//ns:Period', ns):
                    start_time = period.find('ns:timeInterval/ns:start', ns).text
                    print(f"Processing data for period: {start_time}")
                    
                    for point in period.findall('ns:Point', ns):
                        position = point.find('ns:position', ns).text
                        quantity = point.find('ns:quantity', ns).text
                        data_filtered.append([
                            zone,
                            start_time,
                            int(position),
                            float(quantity)
                        ])
            
            if data_filtered:
                df = pd.DataFrame(data_filtered, columns=["zone", "timestamp", "position", "quantity"])
                print(f"Found {len(df)} data points")
                print("\nExample data:")
                print(df.head())
                save_to_bigquery(df)
            else:
                print("No data found in XML response")
                
    except Exception as e:
        print(f"An error occurred: {str(e)}")

    time.sleep(6)

if __name__ == "__main__":
    fetch_data()