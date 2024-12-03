import os
import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import time
from dotenv import load_dotenv

# Ladda miljövariabler från .env
load_dotenv()

# API-token och andra inställningar från .env
API_TOKEN = os.getenv("API_TOKEN")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "Energy_Data"
TABLE_ID = "sweden_daily_load"

# Elpriser API URL
url = "https://www.elprisetjustnu.se/api/v1/prices/{}/{:02d}-{:02d}_{}.json"  # URL med format för år, månad, dag, zon

# Zoner för Sverige
zones = [
    "SE1",  # Zone SE1
    "SE2",  # Zone SE2
    "SE3",  # Zone SE3
    "SE4"   # Zone SE4
]

def get_latest_date_from_bigquery():
    """Hämtar senaste datum från BigQuery-tabellen."""
    client = bigquery.Client()
    query = f"""
        SELECT MAX(DATE(timestamp)) AS latest_date
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}`
    """
    try:
        query_job = client.query(query)
        result = query_job.result()
        latest_date = result.to_dataframe().iloc[0]['latest_date']
        return latest_date
    except Exception as e:
        print(f"Fel vid hämtning av senaste datum från BigQuery: {str(e)}")
        return None

def fetch_data():
    """Hämtar data från Elpriser API för alla svenska zoner från senaste sparade datum."""
    latest_date = get_latest_date_from_bigquery()
    if latest_date:
        start_date = latest_date + timedelta(days=1)  # Börja från dagen efter senaste sparade datum
    else:
        print("Ingen data hittades i databasen, börjar från 10 dagar bakåt.")
        start_date = datetime.utcnow() - timedelta(days=10)

    end_date = datetime.utcnow()  # Slutdatum är idag

    print(f"Hämtar data från {start_date.date()} till {end_date.date()}")

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
                    print(f"Respons Statuskod: {response.status_code}")

                    if response.status_code == 200:
                        try:
                            # Bearbeta API-svar
                            data = response.json()
                            data_filtered = []

                            for item in data:
                                timestamp = item['time_start']
                                price = item['SEK_per_kWh']
                                data_filtered.append([timestamp, price, zone])

                            if data_filtered:
                                df = pd.DataFrame(data_filtered, columns=["timestamp", "price", "zone"])
                                print(f"Hittade {len(df)} datapunkter")
                                print("\nExempeldata:")
                                print(df.head())

                                # Konvertera 'timestamp' kolumnen till datetime
                                df['timestamp'] = pd.to_datetime(df['timestamp'])
                                save_to_bigquery(df)
                            else:
                                print(f"Inga data funna för {zone}")
                        except Exception as e:
                            print(f"Fel vid bearbetning av API-svar: {str(e)}")
                            print("Hoppar över denna förfrågan.")
                        break
                    else:
                        print(f"Fel vid hämtning av data för {zone}, försöker igen... ({retry+1}/{num_retries})")
                        time.sleep(5)
                except Exception as e:
                    print(f"Ett fel inträffade: {str(e)}")
                    if retry < num_retries - 1:
                        print(f"Försöker igen... ({retry+1}/{num_retries})")
                        time.sleep(5)
                    else:
                        print(f"Misslyckades att hämta data för {zone} efter {num_retries} försök.")

            # Sleep för att undvika att göra för många API-anrop snabbt
            time.sleep(5)

            # Gå till nästa dag
            current_date += timedelta(days=1)

def save_to_bigquery(df):
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Definiera BigQuery schema
    schema = [
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("price", "FLOAT"),
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP")
    ]
    
    # Lägg till timestamp för när datan laddades
    df['load_timestamp'] = datetime.utcnow()
    
    try:
        # Skapa tabell eller få den existerande
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        
        # Ladda data till BigQuery
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Vänta på att jobbet slutförs
        
        print(f"Laddade {len(df)} rader till {table_id}")
    except Exception as e:
        print(f"Fel vid spara till BigQuery: {str(e)}")

if __name__ == "__main__":
    fetch_data()
