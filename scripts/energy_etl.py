import os
import requests
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import time
import xml.etree.ElementTree as ET
from dotenv import load_dotenv

# Ladda miljövariabler
load_dotenv()

# Konfigurera credentials och API-inställningar från .env
GOOGLE_APPLICATION_CREDENTIALS = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")
API_TOKEN = os.getenv("API_TOKEN")
PROJECT_ID = os.getenv("PROJECT_ID")
DATASET_ID = "energy_data"
TABLE_ID = "load_data"

# Sätt credentials path
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = os.path.abspath(GOOGLE_APPLICATION_CREDENTIALS)

# URL för ENTSO-E API
url = "https://web-api.tp.entsoe.eu/api"

# Tidszoner och datumformat
now = datetime.utcnow()
start_date = (now - timedelta(days=1)).strftime("%Y%m%d%H00")  # 24 timmar tillbaka
end_date = now.strftime("%Y%m%d%H00")

# Zonidentifierare för Sverige - SE1 (BZN1SE1)
zone = "10YSE-1--------K"

def save_to_bigquery(df):
    """Sparar data till BigQuery"""
    client = bigquery.Client()
    table_id = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    
    # Konfigurera schema för tabellen
    schema = [
        bigquery.SchemaField("zone", "STRING"),
        bigquery.SchemaField("timestamp", "TIMESTAMP"),
        bigquery.SchemaField("position", "INTEGER"),
        bigquery.SchemaField("quantity", "FLOAT"),
        bigquery.SchemaField("load_timestamp", "TIMESTAMP")
    ]
    
    # Lägg till timestamp för när datan laddades
    df['load_timestamp'] = datetime.utcnow()
    
    # Konvertera timestamp till rätt format
    df['timestamp'] = pd.to_datetime(df['timestamp'])
    
    try:
        # Skapa eller hämta tabellen
        table = bigquery.Table(table_id, schema=schema)
        table = client.create_table(table, exists_ok=True)
        
        # Ladda data
        job_config = bigquery.LoadJobConfig(
            schema=schema,
            write_disposition="WRITE_APPEND",
        )
        
        job = client.load_table_from_dataframe(
            df, table_id, job_config=job_config
        )
        job.result()  # Vänta på att jobbet ska bli klart
        
        print(f"Laddade {len(df)} rader till {table_id}")
    except Exception as e:
        print(f"Fel vid sparande till BigQuery: {str(e)}")

def fetch_data():
    """Hämtar data från ENTSO-E API"""
    params = {
        "documentType": "A65",          # Total load
        "processType": "A16",           # Realised
        "outBiddingZone_Domain": zone,
        "periodStart": start_date,
        "periodEnd": end_date,
        "securityToken": API_TOKEN
    }

    print(f"Hämtar data för perioden: {start_date} till {end_date}")
    print(f"API parametrar: {params}")

    try:
        response = requests.get(url, params=params)
        print(f"Response Status Code: {response.status_code}")
        
        if response.status_code == 200:
            # Kontrollera om vi fick ett error-meddelande
            if 'Acknowledgement_MarketDocument' in response.text:
                root = ET.fromstring(response.text)
                reason = root.find('.//Reason/text').text
                print(f"API Meddelande: {reason}")
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
                    print(f"Processerar data för period: {start_time}")
                    
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
                print(f"Hittade {len(df)} datapunkter")
                print("\nExempel på data:")
                print(df.head())
                save_to_bigquery(df)
            else:
                print("Inga data hittades i XML-svaret")
                
    except Exception as e:
        print(f"Ett fel uppstod: {str(e)}")

    time.sleep(6)

if __name__ == "__main__":
    fetch_data()