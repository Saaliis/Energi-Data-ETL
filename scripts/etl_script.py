import os
import requests
import pandas as pd 
from datetime import datetime, timedelta
from dotenv import load_dotenv
import time

# Ladda miljövariabler från .env
load_dotenv()

API_TOKEN = os.getenv("API_TOKEN")

# Funktion för att hämta elpriser från ENTSO-E API
def fetch_data():
    url = "https://web-api.tp.entsoe.eu/api"
    
    # Tidsintervall: 7 dagar bakåt
    start_date = (datetime.now() - timedelta(days=7)).strftime("%Y%m%d%H%M")  # Format: yyyyMMddHHmm
    end_date = datetime.now().strftime("%Y%m%d%H%M")  # Format: yyyyMMddHHmm

    # Zonidentifierare för Sverige (zon 1-4)
    zones = [
        "10YSE-1--------K",  # SE1
        "10YSE-2--------K",  # SE2
        "10YSE-3--------K",  # SE3
        "10YSE-4--------K",  # SE4
    ]

    # Skapa förfrågan för varje zon
    for zone in zones:
        params = {
            "securityToken": API_TOKEN,
            "documentType": "A44",  # Marknadspriser
            "processType": "A01",   # Realiserad data
            "outBiddingZone_Domain": zone,  # Zon
            "periodStart": start_date,  # Format: yyyyMMddHHmm
            "periodEnd": end_date,      # Format: yyyyMMddHHmm
        }

        print(f"Begär data för zon {zone} från {start_date} till {end_date}...")
        response = requests.get(url, params=params)

        if response.status_code == 200:
            print(f"Data hämtad för {zone} från API!")
            data = response.json()  # Om API returnerar JSON
            time.sleep(1)  # Fördröjning för att undvika att överskrida begärningsgränsen
            df = pd.DataFrame(data)
            # Här kan vi spara eller bearbeta datan
            df.to_csv(f"elpriser_{zone}_{start_date}_{end_date}.csv", index=False)
        else:
            print(f"API-anrop misslyckades för {zone}: {response.status_code}")
            print(f"Felmeddelande från API: {response.text}")  # Skriv ut detaljerade felmeddelanden från API
            return None
