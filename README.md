# Energy Data ETL Pipeline

En komplett ETL-pipeline för att extrahera, transformera och ladda energiprisdata för alla svenska elområden. Detta projekt integrerar Python, BigQuery och Power BI för att demonstrera realtidsanalys av energidata.

![Python](https://img.shields.io/badge/-Python-blue)
![Power BI](https://img.shields.io/badge/-Power%20BI-yellow)
![BigQuery](https://img.shields.io/badge/-BigQuery-green)


## Beskrivning
Detta projekt är en ETL-pipeline som hämtar elpriser från [elprisetjustnu.se](https://www.elprisetjustnu.se/), transformerar datan och laddar upp den till BigQuery för vidare analys. Pipeline täcker zonerna SE1, SE2, SE3 och SE4. 

## Databasstruktur
Denna bild visar strukturen i BigQuery-databasen:

![Databasstruktur](images/databasstruktur.png)

## Slutlig Dashboard
Här är en bild av den slutliga Power BI-dashboards:

![Dashboard](images/dashboard.png)


## Funktioner
- **Extract**: Hämtar elpriser från API.
- **Transform**: Beräknar genomsnittliga elpriser per dag och zon.
- **Load**: Laddar datan till Google BigQuery.


## Installation och körning
### Krav
- Python 3.9 eller senare
- Google Cloud-konto och BigQuery-projekt
- API-token från elprisetjustnu.se

### Lokalt
1. Klona repository:
   ```bash
   git clone https://github.com/username/Energy-Data-ETL.git
   cd Energy-Data-ETL
