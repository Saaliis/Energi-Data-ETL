# Använd officiell Python-bild
FROM python:3.10-slim

# Sätt arbetskatalog i containern
WORKDIR /app

# Kopiera requirements.txt till containern
COPY requirements.txt /app/

# Installera Python-bibliotek från requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera hela projektet till containern
COPY . /app/

# Standardkommando för att köra ETL-skriptet
CMD ["python", "scripts/etl_script.py"]
