# Använd officiell Python-bild
FROM python:3.10-slim

# Sätt arbetskatalog i containern
WORKDIR /app

# Kopiera requirements.txt och installera bibliotek
COPY requirements.txt /app/
RUN pip install --no-cache-dir -r requirements.txt

# Kopiera projektfiler
COPY . /app/

# Standardkommando för att köra ETL-skriptet
CMD ["python", "scripts/etl_script.py"]
