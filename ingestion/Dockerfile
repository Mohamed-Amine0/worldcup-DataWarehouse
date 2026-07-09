FROM python:3.11-slim

WORKDIR /app

# Installer les dépendances
COPY ingestion/requirements.txt /app/ingestion/requirements.txt
RUN pip install --no-cache-dir -r /app/ingestion/requirements.txt

# Copier le code source de l'ingestion et des utilitaires
COPY ingestion /app/ingestion
COPY utils /app/utils

# Variables d'environnement par défaut
ENV DATA_DIR=/data/source
ENV DB_HOST=postgres
ENV DB_PORT=5432
ENV DB_USER=postgres
ENV DB_PASSWORD=postgres
ENV DB_NAME=worldcup_dw

# Exécuter le script d'ingestion
CMD ["python", "ingestion/csv_to_postgres.py"]
