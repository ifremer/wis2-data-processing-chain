ARG PYTHON_TAG="3.11-slim"
FROM python:${PYTHON_TAG}

ENV PYTHONPATH=/app
ENV PYWIS_PUBSUB_GDC_URL=https://api.weather.gc.ca/collections/wis2-discovery-metadata
WORKDIR /app

# Dépendances
COPY requirements.txt requirements.txt
RUN \
    pip install --no-cache-dir -r requirements.txt && \
    # init pywis-pubsub library
    pywis-pubsub schema sync

# Script de génération du message
COPY wis2_data_processing_chain wis2_data_processing_chain
