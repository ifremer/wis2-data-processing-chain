FROM python:3.11

WORKDIR /app

# Installation dependances WIS2
RUN \
pip3 install pywcmp pywis-pubsub PySocks python-dotenv && \
pywcmp bundle sync && \
pywis-pubsub schema sync

# copie des scripts
COPY *.py /app
