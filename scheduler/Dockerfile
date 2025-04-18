FROM gitlab-registry.ifremer.fr/ifremer-commons/docker/images/python:3.9-slim AS development

WORKDIR /develop

COPY requirements-*.txt .
COPY makefile .

RUN \
    apt-get update && apt-get install -y make && \
    make install-runtime && \
    make install-dev

FROM development AS test

WORKDIR /tests

COPY . .

RUN \
    make test

FROM apache/airflow:2.10.3-python3.9 AS runtime

COPY requirements-runtime.txt .

RUN \
    pip install -r requirements-runtime.txt