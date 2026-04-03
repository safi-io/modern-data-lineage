FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    docker.io \
    gcc \
    g++ \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

ENV DAGSTER_HOME=/opt/dagster/
RUN mkdir -p /opt/dagster/
COPY dagster.yaml /opt/dagster/dagster.yaml
WORKDIR /opt/dagster/app