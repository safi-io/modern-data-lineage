FROM python:3.12-slim

RUN apt-get update && apt-get install -y \
    docker.io \
    gcc \
    g++ \
    libpq-dev \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --upgrade pip

COPY dagster/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

ENV DAGSTER_HOME=/opt/dagster
COPY dagster/dagster.yaml /opt/dagster/dagster.yaml

# Meltano
COPY meltano /opt/dagster/meltano
WORKDIR /opt/dagster/meltano
RUN meltano install

# DBT
COPY dbt/modern_data_lineage /opt/dagster/dbt/modern_data_lineage
COPY dbt/modern_data_lineage/dbt /root/.dbt
WORKDIR /opt/dagster/dbt/modern_data_lineage
RUN dbt parse

WORKDIR /opt/dagster/app