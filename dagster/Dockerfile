FROM python:3.11-slim

RUN pip install --upgrade pip
RUN pip install \
    dagster \
    dagster-graphql \
    dagster-webserver \
    dagster-postgres \
    dagster-docker \
    dagit \
    pandas \
    "dlt[filesystem]" \
    "dlt[parquet]" \
    pyspark


ENV DAGSTER_HOME=/opt/dagster/

RUN mkdir -p $DAGSTER_HOME

WORKDIR $DAGSTER_HOME
