FROM python:3.11-slim

RUN apt-get update && apt-get install -y \
    curl \
    && rm -rf /var/lib/apt/lists/*

ENV DAGSTER_HOME=/opt/dagster/dagster_home
ENV PYTHONUNBUFFERED=1

RUN mkdir -p /opt/dagster/app /opt/dagster/dagster_home

WORKDIR /opt/dagster/app

COPY . /opt/dagster/app

RUN pip install --upgrade pip
RUN pip install dagster dagit pyspark==3.5.2

EXPOSE 3000

CMD ["dagster-webserver", "-h", "0.0.0.0", "-p", "3000", "-w", "workspace.yaml"]