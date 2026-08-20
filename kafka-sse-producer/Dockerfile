FROM python:3.12-slim

WORKDIR /app

COPY kafka-sse-producer/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY kafka-sse-producer/wikimedia_producer.py .

CMD ["python3", "wikimedia_producer.py"]