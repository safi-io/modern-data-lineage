import os
import json
import logging

from kafka import KafkaProducer
from requests_sse import EventSource

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
)

logger = logging.getLogger("wikimedia-kafka-producer")

KAFKA_PRODUCER_WIKIMEDIA_URL = os.getenv("KAFKA_PRODUCER_WIKIMEDIA_URL")
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_PRODUCER_BOOTSTRAP_SERVERS")
USER_AGENT_HEADERS = os.getenv("KAFKA_PRODUCER_USER_AGENT_HEADERS")
KAFKA_TOPIC = os.getenv("KAFKA_PRODUCER_TOPIC")

logger.info("Starting Wikimedia Kafka producer")
logger.info("Wikimedia SSE URL: %s", KAFKA_PRODUCER_WIKIMEDIA_URL)
logger.info("Kafka bootstrap servers: %s", KAFKA_BOOTSTRAP_SERVERS)
logger.info("Kafka topic: %s", KAFKA_TOPIC)

required_config = {
    "KAFKA_PRODUCER_WIKIMEDIA_URL": KAFKA_PRODUCER_WIKIMEDIA_URL,
    "KAFKA_PRODUCER_BOOTSTRAP_SERVERS": KAFKA_BOOTSTRAP_SERVERS,
    "KAFKA_PRODUCER_USER_AGENT_HEADERS": USER_AGENT_HEADERS,
    "KAFKA_PRODUCER_TOPIC": KAFKA_TOPIC,
}

missing_config = [
    name for name, value in required_config.items()
    if not value
]

if missing_config:
    logger.error(
        "Missing required environment variables: %s",
        ", ".join(missing_config),
    )
    raise RuntimeError("Required environment variables are missing")


logger.info("Initializing Kafka producer")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

logger.info("Kafka producer initialized successfully")


headers = {
    "User-Agent": USER_AGENT_HEADERS
}

last_id = None
published_count = 0
invalid_json_count = 0

logger.info("Connecting to Wikimedia SSE stream...")

try:
    with EventSource(
        KAFKA_PRODUCER_WIKIMEDIA_URL,
        headers=headers
    ) as stream:

        logger.info("Connected to Wikimedia SSE stream")

        for event in stream:

            if event.type != "message":
                logger.debug(
                    "Ignoring SSE event of type: %s",
                    event.type,
                )
                continue

            try:
                change = json.loads(event.data)

            except ValueError:
                invalid_json_count += 1

                logger.warning(
                    "Received invalid JSON from Wikimedia SSE "
                    "(invalid JSON count: %d)",
                    invalid_json_count,
                )

                continue

            if change["meta"]["domain"] == "canary":
                continue

            event_id = str(change["meta"]["id"])
            last_id = event_id

            logger.debug(
                "Received Wikimedia event: %s",
                event_id,
            )

            try:
                future = producer.send(
                    topic=KAFKA_TOPIC,
                    key=event_id,
                    value=change,
                )

                # Kafka acknowledgement.
                metadata = future.get(timeout=10)

                published_count += 1

                if published_count % 1000 == 0:
                    logger.info(
                        "Published batch | "
                        "total_published=%d | "
                        "last_event_id=%s | "
                        "last_partition=%s | "
                        "last_offset=%s",
                        published_count,
                        event_id,
                        metadata.partition,
                        metadata.offset,
                    )

            except Exception:
                logger.exception(
                    "Failed to publish event to Kafka | event_id=%s",
                    event_id,
                )

except Exception:
    logger.exception(
        "Wikimedia SSE stream terminated unexpectedly"
    )
    raise

finally:
    logger.info("Closing Kafka producer...")

    try:
        producer.flush()
        producer.close()

        logger.info(
            "Kafka producer closed | "
            "published=%d | invalid_json=%d | last_id=%s",
            published_count,
            invalid_json_count,
            last_id,
        )

    except Exception:
        logger.exception("Error while closing Kafka producer")