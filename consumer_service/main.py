"""Consumes log/metric messages from RabbitMQ and writes them to MongoDB.

Messages that cannot be decoded or stored are rejected without requeue, so
RabbitMQ routes them to the dead-letter exchange declared by the producer.
"""

import json
import logging
import os
import signal
import sys
import time

import pika
from pymongo import ASCENDING, MongoClient
from pymongo.errors import PyMongoError

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("consumer")

RABBITMQ_URL = os.getenv("RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F")
MONGO_URL = os.getenv("MONGO_URL", "mongodb://mongo:27017")
MONGO_DB = os.getenv("MONGO_DB", "metricflow")
PREFETCH = int(os.getenv("PREFETCH", "50"))

# queue name -> mongo collection name
QUEUES = {
    "logs": "logs",
    "metrics": "metrics",
}


def connect_mongo():
    client = MongoClient(MONGO_URL, serverSelectionTimeoutMS=5000)
    client.admin.command("ping")
    db = client[MONGO_DB]

    # Time-ordered reads per service are the common query pattern.
    for collection in QUEUES.values():
        db[collection].create_index([("service", ASCENDING), ("timestamp", ASCENDING)])

    log.info("connected to mongo at %s, db=%s", MONGO_URL, MONGO_DB)
    return client, db


def connect_rabbit(retries: int = 30):
    """Wait for RabbitMQ to accept connections, then open a channel."""
    params = pika.URLParameters(RABBITMQ_URL)
    params.heartbeat = 30

    for attempt in range(1, retries + 1):
        try:
            connection = pika.BlockingConnection(params)
            log.info("connected to rabbitmq")
            return connection
        except pika.exceptions.AMQPConnectionError:
            log.warning("rabbitmq not ready (attempt %s/%s)", attempt, retries)
            time.sleep(2)

    raise RuntimeError("could not connect to rabbitmq")


def make_handler(collection):
    """Build a pika consume callback that inserts into the given collection."""

    def on_message(channel, method, properties, body):
        try:
            document = json.loads(body)
        except json.JSONDecodeError:
            log.error("undecodable message, dead-lettering: %r", body[:200])
            channel.basic_reject(method.delivery_tag, requeue=False)
            return

        try:
            collection.insert_one(document)
        except PyMongoError as exc:
            # Transient failures should be retried, so requeue rather than
            # dead-letter. A persistently broken document will loop; add a
            # delivery-count limit if that becomes a problem.
            log.error("mongo insert failed, requeueing: %s", exc)
            channel.basic_nack(method.delivery_tag, requeue=True)
            time.sleep(1)
            return

        channel.basic_ack(method.delivery_tag)
        log.info("stored message in %s", collection.name)

    return on_message


def main():
    mongo_client, db = connect_mongo()
    connection = connect_rabbit()
    channel = connection.channel()
    channel.basic_qos(prefetch_count=PREFETCH)

    for queue, collection_name in QUEUES.items():
        # passive: the producer owns the topology (including the DLX wiring).
        channel.queue_declare(queue, durable=True, passive=True)
        channel.basic_consume(queue, make_handler(db[collection_name]))
        log.info("consuming %s -> %s", queue, collection_name)

    def shutdown(signum, frame):
        log.info("shutting down")
        try:
            channel.stop_consuming()
        finally:
            connection.close()
            mongo_client.close()
        sys.exit(0)

    signal.signal(signal.SIGTERM, shutdown)
    signal.signal(signal.SIGINT, shutdown)

    channel.start_consuming()


if __name__ == "__main__":
    main()
