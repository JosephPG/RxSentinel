from confluent_kafka import Producer

from config import settings


def server_logs_factory_data():
    topic = "test"
    producer = Producer({"bootstrap.servers": settings.KAFKA_SERVERLOG_HOST})

    for x in range(10):
        pass
