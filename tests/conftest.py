from time import sleep

import pytest_asyncio
from arangomapper import (
    AsyncConn,
    AsyncStandardDatabase,
    StandardDatabase,
    async_restart_db,
    get_db,
    restart_db,
)
from confluent_kafka.admin import AdminClient
from confluent_kafka.cimpl import NewTopic
from pytest import fixture
from testcontainers.kafka import KafkaContainer

from config import settings


@fixture
def db():
    db: StandardDatabase = get_db("other")
    yield db
    restart_db(db)


@pytest_asyncio.fixture(loop_scope="session")
async def async_db():
    async_db: AsyncStandardDatabase = await AsyncConn.async_get_db("test_async")
    yield async_db
    await async_restart_db(async_db)


@fixture(scope="session")
def kafka_socket():
    container = KafkaContainer()

    # Config REPLICATION_FACTOR to 1 for async calls
    container.with_env("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    container.with_env("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    container.with_env("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")

    with container as kafka:
        bootstrap_server = kafka.get_bootstrap_server()
        conf = {"bootstrap.servers": bootstrap_server}
        admin_client = AdminClient(conf)
        new_topic = NewTopic(
            settings.KAFKA_SERVERLOG_TOPIC, num_partitions=3, replication_factor=1
        )

        admin_client.create_topics([new_topic])

        yield bootstrap_server
