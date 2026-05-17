import socket
from asyncio import gather

from confluent_kafka import Message
from confluent_kafka.aio import AIOProducer
from loguru import logger

from config import settings


async def server_logs_factory(host: str):
    def delivery_report(err, msg: Message):
        if err is not None:
            logger.info("Message delivery failed: {}".format(err))
        else:
            logger.info("Message {} saved in {}".format(msg.value(), msg.topic()))

    conf = {
        "bootstrap.servers": host,
        "client.id": socket.gethostname(),
        "transactional.id": settings.KAFKA_SERVERLOG_TOPIC,
    }

    producer = AIOProducer(conf)

    await producer.init_transactions()
    await producer.begin_transaction()

    msgs = [
        await producer.produce(
            topic=settings.KAFKA_SERVERLOG_TOPIC,
            key=f"othertestkey{i}",
            value=f"othertestvalue{i}",
        )
        for i in range(10)
    ]

    for msg in await gather(*msgs):
        logger.info(
            "Produced to: {} [{}] @ {}".format(msg.topic(), msg.partition(), msg.offset())
        )

    await producer.commit_transaction()
    await producer.close()
