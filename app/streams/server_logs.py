import sys
from asyncio import AbstractEventLoop
from signal import SIGINT, signal
from typing import Callable, ClassVar

from confluent_kafka.aio import AIOConsumer
from loguru import logger
from pydantic import BaseModel
from reactivex import Subject, create, operators
from reactivex.observer import AutoDetachObserver
from reactivex.scheduler.eventloop import AsyncIOScheduler

from app.streams.base import Stream
from app.streams.utils import kafka_consumer_conf, loop_event
from config import settings


class ServerLogs(BaseModel, Stream):
    RUNNING: ClassVar[bool] = True

    def run(self):
        def stop(*_):
            ServerLogs.RUNNING = False

        signal(SIGINT, stop)

        with loop_event() as loop:
            aio_scheduler = AsyncIOScheduler(loop=loop)

            proxy = Subject()
            proxy.subscribe(on_next=lambda i: logger.success(f"proxy: {i}"))

            source = create(self._subscribe(loop))
            source.pipe(operators.map(lambda i: f"echo: {i}")).subscribe(
                proxy, scheduler=aio_scheduler
            )

    def _subscribe(self, loop: AbstractEventLoop) -> Callable:
        def subscribe(observer: AutoDetachObserver, _):
            async def handle():
                async for message in self._consumer():
                    observer.on_next(message)

            loop.create_task(handle())

        return subscribe

    async def _consumer(self):
        """
        Un contextmanager debe hacer yield exactamente una vez por eso se opto por async for
        """
        consumer = AIOConsumer(
            kafka_consumer_conf(
                {
                    "group.id": f"{settings.KAFKA_SERVERLOG_TOPIC}_1",
                    "bootstrap.servers": settings.KAFKA_SERVERLOG_HOST,
                }
            )
        )

        offset = 1
        try:
            await consumer.subscribe([settings.KAFKA_SERVERLOG_TOPIC])
            while ServerLogs.RUNNING:
                logger.info("run")

                if (message := await consumer.poll()) is None:
                    continue

                if err := message.error():
                    logger.error(err)
                    continue

                if (message := await self._validate_message(consumer, message)) is None:
                    continue

                yield message.value()
                offset += 1

                if offset % 100 == 0:
                    await consumer.commit()  # save offset in broker
                    logger.info("Stored offsets were committed")
        except Exception as _:
            logger.exception("Error in consumer loop")
        finally:
            await consumer.unsubscribe()
            await consumer.close()
            logger.info("Closed consumer")
            sys.exit(0)

    async def _validate_message(self, consumer: AIOConsumer, message) -> any:
        try:
            response = message
            await consumer.store_offsets(
                message=message
            )  # save processed message to local memory
            return response
        except Exception as _:
            logger.exception("Error in _process_message")
            await consumer.store_offsets(
                message=message
            )  # save processed message to local memory
            return None
