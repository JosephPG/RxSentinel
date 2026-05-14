from asyncio import AbstractEventLoop, shield
from typing import Callable

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
    running: bool = True

    def run(self):
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
        Contextmanager debe hacer yield exactamente una vez por eso se opto por async for
        consumer.commit(): save offset in broker
        consumer.store_offsets(): save processed message to local memory
        shield: https://docs.python.org/es/3.13/library/asyncio-task.html#asyncio.shield
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
            while self.running:
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
                    await consumer.commit()
                    logger.info("Stored offsets were committed")

            if not self.running:
                raise KeyboardInterrupt
        except Exception as _:
            logger.exception("Error in consumer loop")
        finally:
            await shield(consumer.unsubscribe())
            await shield(consumer.close())
            logger.info("Close consumer")

    async def _validate_message(self, consumer: AIOConsumer, message) -> any:
        try:
            response = message
            await consumer.store_offsets(message=message)
            return response
        except Exception as _:
            logger.exception("Error in _process_message")
            await consumer.store_offsets(message=message)
            return None
