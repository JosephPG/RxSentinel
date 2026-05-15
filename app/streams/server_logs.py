import asyncio
from asyncio import AbstractEventLoop, Task, shield
from typing import Callable

import arangomapper
from arangomapper import (
    AsyncAQLManager,
    AsyncCollectionManager,
    AsyncConn,
    AsyncStandardDatabase,
)
from confluent_kafka.aio import AIOConsumer
from loguru import logger
from pydantic import BaseModel, PrivateAttr
from reactivex import Subject, create, operators
from reactivex.observer import AutoDetachObserver
from reactivex.scheduler.eventloop import AsyncIOScheduler

from app.models import Logs
from app.streams.base import Stream
from app.streams.utils import kafka_consumer_conf
from config import settings


class ServerLogs(BaseModel, Stream):
    running: bool = True

    _loop: AbstractEventLoop | None = PrivateAttr(default=None)
    _handle_task: Task | None = PrivateAttr(default=None)

    async def run(self):
        aio_scheduler = AsyncIOScheduler(loop=self.loop)

        proxy = Subject()
        proxy.subscribe(
            on_next=lambda i: logger.success(f"proxy: {i}"),
            on_completed=lambda: logger.success("Done!"),
        )

        source = create(self._subscribe())
        source.pipe(operators.map(lambda i: f"echo: {i}")).subscribe(
            proxy, scheduler=aio_scheduler
        )

    @property
    def loop(self) -> AbstractEventLoop:
        if not self._loop:
            self._loop = asyncio.get_running_loop()
        return self._loop

    @property
    def handle_task(self) -> Task:
        return self._handle_task

    def _subscribe(self) -> Callable:
        def subscribe(observer: AutoDetachObserver, _):
            async def handle():
                async for message in self._consumer():
                    observer.on_next(message)
                observer.on_completed()

            self._handle_task = self.loop.create_task(handle())

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

            logger.info("run")

            while self.running:
                if (message := await consumer.poll(0.5)) is None:
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
        except Exception as _:
            logger.exception("Error in consumer loop")
        finally:
            await consumer.unsubscribe()
            await consumer.close()
            logger.info("Close consumer")

    async def _validate_message(self, consumer: AIOConsumer, message) -> any:
        try:
            db: AsyncStandardDatabase = await AsyncConn.async_get_db(settings.ARANGO_DB)

            manager = AsyncCollectionManager(db)
            await manager.insert(Logs(value=message.value()))

            await consumer.store_offsets(message=message)

            return message
        except Exception as _:
            logger.exception("Error in _process_message")
            await consumer.store_offsets(message=message)
            return
