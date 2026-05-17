import asyncio
from asyncio import AbstractEventLoop, Task, sleep
from typing import Callable

from arangomapper import (
    AsyncCollectionManager,
    AsyncConn,
    AsyncStandardDatabase,
)
from confluent_kafka import Message
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
    poll_time: float | None = None
    offset_limit: int = 100
    url_bootstrap_server: str = settings.KAFKA_SERVERLOG_HOST
    kafka_topic: str = settings.KAFKA_SERVERLOG_TOPIC

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
        Contextmanager must perform yield exactly once, hence the choice of async for
        consumer.store_offsets(): save processed message to local memory
        consumer.commit(): save offset in broker
        """
        consumer = AIOConsumer(
            kafka_consumer_conf(
                {
                    "group.id": f"{self.kafka_topic}_1",
                    "bootstrap.servers": self.url_bootstrap_server,
                }
            )
        )

        offset = 1
        try:
            await consumer.subscribe([self.kafka_topic])

            logger.info("run")

            while self.running:
                if not (message := await self._poll(consumer)):
                    continue

                if err := message.error():
                    logger.error(err)
                    continue

                message = await self._process_message(consumer, message)

                yield message.value()
                offset += 1

                if offset % self.offset_limit == 0:
                    await consumer.commit()
                    logger.info("Stored offsets were committed")
        except Exception:
            logger.exception("Error in consumer loop")
        finally:
            await consumer.unsubscribe()
            await consumer.close()
            logger.info("Close consumer")

    async def _poll(self, consumer) -> Message:
        if self.poll_time:
            return await consumer.poll(self.poll_time)
        return await consumer.poll()

    async def _process_message(self, consumer: AIOConsumer, message) -> Message:
        # Loop or recursion for retrying in case of error? For unlimited retries,
        # a loop is better because recursion will eventually trigger a stack overflow.

        while self.running:
            try:
                db: AsyncStandardDatabase = await AsyncConn.async_get_db(
                    settings.ARANGO_DB
                )

                manager = AsyncCollectionManager(db)
                await manager.insert(Logs(value=message.value()))

                await consumer.store_offsets(message=message)

                return message
            except Exception:
                logger.exception("Error in _process_message")
                logger.info("Retry _process_message")
                await sleep(2)
