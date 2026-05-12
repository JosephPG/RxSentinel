import socket
from typing import Callable

from confluent_kafka.aio import AIOConsumer
from loguru import logger
from pydantic import BaseModel
from reactivex import Subject, create, operators
from reactivex.scheduler.eventloop import AsyncIOScheduler

from app.streams.base import Stream
from app.streams.utils import loop_event

running = True


class ServerLogs(BaseModel, Stream):
    def run(self):
        with loop_event() as loop:
            aio_scheduler = AsyncIOScheduler(loop=loop)

            proxy = Subject()
            proxy.subscribe(on_next=lambda i: logger.success(f"proxy: {i}"))

            source = create(self._subscribe(loop))
            source.pipe(operators.map(lambda i: f"echo: {i}")).subscribe(
                proxy, scheduler=aio_scheduler
            )

    def _subscribe(self, loop) -> Callable:
        def subscribe(observer, _):
            async def handle():
                topic = "server-logs"
                consumer = AIOConsumer(
                    {
                        "group.id": "server-logs_1",
                        "bootstrap.servers": "localhost:9092",
                        "client.id": socket.gethostname(),
                        "auto.offset.reset": "latest",
                        "enable.auto.commit": "false",
                        "enable.auto.offset.store": "false",
                        "partition.assignment.strategy": "cooperative-sticky",
                    }
                )

                i = 0
                try:
                    await consumer.subscribe([topic])
                    while running:
                        logger.info("run")
                        message = await consumer.poll()

                        if message is None:
                            continue

                        if i % 100 == 0:
                            position = await consumer.position(
                                await consumer.assignment()
                            )
                            logger.info(f"Current position: {position}")
                            await consumer.commit()
                            logger.info("Stored offsets were committed")

                        err = message.error()
                        if err:
                            logger.error(err)
                        else:
                            observer.on_next(message.value())
                            await consumer.store_offsets(message=message)
                finally:
                    await consumer.unsubscribe()
                    await consumer.close()
                    logger.info("Closed consumer")

            loop.create_task(handle())

        return subscribe
