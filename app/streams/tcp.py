from asyncio import start_server
from collections import namedtuple

from pydantic import BaseModel
from reactivex import Subject, create, operators
from reactivex.scheduler.eventloop import AsyncIOScheduler

from app.streams.utils import loop_event

EchoItem = namedtuple("EchoItem", ["future", "data"])


class Tcp(BaseModel):
    def run(self):
        with loop_event() as loop:
            aio_scheduler = AsyncIOScheduler(loop=loop)

            proxy = Subject()
            proxy.subscribe(on_next=lambda i: i.future.set_result(i.data))

            source = create(self._subscribe(loop))
            source.pipe(
                operators.map(lambda i: i._replace(data="echo: {}".format(i.data))),
                operators.delay(5.0),
            ).subscribe(proxy, scheduler=aio_scheduler)

    def _subscribe(self, loop):
        def subscribe(observer, _):
            async def handle(reader, writer):
                print("new client connected")
                while True:
                    data = await reader.readline()
                    data = data.decode("utf-8")

                    if not data:
                        break

                    future = loop.create_future()
                    observer.on_next(EchoItem(future=future, data=data))
                    await future
                    writer.write(future.result().encode("utf-8"))

                print("Close the client socket")
                writer.close()

            print("starting server")
            server = start_server(handle, "127.0.0.1", 8888)
            loop.create_task(server)

        return subscribe
