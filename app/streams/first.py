from pydantic import BaseModel
from reactivex import create, operators
from reactivex.scheduler.eventloop import AsyncIOScheduler
from reactivex.subject import Subject

from app.streams.utils import loop_event


class First(BaseModel):
    def run(self):
        with loop_event() as loop:
            proxy = Subject()

            proxy.subscribe(
                on_next=lambda i: print(f"proxy: {i}"), on_completed=lambda: loop.stop()
            )

            aio_scheduler = AsyncIOScheduler(loop=loop)

            source = create(self._subscribe(loop))

            source.pipe(
                operators.map(lambda i: f"echo: {i}"),
                operators.delay(3.0),
            ).subscribe(proxy, scheduler=aio_scheduler)

    def _subscribe(self, loop):
        def subscribe(observer, _):
            async def handle():
                observer.on_next("Alpha")
                observer.on_next("Beta")
                observer.on_next("Gamma")
                observer.on_next("Delta")
                observer.on_next("Epsilon")
                observer.on_completed()

            loop.create_task(handle())

        return subscribe
