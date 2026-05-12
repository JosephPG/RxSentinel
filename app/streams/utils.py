from asyncio import new_event_loop
from contextlib import contextmanager


@contextmanager
def loop_event():
    loop = new_event_loop()

    yield loop

    loop.run_forever()
    loop.close()
