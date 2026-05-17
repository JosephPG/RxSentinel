from asyncio import wait_for
from asyncio.tasks import create_task, sleep

import pytest
from arangomapper import AsyncAQLManager, AsyncStandardDatabase, For

from app.models import Logs
from app.streams.server_logs import ServerLogs

from tests.utils import server_logs_factory


@pytest.mark.asyncio(loop_scope="session")
async def test_server_logs(async_db: AsyncStandardDatabase, kafka_socket: str):
    create_task(server_logs_factory(kafka_socket))

    sl = ServerLogs(poll_time=0.5, url_bootstrap_server=kafka_socket)

    create_task(sl.run())

    while len(data := await AsyncAQLManager(async_db).add_for(For(Logs)).list()) < 10:
        await sleep(0.2)

    sl.running = False

    await wait_for(sl.handle_task, None)

    assert len(data) == 10


@pytest.mark.asyncio(loop_scope="session")
async def test_another():
    await sleep(1)
