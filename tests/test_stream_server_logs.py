from asyncio import wait_for
from asyncio.tasks import create_task, sleep

import pytest
from arangomapper import AsyncAQLManager, AsyncStandardDatabase, For

from app.models import Logs
from app.streams.server_logs import ServerLogs


@pytest.mark.asyncio(loop_scope="session")
async def test_server_logs(async_db: AsyncStandardDatabase):
    sl = ServerLogs()

    create_task(sl.run())

    while not (data := await AsyncAQLManager(async_db).add_for(For(Logs)).list()):
        await sleep(0.2)

    sl.running = False

    await wait_for(sl.handle_task, None)

    assert len(data) == 11


@pytest.mark.asyncio(loop_scope="session")
async def test_another():
    await sleep(10)
