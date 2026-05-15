import pytest_asyncio
from arangomapper import (
    AsyncConn,
    AsyncStandardDatabase,
    StandardDatabase,
    async_restart_db,
    get_db,
    restart_db,
)
from pytest import fixture


@fixture
def db():
    db: StandardDatabase = get_db("other")
    yield db
    restart_db(db)


@pytest_asyncio.fixture(loop_scope="session")
async def async_db():
    async_db: AsyncStandardDatabase = await AsyncConn.async_get_db("test_async")
    yield async_db
    await async_restart_db(async_db)
