from arangomapper import StandardDatabase, get_db, restart_db
from pytest import fixture


@fixture
def db():
    db: StandardDatabase = get_db("other")
    yield db
    restart_db(db)
