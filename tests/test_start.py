from arangomapper import StandardDatabase


def test_start(db: StandardDatabase):
    assert db
