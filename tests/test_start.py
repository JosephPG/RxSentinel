from arangomapper import AQLManager, CollectionManager, StandardDatabase

from app.models import Daemon


def test_start(db: StandardDatabase):
    cm: CollectionManager = CollectionManager(db)

    daemon = Daemon(name="ssh")

    cm.insert(daemon)

    assert AQLManager(db).get_by_id_or_key(Daemon, daemon.id)
