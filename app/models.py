from typing import ClassVar

from arangomapper import CollectionBase


class User(CollectionBase):
    _collection_name: ClassVar[str] = "users"


class Host(CollectionBase):
    _collection_name: ClassVar[str] = "hosts"


class Daemon(CollectionBase):
    _collection_name: ClassVar[str] = "deamons"

    name: str
