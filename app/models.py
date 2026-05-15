from typing import ClassVar

from arangomapper import CollectionBase


class Logs(CollectionBase):
    _collection_name: ClassVar[str] = "logs"

    value: str


class User(CollectionBase):
    _collection_name: ClassVar[str] = "users"


class Host(CollectionBase):
    _collection_name: ClassVar[str] = "hosts"


class Daemon(CollectionBase):
    _collection_name: ClassVar[str] = "deamons"

    name: str
