import asyncio
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from app.core.storages.runtime_data_storage.mongodb_runtime_data_storage import MongoDBRuntimeDataStorage
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase
from app.core.storages.runtime_data_storage.runtime_data_storage_launcher import RuntimeDataStorageLauncher

_CLIENT = "app.core.storages.runtime_data_storage.mongodb_runtime_data_storage.AsyncMongoClient"

DATA = {"model_inputs": {"a": 1}, "decisions": {"B01": 3}, "corrected_decisions": None,
        "timestamp": "2026-08-19 11:00:00 +0000"}


def _storage(connection_info=None, group="living lab", environment="R-H-01"):
    with patch(_CLIENT) as client_class:
        collection = MagicMock(insert_one=AsyncMock())
        client_class.return_value.__getitem__.return_value.__getitem__.return_value = collection
        client_class.return_value.close = AsyncMock()
        storage = MongoDBRuntimeDataStorage(
            connection_info or {"host": "localhost", "port": 27017}, group, environment, MagicMock())
    return storage, client_class, collection


def test_client_and_collection_names():
    storage, client_class, _ = _storage({"host": "db", "port": 1234, "credentials": {"username": "u", "password": "p"},
                                         "authSource": "x", "serverSelectionTimeoutMS": 100},
                                        group="living lab", environment="R-H-01 a.b")
    client_class.assert_called_once_with(host="db", port=1234, username="u", password="p", authSource="x",
                                         serverSelectionTimeoutMS=100)
    client = client_class.return_value
    client.__getitem__.assert_called_once_with("living_lab")
    client.__getitem__.return_value.__getitem__.assert_called_once_with("building_R-H-01_a_b")


def test_defaults_without_credentials():
    _, client_class, _ = _storage()
    client_class.assert_called_once_with(host="localhost", port=27017, username=None, password=None,
                                         authSource="admin", serverSelectionTimeoutMS=5000)


def test_store_saves_the_timestamp_as_a_date_without_changing_the_data():
    storage, _, collection = _storage()
    original = dict(DATA)
    asyncio.run(storage.store(DATA))
    document = collection.insert_one.await_args.args[0]
    assert document["timestamp"] == datetime(2026, 8, 19, 11, 0, 0, tzinfo=timezone.utc)
    assert {k: v for k, v in document.items() if k != "timestamp"} == {k: v for k, v in DATA.items() if k != "timestamp"}
    assert DATA == original


def test_errors_of_mongodb_are_not_hidden():
    storage, _, collection = _storage()
    collection.insert_one.side_effect = ConnectionError("down")
    with pytest.raises(ConnectionError):
        asyncio.run(storage.store(DATA))


@pytest.mark.parametrize("data", [
    None, [], {k: v for k, v in DATA.items() if k != "decisions"},
    {**DATA, "timestamp": "yesterday"}, {**DATA, "timestamp": None},
])
def test_invalid_data(data):
    storage, _, collection = _storage()
    with pytest.raises(ValueError):
        asyncio.run(storage.store(data))
    collection.insert_one.assert_not_awaited()


def test_stop_closes_the_client():
    storage, client_class, _ = _storage()
    asyncio.run(storage.stop())
    client_class.return_value.close.assert_awaited_once()


@pytest.mark.parametrize("connection_info", [
    {}, {"port": 27017}, {"host": "", "port": 27017}, {"host": "db"}, {"host": "db", "port": "27017"},
    {"host": "db", "port": True}, {"host": "db", "port": 0}, {"host": "db", "port": 70000},
    {"host": "db", "port": 1, "credentials": {"username": "u"}}, {"host": "db", "port": 1, "credentials": "u:p"},
    {"host": "db", "port": 1, "authSource": ""}, {"host": "db", "port": 1, "serverSelectionTimeoutMS": 0},
    {"host": "db", "port": 1, "serverSelectionTimeoutMS": "5"},
])
def test_invalid_connection_info(connection_info):
    with patch(_CLIENT) as client_class:
        with pytest.raises(ValueError):
            MongoDBRuntimeDataStorage(connection_info, "g", "e", MagicMock())
    client_class.assert_not_called()


@pytest.mark.parametrize("group,environment,logger", [("", "e", MagicMock()), ("g", " ", MagicMock()),
                                                      (None, "e", MagicMock()), ("g", "e", None)])
def test_invalid_base_arguments(group, environment, logger):
    with patch(_CLIENT):
        with pytest.raises(ValueError):
            MongoDBRuntimeDataStorage({"host": "db", "port": 1}, group, environment, logger)


def test_connection_info_must_be_a_dictionary():
    with patch(_CLIENT):
        with pytest.raises(ValueError):
            MongoDBRuntimeDataStorage("db", "g", "e", MagicMock())


def test_base_class_cannot_be_instantiated():
    with pytest.raises(TypeError):
        RuntimeDataStorageBase({}, "g", "e", MagicMock())


def test_launcher_builds_the_registered_storage():
    launcher = RuntimeDataStorageLauncher(MagicMock())
    with patch(_CLIENT):
        storage = launcher.build_runtime_data_storage("mongodb", {"host": "db", "port": 1}, "g", "e")
    assert isinstance(storage, MongoDBRuntimeDataStorage)


def test_launcher_rejects_unknown_types():
    with pytest.raises(ValueError):
        RuntimeDataStorageLauncher(MagicMock()).build_runtime_data_storage("sqlite", {}, "g", "e")
