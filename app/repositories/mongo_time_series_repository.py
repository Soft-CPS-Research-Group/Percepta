from pymongo import MongoClient
from pymongo.errors import PyMongoError
from datetime import datetime
from typing import Any, List, Dict
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository


class MongoTimeSeriesRepository(TimeSeriesRepository):
    def __init__(self, group, environment, client, logger):
        super().__init__(group, environment, logger)
        self._client = client
        self._collection = None
        self._connect()

    def _connect(self):
        """
        Establishes a connection to the MongoDB collection using configuration parameters.
        """
        try:
            db = self._client[self._group]
            self._collection = db[f'building_{self._environment.replace(" ", "_")}']
            self._logger.info(f"MongoDB connected: {self._collection.name}")
        except PyMongoError as e:
            self._logger.error(f"MongoDB connection failed: {e}")
            self._collection = None

    def write(self, value: Any) -> None:
        """
        Inserts a single time series value into the collection.
        """
        if self._collection is None:
            self._connect()

        if self._collection is not None:
            try:
                self._collection.insert_one(value)
                self._logger.info("Value successfully written to MongoDB.")
            except PyMongoError as e:
                self._logger.error(f"Failed to write value: {e}")

    def read(self, start_time: datetime, end_time: datetime) -> List[Dict[str, Any]]:
        """
        Reads all time series entries from the collection between two timestamps.
        Results are sorted chronologically in ascending order.
        """
        if not self._collection:
            self._connect()

        if not self._collection:
            self._logger.warning("MongoDB read failed: no active collection.")
            return []

        try:
            query = {
                "timestamp": {
                    "$gte": start_time,
                    "$lte": end_time
                }
            }
            cursor = self._collection.find(query).sort("timestamp", 1)
            return list(cursor)
        except PyMongoError as e:
            self._logger.error(f"MongoDB read error: {e}")
            return []

    def latest(self) -> Dict[str, Any]:
        """
        Retrieves the most recent time series entry from the collection.
        """
        if not self._collection:
            self._connect()

        if not self._collection:
            self._logger.warning("MongoDB latest query failed: no active collection.")
            return {}

        try:
            result = self._collection.find().sort("timestamp", -1).limit(1)
            return result.next() if result.count() > 0 else {}
        except PyMongoError as e:
            self._logger.error(f"MongoDB latest retrieval failed: {e}")
            return {}
