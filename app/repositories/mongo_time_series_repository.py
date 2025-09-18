from pymongo.errors import PyMongoError
from datetime import datetime
from typing import Any
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository
from pymongo import MongoClient
from app.utils.logger import LoggingUtils
from pymongo.collection import Collection

class MongoTimeSeriesRepository(TimeSeriesRepository):
    """
    Time series repository concrete implementation using MongoDB.

    This class provides methods to connect to a specific MongoDB collection,
    insert new time series values, read values within a time range, and retrieve
    the latest recorded entry.

    The connection to MongoDB is automatically established during initialization
    and reconnected if the collection is unavailable during read/write operations.
    """

    _group: str  # Name of the database group to connect to (used to select the MongoDB database)
    _environment: str  # Environment identifier used to construct the collection name
    _logger: LoggingUtils  # Logger instance
    _client: MongoClient     # MongoClient instance to manage the connection to the MongoDB server
    _collection: Collection     # Collection object representing the specific MongoDB collection for time series data

    def __init__(self, group : str, environment : str, client : MongoClient, logger : LoggingUtils):
        self._group = group
        self._environment = environment
        self._logger = logger
        self._client = client
        self._connect()

    def _connect(self) -> None:
        """
        Establishes a connection to the MongoDB collection using configuration parameters.
        """
        try:
            # Access the database using the client and the group name
            db = self._client[self._group]

            # Access the collection with a name based on the environment (spaces replaced by underscores)
            self._collection = db[f'building_{self._environment.replace(" ", "_")}']

            self._logger.info(f"MongoDB connected: {self._collection.name}")

        except PyMongoError as e:
            self._logger.error(f"MongoDB connection failed: {e}")

    def write(self, value: Any) -> None:
        """
        Inserts a single time series value into the collection.

        Args:
            value (Any): Time series value to be inserted.
        """

        # Check if the collection is not initialized
        if self._collection is None:
            # If not, establish the MongoDB connection
            self._connect()

        # If the collection is still not available, log a warning and return an empty list
        if self._collection is None:
            raise Exception("MongoDB write failed: no active collection.")

        # Proceed if the collection is available
        if self._collection is not None:
            try:
                # Insert the provided value as a single document in the collection
                self._collection.insert_one(value)
                self._logger.info("Value successfully written to MongoDB.")
            except PyMongoError as e:
                self._logger.error(f"Failed to write value: {e}")

    def read(self, start_time: datetime, end_time: datetime) -> list:
        """
        Reads all time series entries from the collection between two timestamps.
        Results are sorted chronologically in ascending order.

        Args:
            start_time (datetime): Start time of the time series entries to be read.
            end_time (datetime): End time of the time series entries to be read.

        Returns:
            list: List of time series entries between start_time and end_time.
        """
        # Check if the collection is initialized
        if self._collection is None:
            # If not, establish the MongoDB connection
            self._connect()

        # If the collection is still not available, log a warning and return an empty list
        if self._collection is None:
            self._logger.warning("MongoDB read failed: no active collection.")
            return []

        try:
            # Construct a query to find documents where "timestamp" is between start_time and end_time
            query = {
                "timestamp": {
                    "$gte": start_time,  # greater than or equal to start_time
                    "$lte": end_time  # less than or equal to end_time
                }
            }

            # Execute the query and sort the results by "timestamp" in ascending order
            cursor = self._collection.find(query).sort("timestamp", 1)

            # Convert the cursor to a list and return it
            return list(cursor)
        except PyMongoError as e:
            self._logger.error(f"MongoDB read error: {e}")
            return []

    def latest(self) -> dict:
        """
        Retrieves the most recent time series entry from the collection.

        Returns:
            dict: Latest time series entry retrieved from MongoDB.
        """
        # Check if the collection is initialized
        if self._collection is None:
            # If not, establish the MongoDB connection
            self._connect()

        #TODO VER ISTO MELHOR, CONNECT LANÇA ERRO?
        # If the collection is still not available, log a warning and return an empty dictionary
        if self._collection is None:
            self._logger.warning("MongoDB latest query failed: no active collection.")
            return {}

        try:
            # Execute a query to retrieve all documents, sort by "timestamp" descending, and limit to 1 document
            result = self._collection.find().sort("timestamp", -1).limit(1)

            # Return the first document if it exists; otherwise, return an empty dictionary
            return result.next() if result.count() > 0 else {}
        except PyMongoError as e:
            self._logger.error(f"MongoDB latest retrieval failed: {e}")
            return {}
