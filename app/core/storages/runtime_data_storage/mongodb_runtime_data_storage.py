# --- Standard Library Imports ---
import re
from typing import Any, Dict

# --- Third-Party Imports ---
from pymongo import AsyncMongoClient

# --- Internal Framework Imports ---
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import parse_timestamp


class MongoDBRuntimeDataStorage(RuntimeDataStorageBase):
    """Runtime Data Storage that keeps the data of an environment in a MongoDB collection.

    The data base is the group of the environment and the collection is "building_<environment name>". The characters
    that MongoDB does not accept in these names (such as spaces and dots) become underscores, e.g., the group
    "living lab" is the data base "living_lab".

    Every inference is a document with the structure of 'store', and its 'timestamp' is saved as a date (and not as
    text), so that the documents can be searched by time.

    The storage uses the asynchronous client of PyMongo, so it does not block the other environments while it waits for
    the database. The client connects when it is first used, not when the storage is created.
    """

    _LOG_PREFIX: str = "MongoDB Runtime Data Storage |"

    # The characters that MongoDB does not accept in the name of a data base or of a collection
    _INVALID_NAME_CHARACTERS = re.compile(r'[ /\\."$*<>:|?]')

    _REQUIRED_KEYS = ("model_inputs", "decisions", "corrected_decisions", "timestamp")

    def __init__(self, connection_info: Dict[str, Any], group: str, environment_name: str, logger: LoggingUtils) -> None:
        """Validates the connection info and prepares the collection.

        Expected `connection_info` Data Structure:
        ------------------------------------------
        {
            "host": "localhost",                    # Host of the MongoDB server
            "port": 27017,                          # Port of the MongoDB server
            "credentials": {                        # (dict, optional) Without it, there is no authentication
                "username": "percepta",
                "password": "secret"
            },
            "authSource": "admin",                  # (str, optional) Data base with the user. Defaults to "admin"
            "serverSelectionTimeoutMS": 5000        # (int, optional) How long to wait for the server. Defaults to 5000
        }

        Args:
            connection_info (Dict[str, Any]): Settings to access MongoDB, as detailed above.
            group (str): Group of the environment, which is the name of the data base.
            environment_name (str): Name of the environment, which gives the name of the collection.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the connection info is invalid (see above), or the base class rejects an argument.
        """
        super().__init__(connection_info, group, environment_name, logger)

        # Guard Clause 1: Checks the host.
        host = connection_info.get("host")
        if not isinstance(host, str) or not host.strip():
            raise ValueError(f"{self._LOG_PREFIX} 'host' must be a non-empty string.")

        # Guard Clause 2: Checks the port ('bool' is a number in Python, but not here).
        port = connection_info.get("port")
        if isinstance(port, bool) or not isinstance(port, int) or not 0 < port < 65536:
            raise ValueError(f"{self._LOG_PREFIX} 'port' must be an integer between 1 and 65535.")

        # Guard Clause 3: Checks the credentials, if there are any.
        credentials = connection_info.get("credentials")
        if credentials is not None and (
                not isinstance(credentials, dict)
                or not all(isinstance(credentials.get(key), str) and credentials[key] for key in ("username", "password"))
        ):
            raise ValueError(f"{self._LOG_PREFIX} 'credentials' must have a 'username' and a 'password'.")
        credentials = credentials or {}

        # Guard Clause 4: Checks the authentication data base.
        auth_source = connection_info.get("authSource", "admin")
        if not isinstance(auth_source, str) or not auth_source.strip():
            raise ValueError(f"{self._LOG_PREFIX} 'authSource' must be a non-empty string.")

        # Guard Clause 5: Checks the timeout.
        timeout = connection_info.get("serverSelectionTimeoutMS", 5000)
        if isinstance(timeout, bool) or not isinstance(timeout, int) or timeout <= 0:
            raise ValueError(f"{self._LOG_PREFIX} 'serverSelectionTimeoutMS' must be a positive integer.")

        self._client = AsyncMongoClient(
            host=host,
            port=port,
            username=credentials.get("username"),
            password=credentials.get("password"),
            authSource=auth_source,
            serverSelectionTimeoutMS=timeout
        )
        database_name = self._INVALID_NAME_CHARACTERS.sub("_", group)
        collection_name = "building_" + self._INVALID_NAME_CHARACTERS.sub("_", environment_name)
        self._collection = self._client[database_name][collection_name]

    async def store(self, data: Dict[str, Any]) -> None:
        """Stores the data of one inference as a document.

        The errors of MongoDB (e.g., the server is unreachable) are NOT hidden: they are raised, and whoever stores the
        data decides what to do (see the base class).

        Expected Input Structure:
        -------------------------
        See `RuntimeDataStorageBase.store`.

        Args:
            data (Dict[str, Any]): The data of the inference.

        Raises:
            ValueError: If the data is not a dictionary with the four keys above, or its 'timestamp' is not in the
                format "%Y-%m-%d %H:%M:%S %z".
            pymongo.errors.PyMongoError: If MongoDB fails.
        """
        # Guard Clause 1: Checks that the data has everything the document needs.
        if not isinstance(data, dict) or any(key not in data for key in self._REQUIRED_KEYS):
            raise ValueError(f"{self._LOG_PREFIX} The data must be a dictionary with {list(self._REQUIRED_KEYS)}.")

        # A copy, because MongoDB adds the '_id' to the document it inserts and the caller's data must not change.
        # The 'timestamp' becomes a date (MongoDB saves it in UTC). 'parse_timestamp' raises a ValueError if it is wrong.
        document = {**data, "timestamp": parse_timestamp(data["timestamp"])}

        await self._collection.insert_one(document)

    async def stop(self) -> None:
        """Closes the connection to MongoDB."""
        await self._client.close()
