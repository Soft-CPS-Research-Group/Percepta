from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.repositories.file_environment_repository import FileEnvironmentRepository
from app.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.repositories.mongo_time_series_repository import MongoTimeSeriesRepository


class RepositoryFactory:
    def __init__(self, configurations, logger):
        self._repositories = configurations.get("repositories")
        self._logger = logger
        self._mongo_client = None

    def build_environment_repository(self) -> EnvironmentRepository:
        repository_configs = self._repositories.get("environment_repository")
        repository_type = repository_configs.get("repository_type")

        if repository_type == 'file':
            repository_parameters = repository_configs.get("parameters")
            return FileEnvironmentRepository(repository_parameters, self._logger)
        else:
            raise ValueError(f"Unknown repository type: {repository_type}")

    def build_time_series_repository(self, group, environment) -> TimeSeriesRepository:
        repository_configs = self._repositories.get("time_series_repository")
        repository_type = repository_configs.get("repository_type")

        if repository_type == 'mongo':
            connection_parameters = repository_configs.get("parameters").get("connection_parameters")

            if self._mongo_client is None:
                from pymongo import MongoClient
                credentials = connection_parameters['credentials']
                self._mongo_client = MongoClient(
                    host=connection_parameters['host'],
                    port=connection_parameters['port'],
                    username=credentials['username'],
                    password=credentials['password'],
                    authSource=connection_parameters.get('authSource', 'admin'),
                    serverSelectionTimeoutMS=connection_parameters.get('serverSelectionTimeoutMS', 5000)
                )

            return MongoTimeSeriesRepository(group, environment, self._mongo_client, self._logger)
        else:
            raise ValueError(f"Unknown repository type: {repository_type}")