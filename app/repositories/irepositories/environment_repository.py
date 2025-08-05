from abc import ABC, abstractmethod

class EnvironmentRepository(ABC):
    @abstractmethod
    def get_environments(self) -> dict:
        """
        Returns all environments with their associated entities and characteristics.

        The method combines entities from different providers if they belong to the same environment.

        Returns:
            dict -> A dictionary where each key is an environment name, and the value is a structure
                    containing all related entities and their attributes, regardless of the provider.
        """
        pass

    @abstractmethod
    def get_environments_by_provider(self, provider: str) -> dict:
        """
        Returns all environments related to a specific provider.

        Each environment includes only the entities that belong to the specified provider.

        provider:
            str -> Name or ID of the data provider to filter environments by

        Returns:
            dict -> A dictionary of environments with their respective entities and attributes,
                    limited to those provided by the specified provider.
        """
        pass
