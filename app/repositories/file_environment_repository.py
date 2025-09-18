import os
import json
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.utils.logger import LoggingUtils


class FileEnvironmentRepository(EnvironmentRepository):
    """
    Environment repository implementation that loads and manages environment configurations from JSON files.

    This class reads JSON files from a specified folder, organizes environments by provider,
    and merges them into a unified dictionary.
    """

    _all_environments : dict            # Dictionary storing all loaded environments, merging data from all providers.
    _environments_by_provider : dict    # Dictionary storing environments separated by provider.
    _logger: LoggingUtils               # Logger instance for structured logging

    def __init__(self, configurations: dict, logger : LoggingUtils) -> None:
        """
        Initializes the repository and processes JSON files in the specified folder.

        Args:
            configurations (dict): Configuration dictionary, must include "environment_files" path.
            logger: Logger object for logging events.
        """
        self._all_environments = {}
        self._environments_by_provider = {}
        self._logger = logger

        # Process all JSON files in the folder specified in configurations
        self.process_json_files_in_folder(configurations.get("environment_files"))

    def get_environments(self) -> dict:
        """
        Returns all loaded environments, merging data from all providers.
        """
        return self._all_environments

    def get_environments_by_provider(self, provider: str) -> dict:
        """
        Returns environments loaded for a specific provider.

        Args:
            provider (str): Name of the provider.
        """
        return self._environments_by_provider.get(provider)

    def process_json_files_in_folder(self, folder_path: str) -> None:
        """
        Processes all JSON files in a given folder.

        Args:
            folder_path (str): Path to the folder containing JSON files.
        """
        # List all files in the folder
        files: list = os.listdir(folder_path)

        # Filter only JSON files
        json_files: list = [file for file in files if file.endswith('.json')]

        # Process each JSON file
        for json_file in json_files:
            file_path: str = os.path.join(folder_path, json_file)

            # Read the JSON file content
            schema: dict = self._read_repo_file(file_path)

            # Extract and remove the 'provider' key from the schema
            provider: str = schema.pop('provider')

            # Store schema by provider
            self._environments_by_provider[provider] = schema

            # Process the environment data and store it
            self._house_identifier(schema, provider)

    def _house_identifier(self, schema: dict, provider: str) -> None:
        """
        Processes and stores environments from a provider, adding additional metadata.

        Args:
            schema (dict): Dictionary containing environment definitions for a provider.
            provider (str): Provider name associated with this schema.
        """
        for key, value in schema.items():
            # Assign the provider to each device entity
            for device in value['entities'].values():
                device['provider'] = provider
            # Replace spaces in group names with underscores
            value['group'] = value['group'].replace(' ', '_')
            # Merge or assign processed value to the main dictionary
            if key in self._all_environments:
                self._all_environments[key].update(value)
            else:
                self._all_environments[key] = value

    @staticmethod
    def _read_repo_file(filepath: str, **kwargs) -> dict:
        """
        Reads a JSON file and returns its content as a dictionary.

        Args:
            filepath (str): Path to the JSON file to read.
            **kwargs: Additional keyword arguments to pass to json.load().

        Returns:
            dict: Parsed JSON content from the file.
        """
        with open(filepath) as f:
            json_file: dict = json.load(f, **kwargs)
        return json_file
