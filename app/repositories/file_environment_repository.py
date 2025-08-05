import os
import json
from app.repositories.irepositories.environment_repository import EnvironmentRepository


class FileEnvironmentRepository(EnvironmentRepository):
    def __init__(self, configurations, logger):
        # Dictionary to store all environments loaded from JSON files
        self._all_environments = {}

        self._environments_by_provider = {}

        self._logger = logger

        # Process all JSON files in the specified folder
        self.process_json_files_in_folder(configurations.get("environment_files"))

    def get_environments(self) -> dict:
        # Return the specifications of a specific environment by its ID (merging all the providers' data for the same environment)
        return self._all_environments

    def get_environments_by_provider(self, provider: str) -> dict:
        return self._environments_by_provider[provider]

    def process_json_files_in_folder(self, folder_path):
        # List all files in the specified folder
        files = os.listdir(folder_path)

        # Filter only JSON files
        json_files = [file for file in files if file.endswith('.json')]

        # Process each JSON file
        for json_file in json_files:
            file_path = os.path.join(folder_path, json_file)

            # Read the content of the JSON file
            schema = FileEnvironmentRepository._read_file(file_path)
            # Extract and remove the provider key from the schema
            provider = schema.pop('provider')

            self._environments_by_provider[provider] = schema

            # Process the environment data and store it
            FileEnvironmentRepository._house_identifier(self._all_environments, schema, provider)

    @staticmethod
    def _read_file(filepath: str, **kwargs):
        # Open the specified file in read mode
        with open(filepath) as f:
            # Load the JSON content from the file into a Python dictionary
            # Pass any additional keyword arguments to the json.load() function
            json_file = json.load(f, **kwargs)

        # Return the parsed JSON content
        return json_file

    @staticmethod
    def _house_identifier(dic, schema, provider):
        # Iterate through each key-value pair in the schema
        for key, value in schema.items():
            # Assign the provider to each device entity
            for device in value['entities'].values():
                device['provider'] = provider
            # Replace spaces in group name with underscores
            value['group'] = value['group'].replace(' ', '_')
            # Merge or assign the processed value to the dictionary
            if key in dic:
                dic[key].extend(value)
            else:
                dic[key] = value
