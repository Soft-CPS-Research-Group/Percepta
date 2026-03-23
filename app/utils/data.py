import simplejson as json
import os

class DataSet:

    @staticmethod
    def get_schema(filepath: str) -> dict:
        return DataSet.read_json(filepath)

    @staticmethod
    def read_json(filepath: str, **kwargs):
        """Return json document as dictionary.

        Parameters
        ----------
        filepath : str
        pathname of JSON document.

        Other Parameters
        ----------------
        **kwargs : dict
            Other infrequently used keyword arguments to be parsed to `simplejson.load`.

        Returns
        -------
        dict
            JSON document converted to dictionary.
        """

        with open(filepath) as f:
            json_file = json.load(f,**kwargs)

        return json_file


    @staticmethod
    def lists_concat(data:dict) -> list:
        consolidated_list = []
    
        for key in data.keys():
            if "house" in key:
                consolidated_list.extend(data[key])
        
        return consolidated_list

    @staticmethod
    def process_json_files_in_folder(folder_path, housesDic):
        files = os.listdir(folder_path)
        
        json_files = [file for file in files if file.endswith('.json')]
        
        for json_file in json_files:
            file_path = os.path.join(folder_path, json_file)

            schema = DataSet.get_schema(file_path)
            provider = schema.pop('provider')
            DataSet.house_identifier(housesDic, schema, provider)

    @staticmethod
    def house_identifier(dic, schema, provider) :
        for key, value in schema.items():
            for device in value['entities'].values():
                device['provider'] = provider
            value['site'] = value['site'].replace(' ', '_')
            if key in dic:
                dic[key].extend(value)
            else:
                dic[key] = value
                    
    @staticmethod
    def calculate_interval (schema) -> int:
        value = schema.get('value')
        unit = schema.get('unit')
        if unit == 'days':
           return value * 24 * 60 * 60
        elif unit == 'hours':
           return value * 60 * 60
        elif unit == 'minutes':
           return value * 60
        else:
           return value

    @staticmethod
    def get_cron_expressions(total_seconds: int) -> dict:
        """
        Converts total seconds into an APScheduler Cron parameter dictionary.
        Ensures the job runs at the most appropriate time unit to avoid 60-second limit issues.
        """
        if total_seconds <= 0:
            raise ValueError("Time interval must be greater than zero.")

        # Scenario 1: Interval is less than 1 minute
        if total_seconds < 60:
            return {"second": f"*/{total_seconds}"}

        # Scenario 2: Interval is exactly in minutes (e.g., 120s, 300s)
        if total_seconds % 60 == 0:
            total_minutes = total_seconds // 60

            # If it fits within an hour (e.g., every 2 or 45 minutes)
            if total_minutes < 60:
                return {"minute": f"*/{total_minutes}", "second": "0"}

            # Scenario 3: Interval is exactly in hours (e.g., 3600s, 7200s)
            if total_minutes % 60 == 0:
                return {"hour": f"*/{total_minutes // 60}", "minute": "0", "second": "0"}

            # Fallback for large minute intervals that aren't exact hours
            return {"minute": f"*/{total_minutes}", "second": "0"}

        # Fallback: For "uneven" intervals (e.g., 90s), cron might align poorly.
        # It will trigger at second 0 and 30 of every minute.
        return {"second": f"*/{total_seconds}"}