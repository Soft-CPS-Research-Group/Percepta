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
       
            