from collections import OrderedDict
import os
import sys
import pandas as pd
from training.Translator import Translator
from utils.data import DataSet

# Load configurations
configurations = DataSet.get_schema('./configs/historicConfigurations.json')


class ICHistoricDataTranslator(Translator):
    @staticmethod
    def translate(house, devices, data, start_date, end_date, period):
        print(f'Translating historic data for house {house}...')
        dataById = {}
        messageIC = configurations['messageIC']
        for entry in data:
            for device in devices:
                for pm in messageIC.keys():
                    print(f'{pm} in {entry} and {device} \n')
                    if pm in entry and device.get('label') == messageIC[pm]:
                        deviceId = device.get('id')
                        dici = {'Date': entry.get('time'), 'Value': entry.get(pm)}
                        if deviceId not in dataById:
                            dataById[deviceId] = [dici]
                        else:
                            dataById[deviceId].append(dici)
        print(dataById.keys())
        
        for deviceId in dataById.keys():
            df = ICHistoricDataTranslator._data_format(dataById[deviceId], period, start_date, end_date, ['Date', 'Value'])
            tosend = ICHistoricDataTranslator._interpolateMissingValues(df)
            tosend = {date: value for date, value in tosend.items() if start_date <= pd.to_datetime(date) <= end_date}
            tosend = OrderedDict(sorted(tosend.items()))
            ICHistoricDataTranslator._send(house, deviceId, tosend)
            directory = 'devicesAndTags'
            if not os.path.exists(directory):
                os.makedirs(directory)
            filename = os.path.join(directory, f"{deviceId}.csv")
            ICHistoricDataTranslator._tocsv(filename, tosend, ['Date', 'Value'])


    
     
        