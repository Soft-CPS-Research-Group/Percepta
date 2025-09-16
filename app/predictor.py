import random
from pydoc import locate

class Predictor():
    # TODO meter tipos
    def __init__(self, environment, environment_specs, time_series_repository, forwarders, configurations, logger):
        self._providers = configurations.get('Providers')
        self._entities = environment_specs["entities"]
        self._group = environment_specs["group"]
        self._environment = environment
        self._forwarders = forwarders
        self._logger = logger
        self._time_series_repository = time_series_repository

    def predict(self, message):
        result = 0
        self._energaize_simulator(message)
        self._forwarder(result)
        self._save_data(message, result)
        
    def _energaize_simulator(self, message):
        pass

    def _forwarder(self,result):
        pass

    def _save_data(self,message,result):
       # TODO CHAMAR AQUI QUALQUER COISA
        return 0