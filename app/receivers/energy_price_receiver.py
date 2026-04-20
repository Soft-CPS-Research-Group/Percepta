import datetime
import time
import threading
from app.translators.energy_price_translator import EnergyPriceTranslator
from app.utils.ren_data import ElectricityPriceFetcher
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider


class EnergyPriceReceiver(ReceiverHTTPBase):
    provider = Provider.REN.value     # Provider ID

    _translator: EnergyPriceTranslator   # Translator which translates EnergyPrice-specific format into Percepta-specific format
    _ren_data_init : bool = False
    _ren_data_lock = threading.Lock()

    def __init__(self, environment_name: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the EnergyPriceReceiver instance.

        Args:
            environment_name (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment_name, environment_specs, configurations, logger)
        self._original_time_interval = self._time_interval

        self._time_interval = 15*60

        self._translator = EnergyPriceTranslator(environment_name, environment_specs, configurations, logger)

        #time.sleep(2) # TODO dar tempo para que os preços atualizem do outro lado, verificar se é a melhor alternativa
        #self._job()

    def stop(self):
        """
        Stops the receiver and gracefully stops the Electricity Price Fetcher.
        """
        self._logger.info(f"Stopping thread {self._environment_name}...")
        super().stop()

        with type(self)._ren_data_lock:
            if type(self)._ren_data_init:
                ElectricityPriceFetcher.stop_electricity_price_fetcher_service()
                type(self)._ren_data_init = False

    def _job(self):
        """
        Executes the main data retrieval job:
            - Ensures a valid session.
            - Retrieves raw data for all configured entities and parameters in parallel.
            - Passes collected data to CWTranslator after all requests complete.
        """

        time.sleep(self._original_time_interval) # TODO isto está um bocado remendado, ver qual a melhor forma para tratar isto
        #self._logger.info("Energy Price called")

        message_to_translate = {
                "value" : ElectricityPriceFetcher.get_future_prices(),
                "entity_id" : "OMIE" # TODO arranjar maneira de ir buscar as configurações o nome
            }

        self._translator.translate(message_to_translate)

    @classmethod
    def launch(cls, environments : dict, configurations : dict):
        threads = []

        logger_energy_price_fetcher = LoggingUtils(f"{cls.provider}_energy_price_fetcher", configurations)

        with cls._ren_data_lock:
            if not cls._ren_data_init:
                energy_price_fetcher = threading.Thread(
                    target=ElectricityPriceFetcher.start_electricity_price_fetcher_service,
                    args=(logger_energy_price_fetcher, configurations),
                    daemon=True # TODO considerar se daemon fica a True ou False
                )

                try:
                    energy_price_fetcher.start()
                    cls._ren_data_init = True
                    threads.append(energy_price_fetcher)
                except Exception:
                    raise Exception("Failed to start energy price fetcher") # TODO ver o que faço com isto

        for environment, environment_specs in environments.items():
            logger_per_environment = LoggingUtils(f"{cls.provider}_receiver", configurations, environment)
            receiver = cls(environment, environment_specs, configurations, logger_per_environment)
            receiver.start()
            threads.append(receiver)

        return threads