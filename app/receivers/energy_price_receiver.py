import datetime
from app.translators.energy_price_translator import EnergyPriceTranslator
from app.utils.ren_data import ElectricityPriceFetcher
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider


class EnergyPriceReceiver(ReceiverHTTPBase):
    provider = Provider.REN.value     # Provider ID

    _translator: EnergyPriceTranslator   # Translator which translates EnergyPrice-specific format into Percepta-specific format

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the EnergyPriceReceiver instance.

        Args:
            environment (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        self._time_interval = 3600

        self._translator = EnergyPriceTranslator(environment, environment_specs, configurations, logger)

    def stop(self):
        """
        Stops the receiver and gracefully stops the Electricity Price Fetcher.
        """
        self._logger.info(f"Stopping thread {self._environment}...")
        super().stop()

        ElectricityPriceFetcher.stop_electricity_price_fetcher_service()

    def _job(self):
        """
        Executes the main data retrieval job:
            - Ensures a valid session.
            - Retrieves raw data for all configured entities and parameters in parallel.
            - Passes collected data to CWTranslator after all requests complete.
        """
        utc_hour = datetime.datetime.now(datetime.timezone.utc).hour

        self._translator.translate(
            {
                "value" : ElectricityPriceFetcher.get_price(utc_hour),
                "entity_id" : "EP"
            })


    # TODO: Verificar se isto é a melhor alternativa
    @classmethod
    def pre_start(cls, configurations : dict, logger : LoggingUtils):
        """
        Performs pre-start initialization for the receiver by starting the
        electricity price fetcher.

        Args:
            logger (LoggingUtils): Logger instance.
            configurations (dict): General configurations for CWSession.
        """
        ElectricityPriceFetcher.start_electricity_price_fetcher_service(logger, configurations)
