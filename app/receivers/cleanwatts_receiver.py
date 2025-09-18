from app.translators.cw_translator import CWTranslator
from app.utils.cwlogin import CWSession
from app.receivers.receiver_http_base import ReceiverHTTPBase
from app.utils.logger import LoggingUtils
from app.utils.providers import Provider

class CWReceiver(ReceiverHTTPBase):
    """
    CWReceiver is responsible for retrieving raw data from the Cleanwatts API
    for configured entities and parameters, and maintaining session management
    using CWSession.

    Note:
        Translation of provider-specific data into Percepta-specific format
        is handled by CWTranslator.
    """
    provider = Provider.CLEANWATTS.value     # Provider ID

    _translator: CWTranslator   # Translator which translates Cleanwatts-specific format into Percepta-specific format

    def __init__(self, environment: str, environment_specs: dict, configurations: dict, logger: LoggingUtils):
        """
        Initializes the CWReceiver instance.

        Args:
            environment (str): Name of the environment the receiver will operate in.
            environment_specs (dict): Specifications for the environment, including entities.
            configurations (dict): General configurations for the receiver, e.g., max reconnect attempts, frequency.
            logger (LoggingUtils): Logger instance for structured logging.
        """
        super().__init__(environment, environment_specs, configurations, logger)

        # Translator instance is created
        self._translator = CWTranslator(environment, configurations, logger)

    def stop(self):
        """
        Stops the receiver and gracefully stops the Cleanwatts token refresher.
        """
        self._logger.info(f"Stopping thread {self._environment}...")
        super().stop()

        #TODO não faz sentido isto estar aqui!
        CWSession.stop_token_refresher()

    def _job(self):
        """
        Executes the main data retrieval job:
            - Ensures a valid session.
            - Retrieves raw data for all configured entities and parameters.
            - Passes collected data to CWTranslator (does not perform translation itself).
        """
        try:
            self._header_updater()

            for entity_id, values in self._entities.items():
                all_entity_parameter_data = {}

                for param_name, param_attr in values.get('parameters', {}).items():
                    if param_attr:
                        tag_id = param_attr.get('id')

                        # Make the GET request with specified timeout
                        data = self.retrieve_data(f"{self._server.get('resources').get('data')}{tag_id}")

                        if data:
                            self._logger.info(f"Tag {tag_id} successfully retrieved!")
                            all_entity_parameter_data.update({param_name: data})
                        else:
                            # Data is empty, treat as a failure
                            self._logger.warning(f"Tag {tag_id} returned empty data.")
                            all_entity_parameter_data.update({param_name: []})

                # Pass raw data to translator; translation is not performed here

                self._translator.translate({'entity_id' : entity_id, 'label' : values.get('label'), 'parameters' : all_entity_parameter_data})

        except Exception as e:
            self._logger.error(e)

    def _header_updater(self):
        """
        Sets the authorization header using the current CWSession token.

        """
        token = CWSession.get_token()

        if token is None:
            raise RuntimeError(f"Token is None.")

        self._header = {'Authorization': f"CW {token}"}

    # TODO: Verificar se isto é a melhor alternativa
    @classmethod
    def pre_start(cls, configurations : dict, logger : LoggingUtils):
        """
        Performs pre-start initialization for the receiver by starting the
        CWSession token refresher.

        Args:
            logger (LoggingUtils): Logger instance.
            configurations (dict): General configurations for CWSession.
        """
        CWSession.start_token_refresher(logger, configurations)
