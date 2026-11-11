# --- Standard Library Imports ---
import copy
import itertools
import json
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.inference.ml_model_adapter_base import MLModelAdapterBase
from app.core.inference.decision_validation_correction_logic_base import DecisionValidationCorrectionLogicBase
from app.core.inference.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.output_handlers.output_handler_base import OutputHandlerBase
from app.core.storages.runtime_data_storage.runtime_data_storage_base import RuntimeDataStorageBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import format_timestamp, parse_timestamp


class InferenceService:
    """Application Service of the Inference container of ONE environment.

    Every time the unified message of a period arrives from Data Processing (the Input Handler calls
    'receive_message'), the service gets the decisions of the ML Model and sends them to the next container (Decision
    Dispatching):

        1. The Adapter maps the message into the format that the ML Model requires.
        2. The Output Handler (ML Model) sends it to the model and receives its decisions in return. If an error
           occurs, the Model Execution Error Handling Logic says if the inference is tried again or if the process
           ends. When it ends, the input of the model is still stored (without decisions), but the decisions are not
           validated, mapped or sent.
        3. The Decision Validation and Correction Logic, if there is one, validates the decisions and corrects them.
        4. The Runtime Data Storage, if there is one, stores the input of the model, its decisions and the corrected
           decisions (all in the format of the ML Model). If the inference failed, the decisions and the corrected
           decisions are None. If the storage fails, the error is logged and the service carries on, because the
           decisions must be dispatched anyway.
        5. The Adapter maps the decisions into the normalised format.
        6. The decisions in the normalised format are sent to the next container through the Output Handler.

    If something fails while a message is being processed (e.g., the Adapter raises an error), the error is logged and
    no decisions are sent for that period. The service is not stopped, so the next message is processed normally.

    NOTE The Output Handler (ML Model) must be an HTTP one: the service reads the response in the format that the HTTP
    Output Handler returns (see '_read_decisions').
    """

    _LOG_PREFIX: str = "Inference Service |"

    def __init__(
            self,
            adapter: MLModelAdapterBase,
            model_output_handler: OutputHandlerBase,
            error_handling_logic: ModelExecutionErrorHandlingLogicBase,
            validation_logic: Optional[DecisionValidationCorrectionLogicBase],
            runtime_data_storage: Optional[RuntimeDataStorageBase],
            output_handler: OutputHandlerBase,
            logger: LoggingUtils
    ) -> None:
        """Initializes the service.

        Args:
            adapter (MLModelAdapterBase): The Adapter of the ML Model.
            model_output_handler (OutputHandlerBase): The Output Handler used to communicate with the ML Model: it
                sends the mapped data and receives the decisions in return.
            error_handling_logic (ModelExecutionErrorHandlingLogicBase): The Model Execution Error Handling Logic.
            validation_logic (Optional[DecisionValidationCorrectionLogicBase]): The Decision Validation and Correction
                Logic, or None if the ML Model does not have one.
            runtime_data_storage (Optional[RuntimeDataStorageBase]): The Runtime Data Storage, or None if the instance
                does not store the data.
            output_handler (OutputHandlerBase): Sends the decisions to the next container (Decision Dispatching).
            logger (LoggingUtils): Logger of the environment.

        Raises:
            TypeError: If one of the components is not of the expected type.
        """
        self._logger = logger

        # --- Guard Clause 1: each component must be of the expected type ---
        # A wrong type would only fail later, in the middle of a period, so it is better to fail here.
        if not isinstance(adapter, MLModelAdapterBase):
            self._raise_type_error("'adapter' must extend MLModelAdapterBase.")
        if not isinstance(model_output_handler, OutputHandlerBase):
            self._raise_type_error("'model_output_handler' must extend OutputHandlerBase.")
        if not isinstance(error_handling_logic, ModelExecutionErrorHandlingLogicBase):
            self._raise_type_error("'error_handling_logic' must extend ModelExecutionErrorHandlingLogicBase.")
        if validation_logic is not None and not isinstance(validation_logic, DecisionValidationCorrectionLogicBase):
            self._raise_type_error("'validation_logic' must extend DecisionValidationCorrectionLogicBase.")
        if runtime_data_storage is not None and not isinstance(runtime_data_storage, RuntimeDataStorageBase):
            self._raise_type_error("'runtime_data_storage' must extend RuntimeDataStorageBase.")
        if not isinstance(output_handler, OutputHandlerBase):
            self._raise_type_error("'output_handler' must extend OutputHandlerBase.")

        self._adapter = adapter
        self._model_output_handler = model_output_handler
        self._error_handling_logic = error_handling_logic
        self._validation_logic = validation_logic
        self._runtime_data_storage = runtime_data_storage
        self._output_handler = output_handler

        # Gives an id to the messages that have no 'period_end', so the errors of a period can still be told apart
        self._message_counter = itertools.count(1)

    async def receive_message(self, envelope: Dict[str, Any]) -> None:
        """Gets the decisions for the unified message of a period and sends them to the next container.

        The Input Handler calls this for every message that arrives from Data Processing.

        Expected Input Structure:
        -------------------------
        envelope = {
            "protocol": "AMQP",
            "source": "exchange/queue",
            "status": None,
            "content_type": "application/json",
            "metadata": {},
            "payload": {                                    # The unified message, or the same message as JSON text/bytes
                "period_start": "2026-08-19 11:00:00 +0000",
                "period_end": "2026-08-19 11:15:00 +0000",
                "entities": {...},
                ...                                         # The structure depends on the Combination Logic
            }
        }

        Args:
            envelope (Dict[str, Any]): The message, as given by the Input Handler.
        """
        message = self._read_message(envelope)

        # Guard Clause 1: A message that cannot be read is skipped.
        if message is None:
            return

        period_id: str = self._period_id(message)

        try:
            await self._infer(message, period_id)
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not process the period '{period_id}', so no decisions were sent: "
                f"{type(err).__name__}: {err}"
            )

    async def _infer(self, message: Dict[str, Any], period_id: str) -> None:
        """Runs the steps of the class docstring for the message of a period."""
        model_input = self._adapter.adapt_model_input(message)

        decisions = await self._request_decisions(model_input, period_id)
        self._logger.info(
            f"{self._LOG_PREFIX} Decisions of the ML Model for the period '{period_id}': "
            f"\n{json.dumps(decisions, default=str, indent=2)}"
        )

        # Guard Clause 1: When the Model Execution Error Handling Logic ends the process, there is nothing to validate
        # or send, but the period is still stored (with the input of the model and without decisions), so that the
        # failure is not lost.
        if decisions is None:
            self._logger.error(f"{self._LOG_PREFIX} No decisions for the period '{period_id}'.")
            await self._store(message, model_input, None, None, period_id)
            return

        # The logic receives a copy, because it may change the decisions it is given, and the storage keeps both the
        # decisions of the model and the corrected ones.
        corrected_decisions: Optional[Any] = None
        if self._validation_logic is not None:
            corrected_decisions = self._validation_logic.validate_and_correct(copy.deepcopy(decisions))

        await self._store(message, model_input, decisions, corrected_decisions, period_id)

        # The decisions that go on are the corrected ones, if there are any (they replace the ones of the model).
        normalised_decisions: List[Dict[str, Any]] = self._adapter.adapt_model_decisions(
            decisions if corrected_decisions is None else corrected_decisions
        )

        # Guard Clause 2: Without decisions there is nothing to send.
        if not normalised_decisions:
            self._logger.warning(f"{self._LOG_PREFIX} No decisions to send for the period '{period_id}'.")
            return

        self._logger.info(
            f"{self._LOG_PREFIX} Normalised decisions of the period '{period_id}': "
            f"\n{json.dumps(normalised_decisions, default=str, indent=2)}"
        )
        try:
            await self._output_handler.send(normalised_decisions)
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not send the decisions of the period '{period_id}': "
                f"{type(err).__name__}: {err}"
            )

    async def _store(
            self, message: Dict[str, Any], model_input: Any, decisions: Any, corrected_decisions: Optional[Any],
            period_id: str
    ) -> None:
        """Stores the data of the inference in the Runtime Data Storage, if there is one.

        A failure of the storage is logged and does not stop the service: the decisions must be dispatched anyway.

        Args:
            message (Dict[str, Any]): The unified message, whose 'period_end' is the 'timestamp' of the record.
            model_input (Any): The input of the ML Model.
            decisions (Any): The decisions of the ML Model, before any correction. None if the inference failed.
            corrected_decisions (Optional[Any]): The decisions after the correction, or None if there is no
                Decision Validation and Correction Logic or the inference failed.
            period_id (str): Identifies the period (only used in the logs).
        """
        # Guard Clause 1: Without a storage there is nothing to store.
        if self._runtime_data_storage is None:
            return

        try:
            await self._runtime_data_storage.store({
                "model_inputs": model_input,
                "decisions": decisions,
                "corrected_decisions": corrected_decisions,
                "timestamp": self._record_timestamp(message)
            })
        except Exception as err:
            self._logger.error(
                f"{self._LOG_PREFIX} Could not store the data of the period '{period_id}': "
                f"{type(err).__name__}: {err}"
            )

    @staticmethod
    def _record_timestamp(message: Dict[str, Any]) -> str:
        """The 'period_end' of the message if it is valid, otherwise the current time (UTC)."""
        timestamp = message.get("period_end")
        try:
            parse_timestamp(timestamp)
            return timestamp
        except ValueError:
            return format_timestamp(datetime.now(timezone.utc))

    async def _request_decisions(self, model_input: Any, period_id: str) -> Optional[Any]:
        """Sends the data to the ML Model and gets its decisions, trying again as the error handling logic says.

        Args:
            model_input (Any): The data, in the format that the ML Model requires.
            period_id (str): Identifies the period, for the error handling logic.

        Returns:
            Optional[Any]: The decisions, in the format of the ML Model. None if the inference failed and the Model
                Execution Error Handling Logic ended the process.
        """
        while True:
            try:
                response = await self._model_output_handler.send(model_input)
                return self._read_decisions(response)
            except Exception as err:
                if not await self._error_handling_logic.should_retry(err, period_id):
                    return None

    @staticmethod
    def _read_decisions(response: Any) -> Any:
        """Gets the decisions from the response of the HTTP Output Handler.

        The HTTP Output Handler does not raise errors: it returns the result of each endpoint, so this method turns the
        results that are errors into an exception for the error handling logic.

        Expected Input Structure:
        -------------------------
        response = {
            "/inference": {                                 # The result of an endpoint, by its path
                "status": "success",                        # "success" or "error"
                "code": 200,                                # The HTTP status code
                "data": {"AC000001_1": 3}                   # The response of the ML Model
            }
        }
        # On error: {"/inference": {"status": "error", "error": "Connection refused"}}

        Expected Output Structure:
        --------------------------
        {"AC000001_1": 3}                                   # The 'data' of the endpoint

        Args:
            response (Any): What the Output Handler (ML Model) returned.

        Returns:
            Any: The 'data' of the first endpoint.

        Raises:
            RuntimeError: If there is no response, or the endpoint returned an error or a status code that is not 2xx.
        """
        # Guard Clause 1: Checks that there is a response.
        if not isinstance(response, dict) or not response:
            raise RuntimeError(f"The ML Model returned no response ({response!r}).")

        result = next(iter(response.values()))

        # Guard Clause 2: Checks that the result of the endpoint is a success.
        if not isinstance(result, dict) or result.get("status") != "success":
            error = result.get("error") if isinstance(result, dict) else result
            raise RuntimeError(f"The request to the ML Model failed: {error}")

        # Guard Clause 3: Checks that the status code is 2xx.
        code = result.get("code")
        if isinstance(code, int) and not 200 <= code < 300:
            raise RuntimeError(f"The ML Model returned the status code {code}: {result.get('data')}")

        return result.get("data")

    def _read_message(self, envelope: Any) -> Optional[Dict[str, Any]]:
        """Gets the unified message from the envelope that the Input Handler delivered.

        The payload is a dictionary when the message came from the same process (in-memory connector), and JSON, as
        text or bytes, when it came from the network (e.g., RabbitMQ). Both are accepted. The structure of the message
        is not checked, because it depends on the instance.

        Args:
            envelope (Any): The message, as given by the Input Handler.

        Returns:
            Optional[Dict[str, Any]]: The unified message, or None (with a warning in the log) if it cannot be read.
        """
        payload = envelope.get("payload") if isinstance(envelope, dict) else None

        if isinstance(payload, (bytes, str)):
            try:
                payload = json.loads(payload)
            except ValueError as err:
                self._logger.warning(f"{self._LOG_PREFIX} Skipping a message that is not valid JSON: {err}")
                return None

        # Guard Clause 1: The unified message is a dictionary.
        if not isinstance(payload, dict):
            self._logger.warning(
                f"{self._LOG_PREFIX} Skipping a message whose payload is not a dictionary, got "
                f"{type(payload).__name__}."
            )
            return None

        return payload

    def _period_id(self, message: Dict[str, Any]) -> str:
        """Identifies the period of a message: its 'period_end' (the end of the period), or a number if it has none."""
        timestamp = message.get("period_end")
        if isinstance(timestamp, str) and timestamp.strip():
            return timestamp
        return f"message-{next(self._message_counter)}"

    def _raise_type_error(self, detail: str) -> None:
        """Logs a wrong type of component and raises it as a TypeError."""
        error_msg = f"{self._LOG_PREFIX} Initialization error: {detail}"
        self._logger.error(error_msg)
        raise TypeError(error_msg)
