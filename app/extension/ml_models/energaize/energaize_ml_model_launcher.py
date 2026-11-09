# --- Internal Framework Imports ---
from app.core.inference.decision_validation_correction_logic_base import (
    DecisionValidationCorrectionLogicBase
)
from app.core.inference.model_execution_error_handling_logic_base import ModelExecutionErrorHandlingLogicBase
from app.core.inference.ml_model_launcher_base import (
    MLModelComponents,
    MLModelLauncherBase
)
from app.core.utils.labels import OutputHandlerLabel
from app.core.utils.logger import LoggingUtils
from app.extension.ml_models.energaize.energaize_ml_model_adapter import EnergAIzeMLModelAdapter
from app.extension.ml_models.energaize.energaize_model_execution_error_handling_logic import (
    EnergAIzeModelExecutionErrorHandlingLogic
)


class EnergAIzeMLModelLauncher(MLModelLauncherBase):
    """Launcher of the EnergAIze ML Model (reached over HTTP).

    Each environment has its own EnergAIze model, and each model has its own entry in the Percepta Instance
    Configuration, with its own connection configuration. The way to reach them is the same, so they all use this
    launcher.

    For each environment, it returns the EnergAIze Adapter, the HTTP Output Handler that sends the data to the model and
    receives the decisions, and the two logics of the model, which it reads from the entry of the model in the Percepta
    Instance Configuration:
    - 'model_execution_error_handling_logic': if the entry has none, the EnergAIze one is used, which is the same for
      all EnergAIze instances;
    - 'decision_validation_correction_logic': it is optional and specific to each installation (it has its safety
      limits), so if the entry has none, there is no validation and the decisions go on as the model produced them.
    """

    _LOG_PREFIX: str = "EnergAIze ML Model Launcher |"

    async def launch(
        self,
        ml_model_id: str,
        ml_model_configuration: dict,
        environment_name: str,
        environment: dict,
        logger: LoggingUtils
    ) -> MLModelComponents:
        """Prepares the EnergAIze Adapter, Output Handler and logic for one environment.

        Expected `ml_model_configuration` Data Structure (the entry of the model):
        --------------------------------------------------------------------------
        {
            "path": "app.extension.ml_models.energaize.energaize_ml_model_launcher.EnergAIzeMLModelLauncher",
            "connection_configuration": {
                "protocol_id": "AsyncHTTP",                 # Output Handler type
                "connection_info": {
                    "connection": {
                        "url": "http://localhost:8000"
                    },
                    "topology": {
                        "endpoints": [
                            {
                                "path": "/inference",
                                "method": "POST",
                                "timeout_ms": 5000,
                                "headers": {"Content-Type": "application/json"},
                                "rpc_properties": {"enabled": True}     # The decisions come in the response
                            }
                        ]
                    }
                }
            },
            "model_execution_error_handling_logic": {               # (dict, optional)
                "path": "app.extension.ml_models.energaize.energaize_model_execution_error_handling_logic."
                        "EnergAIzeModelExecutionErrorHandlingLogic"
            },
            "decision_validation_correction_logic": {               # (dict, optional)
                "path": "app.extension.ml_models.energaize.my_validation.MyValidation"
            }
        }

        Args:
            ml_model_id (str): Id of the ML Model.
            ml_model_configuration (dict): The entry of the EnergAIze model, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-01").
            environment (dict): The environment, as returned by the Schema Storage.
            logger (LoggingUtils): Logger of the environment, given to the Adapter, the Output Handler and the logic, so
                their logs show the environment.

        Returns:
            MLModelComponents: The Adapter, the Output Handler (already connected), the Model Execution Error Handling
                Logic and the Decision Validation and Correction Logic (None if the entry has none).

        Raises:
            ValueError: If the configuration of the model is invalid, or a logic cannot be created.
        """
        connection_configuration = ml_model_configuration.get("connection_configuration")

        # Guard Clause 0: Checks that the entry has a connection configuration.
        if not isinstance(connection_configuration, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_configuration' is missing for ML model '{ml_model_id}'."
            )

        protocol_id = connection_configuration.get("protocol_id")
        connection_info = connection_configuration.get("connection_info")

        # Guard Clause 1: Checks that the protocol is HTTP, which is how EnergAIze is reached.
        if protocol_id != OutputHandlerLabel.ASYNC_HTTP.value:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: the 'protocol_id' of ML model '{ml_model_id}' must be "
                f"'{OutputHandlerLabel.ASYNC_HTTP.value}', and it is {protocol_id!r}."
            )

        # Guard Clause 2: Checks that the connection_info and its connection exist.
        if not isinstance(connection_info, dict) or not isinstance(connection_info.get("connection"), dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.connection' is missing for "
                f"ML model '{ml_model_id}'."
            )

        topology = connection_info.get("topology")
        endpoints = topology.get("endpoints") if isinstance(topology, dict) else None

        # Guard Clause 3: Checks that there is at least one endpoint.
        if not isinstance(endpoints, list) or not endpoints:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info.topology.endpoints' must be a non-empty list "
                f"for ML model '{ml_model_id}'."
            )

        # Guard Clause 4: Checks that every endpoint has a path and returns the response, because the decisions of
        # EnergAIze are in the response of the request.
        for endpoint in endpoints:
            if not isinstance(endpoint, dict) or not isinstance(endpoint.get("path"), str) \
                    or not endpoint["path"].strip():
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: every endpoint of ML model '{ml_model_id}' must be a "
                    f"dictionary with a 'path'."
                )
            if not endpoint.get("rpc_properties", {}).get("enabled", False):
                raise ValueError(
                    f"{self._LOG_PREFIX} Launch error: the endpoint '{endpoint['path']}' of ML model "
                    f"'{ml_model_id}' must have 'rpc_properties.enabled' set to true, because the decisions are in "
                    f"the response."
                )

        adapter = EnergAIzeMLModelAdapter(environment_name, environment, logger)

        # The logics come from the entry of the model. The error handling one is the same for all EnergAIze instances,
        # so it is the default if the entry has none; the validation one is only used if the entry has one.
        error_handling_logic = self._build_logic(
            ml_model_configuration, "model_execution_error_handling_logic",
            ModelExecutionErrorHandlingLogicBase, environment, logger
        ) or EnergAIzeModelExecutionErrorHandlingLogic(environment, logger)
        validation_logic = self._build_logic(
            ml_model_configuration, "decision_validation_correction_logic",
            DecisionValidationCorrectionLogicBase, environment, logger
        )

        output_handler, _ = await self._output_launcher.build_output_handler(
            handler_type=protocol_id,
            topology_setup=topology,
            server_configurations=connection_info["connection"],
            logger=logger
        )

        return MLModelComponents(
            adapter=adapter,
            output_handler=output_handler,
            error_handling_logic=error_handling_logic,
            validation_logic=validation_logic
        )
