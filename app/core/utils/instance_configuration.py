# --- Standard Library Imports ---
import importlib
from enum import Enum
from typing import Optional, Type

# --- Internal Framework Imports ---
from app.core.data_processing.combination_logic_base import CombinationLogicBase
from app.core.data_processing.data_derivation_logic_base import DataDerivationLogicBase
from app.core.utils.environment_variables import expand_environment_variables
from app.core.utils.json_reader import read_json_object
from app.core.utils.labels import (
    DataIssueHandlingApproachLabel, InputHandlerLabel, OutputHandlerLabel, RuntimeDataStorageLabel, SchemaStorageLabel
)


class InstanceConfiguration:
    """The Percepta Instance Configuration, with its general format validated.

    The configuration is validated once, here, when it is read. The containers and launchers that use it receive this
    object, so they do not need to check its format again, and they get each part through a property.

    This class only validates the GENERAL format: that each part exists, is of the right type, and that each
    connection configuration has its id and its 'connection_info'. It does NOT validate what is inside the
    'connection_info' (e.g., the connection settings or the topology of RabbitMQ). That belongs to who uses it: the
    Input Handlers, the Output Handlers and the Storages validate their own 'connection_info'.

    A wrong value is reported when the application starts, and not later, when the component is built. Besides the
    format, two things are checked:

    1. The ids exist in the labels of Percepta (see app/core/utils/labels.py):
       - 'schema_storage_type_id' must be a SchemaStorageLabel;
       - 'runtime_data_storage_type_id' must be a RuntimeDataStorageLabel;
       - the 'protocol_id' of a Telemetry System must be an InputHandlerLabel (the type of its Input Handler);
       - the 'protocol_id' of a Target System, of a Machine Learning model, and of the communication from Data
         Ingestion to Data Processing, must be an OutputHandlerLabel (the type of the Output Handler that sends the
         data).

    2. The launchers and the integrated logic exist and are of the right type. The 'path' of each Telemetry System,
       Target System and Machine Learning model, and each path of the 'integrated_logic', must lead to a class that
       exists (the module can be imported and it has the class), and that class must extend the base class of its
       type. The modules are imported here only to check them.

    3. The times make sense. The 'frequency' must evenly divide the hour (e.g., 15 seconds, 5 minutes, 1 hour), so that
       the data is always processed at fixed points in time (a 15-minute frequency processes at 00, 15, 30 and 45
       minutes past the hour). The 'grace_period', if it is longer than zero, must be shorter than the 'frequency'.
       The ids in 'data_issue_handling_approaches' must be DataIssueHandlingApproachLabel.

    The parts used by the Data Processing container ('grace_period', 'data_issue_handling_approaches' and
    'integrated_logic') are optional here, because the application still launches only the Data Ingestion container.
    TODO When the Data Processing container is launched, decide which of them are mandatory.

    Any value of the file that is only "${NAME}" (e.g., "password": "${CW_PASSWORD}") is replaced by the environment
    variable NAME when the file is read. The application does not start if the variable is not set.

    Expected Input Structure:
    -------------------------
    {
        "timezone": "UTC",                                          # (str, optional) Defaults to "UTC"
        "frequency": {"value": 15, "unit": "seconds"},              # Unit: "seconds", "minutes" or "hours"
        "grace_period": {"value": 2, "unit": "seconds"},            # (dict, optional) Zero or less means no grace
        "data_issue_handling_approaches": [                         # (list, optional) Applied in this order
            "missing_data", "duplicated_data", "out_of_order_data", "frequency_harmonisation"
        ],
        "integrated_logic": {                                       # (dict, optional) Each value is the path of a class
            "combination_logic": "app.extension.logic.combination.CombinationLogic1",
            "data_derivation_logic": "app.extension.logic.derivation.DerivationLogic1"     # (optional)
        },
        "schema_storage": {
            "connection_configuration": {
                "schema_storage_type_id": "json",                   # Id of a Schema Storage type known to Percepta
                "connection_info": {"file_path": "environment_files/environments.json"}
            }
        },
        "runtime_data_storage": {                                   # (dict, optional) Used by the Inference container
            "connection_configuration": {
                "runtime_data_storage_type_id": "mongodb",          # Id of a Runtime Data Storage type known to Percepta
                "connection_info": {"host": "localhost", "port": 27017}
            }
        },
        "external_systems": {
            "telemetry_systems": {
                "telemetry_system_1_id": {
                    "path": "app.extension.telemetry_systems.system_1.launcher_1.Launcher1",
                    "connection_configuration": {
                        "protocol_id": "AsyncHTTP",                 # Id of an Input Handler type known to Percepta
                        "connection_info": {}
                    }
                }
            },
            "target_systems": {                                     # (dict, optional)
                "target_system_1_id": {
                    "path": "app.extension.target_systems.system_1.launcher_1.Launcher1",
                    "connection_configuration": {"protocol_id": "AsyncHTTP", "connection_info": {}},
                    "actuation_error_handling_logic": {             # (dict, optional)
                        "path": "app.extension.target_systems.system_1.error_handling_1.ErrorHandling1"
                    }
                }
            },
            "ml_models": {                                          # (dict, optional)
                "ml_model_1_id": {
                    "path": "app.extension.ml_models.model_1.launcher_1.Launcher1",
                    "connection_configuration": {"protocol_id": "AsyncHTTP", "connection_info": {}},
                    "model_execution_error_handling_logic": {       # (dict, optional)
                        "path": "app.extension.ml_models.model_1.error_handling_1.ErrorHandling1"
                    },
                    "decision_validation_correction_logic": {       # (dict, optional)
                        "path": "app.extension.ml_models.model_1.validation_1.Validation1"
                    }
                }
            }
        },
        "inter_container_communication": {                          # (dict, optional)
            "data_ingestion_to_data_processing": {                  # (dict, optional)
                "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
            },
            "data_processing_to_inference": {                       # (dict, optional)
                "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
            },
            "inference_to_decision_dispatching": {                  # (dict, optional)
                "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
            },
            "data_processing_to_rec_bridge": {                      # (dict, optional) Only used with 'rec_bridge'
                "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
            },
            "rec_bridge_to_inference": {                            # (dict, optional) Only used with 'rec_bridge'
                "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
            }
        },
        "application_launcher": {                                   # (dict, optional) Which Application Launcher runs this instance
            "path": "app.core.launchers.application_launchers.until_rec_bridge_application_launcher.UntilRECBridgeApplicationLauncher"
        },                                                          # Default: the SingleProcessApplicationLauncher (all the containers)
        "rec_bridge": {                                             # (dict, optional) Adds the REC Bridge container
            "path": "app.extension.rec_bridge.energaize.energaize_rec_bridge_launcher.EnergAIzeRECBridgeLauncher",
            "connection_configuration": {                           # How the installations of the REC talk to each other
                "protocol_id": "AsyncRabbitMQ",                     # Only AsyncRabbitMQ
                "connection_info": {
                    "collection_window_seconds": 5,                 # How long it waits for the other installations. Shorter than the frequency
                    "connection": {},
                    "topology": {"publish": {}, "subscribe": {}}    # Exchanges and queues, with {community_id}, {installation_id} and {peer_id}
                }
            }
        },
        "logging": {                                                # (dict, optional)
            "console_level": "INFO",                                # (str, optional) Level of the console: DEBUG, INFO, WARNING, ERROR or CRITICAL. Defaults to INFO. The log files always keep DEBUG
            "log_files": {"max_size": 10485760}
        }
    }
    """

    _LOG_PREFIX: str = "Instance Configuration |"
    _DEFAULT_APPLICATION_LAUNCHER_PATH: str = (
        "app.core.launchers.application_launchers.single_process_application_launcher.SingleProcessApplicationLauncher"
    )
    _SECONDS_IN_AN_HOUR: int = 3600
    _SECONDS_PER_UNIT: dict = {"seconds": 1, "minutes": 60, "hours": 3600}
    # The types of integrated logic, and the base class that the class of each one must extend. The Target System of
    # each decision is not configured: it is given by the 'actuator_mapping' of the environment.
    _INTEGRATED_LOGIC_BASE_CLASSES: dict = {
        "combination_logic": CombinationLogicBase,
        "data_derivation_logic": DataDerivationLogicBase
    }

    def __init__(self, path: str) -> None:
        """Reads the configuration from a JSON file and validates its general format.

        Args:
            path (str): Path to the Percepta Instance Configuration (JSON), with the structure detailed above.

        Raises:
            FileNotFoundError: If the file does not exist.
            ValueError: If the file is not valid JSON, a mandatory part is missing, or a part has the wrong type.
        """
        try:
            # A value that is only "${NAME}" is replaced by the environment variable NAME, so that what must not be in
            # the file (e.g., a password) can be given outside of it
            content = expand_environment_variables(read_json_object(path))
        except (FileNotFoundError, ValueError) as err:
            # Keeps the type of the error and adds context
            raise type(err)(f"{self._LOG_PREFIX} Could not read the Percepta Instance Configuration: {err}") from err

        # --- Guard Clause 1: the configuration must not be empty ---
        if not content:
            raise ValueError(f"{self._LOG_PREFIX} The configuration in '{path}' is empty.")

        # --- Guard Clause 2: timezone is optional, but it must be a string if given ---
        timezone = content.get("timezone", "UTC")
        if not isinstance(timezone, str) or not timezone.strip():
            raise ValueError(f"{self._LOG_PREFIX} 'timezone' must be a non-empty string.")

        # --- Guard Clause 3: frequency is mandatory, and it must evenly divide the hour ---
        frequency = content.get("frequency")
        frequency_seconds = self._validate_duration(frequency, "frequency", must_be_positive=True)
        if self._SECONDS_IN_AN_HOUR % frequency_seconds != 0:
            raise ValueError(
                f"{self._LOG_PREFIX} 'frequency' is {frequency['value']} {frequency['unit']}, which does not evenly "
                f"divide the hour. Examples of valid frequencies: 15 seconds, 5 minutes, 1 hour."
            )

        # --- Guard Clause 4: the Schema Storage must be configured ---
        schema_storage = self._get_dict(content, "schema_storage", mandatory=True)
        schema_storage_connection = self._validate_connection_configuration(
            schema_storage.get("connection_configuration"), "schema_storage.connection_configuration",
            id_key="schema_storage_type_id", valid_ids=SchemaStorageLabel
        )

        # --- Guard Clause 4.1: the Runtime Data Storage is optional here, because only the Inference container uses it ---
        runtime_data_storage = self._get_dict(content, "runtime_data_storage")
        runtime_data_storage_connection = None
        if runtime_data_storage:
            runtime_data_storage_connection = self._validate_connection_configuration(
                runtime_data_storage.get("connection_configuration"), "runtime_data_storage.connection_configuration",
                id_key="runtime_data_storage_type_id", valid_ids=RuntimeDataStorageLabel
            )

        external_systems = self._get_dict(content, "external_systems", mandatory=True)

        # --- Guard Clause 5: there must be at least one Telemetry System, and each one must be well formed ---
        telemetry_systems = self._get_dict(
            external_systems, "telemetry_systems", parent_name="external_systems", mandatory=True
        )
        if not telemetry_systems:
            raise ValueError(f"{self._LOG_PREFIX} 'external_systems.telemetry_systems' must not be empty.")

        # Imported here, and not at the top of the file, to avoid a circular import (the launchers use this module)
        from app.core.inference.decision_validation_correction_logic_base import (
            DecisionValidationCorrectionLogicBase
        )
        from app.core.inference.model_execution_error_handling_logic_base import (
            ModelExecutionErrorHandlingLogicBase
        )
        from app.core.decision_dispatching.actuation_error_handling_logic_base import ActuationErrorHandlingLogicBase
        from app.core.inference.ml_model_launcher_base import MLModelLauncherBase
        from app.core.decision_dispatching.target_system_launcher_base import (
            TargetSystemLauncherBase
        )
        from app.core.data_ingestion.telemetry_system_launcher_base import (
            TelemetrySystemLauncherBase
        )
        for system_id, system in telemetry_systems.items():
            system_name = f"external_systems.telemetry_systems.{system_id}"

            if not isinstance(system, dict) or not isinstance(system.get("path"), str) or not system["path"].strip():
                raise ValueError(f"{self._LOG_PREFIX} '{system_name}' must be a dictionary with a 'path'.")

            self._validate_launcher_path(system["path"], f"{system_name}.path", TelemetrySystemLauncherBase)
            self._validate_connection_configuration(
                system.get("connection_configuration"), f"{system_name}.connection_configuration",
                valid_ids=InputHandlerLabel
            )

        # --- Guard Clause 6: Target Systems are optional, but each one must be well formed if given ---
        target_systems = self._get_dict(external_systems, "target_systems", parent_name="external_systems")
        for system_id, system in target_systems.items():
            system_name = f"external_systems.target_systems.{system_id}"

            if not isinstance(system, dict) or not isinstance(system.get("path"), str) or not system["path"].strip():
                raise ValueError(f"{self._LOG_PREFIX} '{system_name}' must be a dictionary with a 'path'.")

            self._validate_launcher_path(system["path"], f"{system_name}.path", TargetSystemLauncherBase)
            self._validate_connection_configuration(
                system.get("connection_configuration"), f"{system_name}.connection_configuration",
                valid_ids=OutputHandlerLabel
            )

            # The Actuation Error Handling Logic is optional. If it is not given, the launcher of the Target System
            # uses its own.
            error_handling_logic = system.get("actuation_error_handling_logic")
            if error_handling_logic is not None:
                logic_name = f"{system_name}.actuation_error_handling_logic"
                if not isinstance(error_handling_logic, dict) or not isinstance(error_handling_logic.get("path"), str) \
                        or not error_handling_logic["path"].strip():
                    raise ValueError(f"{self._LOG_PREFIX} '{logic_name}' must be a dictionary with a 'path'.")
                self._validate_launcher_path(
                    error_handling_logic["path"], f"{logic_name}.path", ActuationErrorHandlingLogicBase
                )

        # --- Guard Clause 7: Machine Learning models are optional, but each one must be well formed if given ---
        ml_models = self._get_dict(external_systems, "ml_models", parent_name="external_systems")
        for ml_model_id, ml_model in ml_models.items():
            ml_model_name = f"external_systems.ml_models.{ml_model_id}"

            if not isinstance(ml_model, dict) or not isinstance(ml_model.get("path"), str) \
                    or not ml_model["path"].strip():
                raise ValueError(f"{self._LOG_PREFIX} '{ml_model_name}' must be a dictionary with a 'path'.")

            self._validate_launcher_path(ml_model["path"], f"{ml_model_name}.path", MLModelLauncherBase)
            self._validate_connection_configuration(
                ml_model.get("connection_configuration"), f"{ml_model_name}.connection_configuration",
                valid_ids=OutputHandlerLabel
            )

            # The logics of the model are optional. If one is not given, the ML Model launcher uses its own (the
            # Decision Validation and Correction Logic has the safety limits of the installation, so each installation
            # writes its own class)
            for logic_key, logic_base in (
                    ("model_execution_error_handling_logic", ModelExecutionErrorHandlingLogicBase),
                    ("decision_validation_correction_logic", DecisionValidationCorrectionLogicBase)
            ):
                logic = ml_model.get(logic_key)
                if logic is None:
                    continue
                logic_name = f"{ml_model_name}.{logic_key}"
                if not isinstance(logic, dict) or not isinstance(logic.get("path"), str) or not logic["path"].strip():
                    raise ValueError(f"{self._LOG_PREFIX} '{logic_name}' must be a dictionary with a 'path'.")
                self._validate_launcher_path(logic["path"], f"{logic_name}.path", logic_base)

        # --- Guard Clause 8: the communication between containers is optional here ---
        # It is optional for the InstanceConfiguration, because an application may launch only some containers (e.g.,
        # only Data Ingestion). The launcher of a container requires the links it needs. The 'protocol_id' of each
        # link says if the containers run in the same process ("AsyncLocal") or talk over the network.
        inter_container = self._get_dict(content, "inter_container_communication")
        data_ingestion_output = self._get_dict(
            inter_container, "data_ingestion_to_data_processing", parent_name="inter_container_communication"
        )
        data_ingestion_output_connection = None
        if data_ingestion_output:
            data_ingestion_output_connection = self._validate_connection_configuration(
                data_ingestion_output.get("connection_configuration"),
                "inter_container_communication.data_ingestion_to_data_processing.connection_configuration",
                valid_ids=OutputHandlerLabel
            )

        # Same for the communication from Data Processing to Inference. Each link between two containers has its own
        # entry, named after the container that sends and the one that receives, and is configured on its own.
        data_processing_output = self._get_dict(
            inter_container, "data_processing_to_inference", parent_name="inter_container_communication"
        )
        data_processing_output_connection = None
        if data_processing_output:
            data_processing_output_connection = self._validate_connection_configuration(
                data_processing_output.get("connection_configuration"),
                "inter_container_communication.data_processing_to_inference.connection_configuration",
                valid_ids=OutputHandlerLabel
            )

        # Same for the communication from Inference to Decision Dispatching.
        inference_output = self._get_dict(
            inter_container, "inference_to_decision_dispatching", parent_name="inter_container_communication"
        )
        inference_output_connection = None
        if inference_output:
            inference_output_connection = self._validate_connection_configuration(
                inference_output.get("connection_configuration"),
                "inter_container_communication.inference_to_decision_dispatching.connection_configuration",
                valid_ids=OutputHandlerLabel
            )

        # The two links that replace 'data_processing_to_inference' when the REC Bridge is used: Data Processing sends to
        # the REC Bridge, and the REC Bridge sends to Inference.
        data_processing_to_rec_bridge_connection = self._optional_link(
            inter_container, "data_processing_to_rec_bridge"
        )
        rec_bridge_to_inference_connection = self._optional_link(inter_container, "rec_bridge_to_inference")

        # --- Guard Clause 9: logging is optional, but must be well formed if given ---
        logging_configuration = self._get_dict(content, "logging")
        self._get_dict(logging_configuration, "log_files", parent_name="logging")
        console_level = logging_configuration.get("console_level", "INFO")
        accepted_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        if not isinstance(console_level, str) or console_level.strip().upper() not in accepted_levels:
            raise ValueError(
                f"{self._LOG_PREFIX} 'logging.console_level' is '{console_level}', which is not a known level. "
                f"Known levels: {accepted_levels}."
            )

        # --- Guard Clause 10: the grace period is optional, but it must be shorter than the frequency ---
        # Zero or less means that there is no grace period: the data of a period is processed as soon as it ends
        grace_period = content.get("grace_period")
        grace_period_seconds = 0.0
        if grace_period is not None:
            grace_period_seconds = self._validate_duration(grace_period, "grace_period", must_be_positive=False)
            if grace_period_seconds >= frequency_seconds:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'grace_period' ({grace_period['value']} {grace_period['unit']}) must be "
                    f"shorter than the 'frequency' ({frequency['value']} {frequency['unit']})."
                )

        # --- Guard Clause 11: the Data-Issue Handling Approaches are optional, but their ids must be known ---
        data_issue_handling_approaches = content.get("data_issue_handling_approaches", [])
        if not isinstance(data_issue_handling_approaches, list):
            raise ValueError(f"{self._LOG_PREFIX} 'data_issue_handling_approaches' must be a list.")
        accepted_approaches = [label.value for label in DataIssueHandlingApproachLabel]
        for approach_id in data_issue_handling_approaches:
            if approach_id not in accepted_approaches:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'data_issue_handling_approaches' has '{approach_id}', which is not a known "
                    f"id. Known ids: {accepted_approaches}."
                )

        # --- Guard Clause 12: the integrated logic is optional, but each path must lead to an existing class ---
        integrated_logic = self._get_dict(content, "integrated_logic")
        for logic_name, logic_path in integrated_logic.items():
            logic_full_name = f"integrated_logic.{logic_name}"

            # A name that is not in the list is probably a typo, and would be ignored without any warning
            if logic_name not in self._INTEGRATED_LOGIC_BASE_CLASSES:
                raise ValueError(
                    f"{self._LOG_PREFIX} '{logic_full_name}' is not a known type of integrated logic. "
                    f"Known types: {list(self._INTEGRATED_LOGIC_BASE_CLASSES)}."
                )
            if not isinstance(logic_path, str) or not logic_path.strip():
                raise ValueError(f"{self._LOG_PREFIX} '{logic_full_name}' must be the path of a class.")

            self._validate_launcher_path(logic_path, logic_full_name, self._INTEGRATED_LOGIC_BASE_CLASSES[logic_name])

        # --- Guard Clause 12b: the Application Launcher is optional. Without it, all the containers are launched ---
        application_launcher_content = self._get_dict(content, "application_launcher")
        application_launcher_path = self._DEFAULT_APPLICATION_LAUNCHER_PATH
        if application_launcher_content:
            application_launcher_path = application_launcher_content.get("path")
            if not isinstance(application_launcher_path, str) or not application_launcher_path.strip():
                raise ValueError(f"{self._LOG_PREFIX} 'application_launcher.path' must be the path of a class.")
            self._validate_launcher_path(application_launcher_path, "application_launcher.path")

        # --- Guard Clause 13: the REC Bridge is optional. If it is configured, it needs everything below ---
        rec_bridge_content = self._get_dict(content, "rec_bridge")
        rec_bridge = None
        if rec_bridge_content:
            # The REC Bridge is an extension: Percepta only knows the path of its launcher. That it extends the right
            # class is checked by the application launcher, which builds it.
            launcher_path = rec_bridge_content.get("path")
            if not isinstance(launcher_path, str) or not launcher_path.strip():
                raise ValueError(f"{self._LOG_PREFIX} 'rec_bridge.path' must be the path of a class.")
            self._validate_launcher_path(launcher_path, "rec_bridge.path")

            # How the installations of the REC talk to each other: like the other external systems, with a
            # 'connection_configuration'. They are different applications, so it has to be over the network.
            rec_connection = self._validate_connection_configuration(
                rec_bridge_content.get("connection_configuration"), "rec_bridge.connection_configuration",
                valid_ids=OutputHandlerLabel
            )
            if rec_connection["protocol_id"] != OutputHandlerLabel.ASYNC_RABBITMQ.value:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'rec_bridge.connection_configuration.protocol_id' must be "
                    f"'{OutputHandlerLabel.ASYNC_RABBITMQ.value}', because the installations of a REC are different "
                    f"applications that talk over the network."
                )

            # The topology has two parts: where this installation publishes and where it receives from the others
            topology = rec_connection["connection_info"].get("topology")
            if not isinstance(topology, dict) or not isinstance(topology.get("publish"), dict) \
                    or not isinstance(topology.get("subscribe"), dict):
                raise ValueError(
                    f"{self._LOG_PREFIX} 'rec_bridge.connection_configuration.connection_info.topology' must be a "
                    f"dictionary with 'publish' and 'subscribe'."
                )

            # The Bridge waits for the other installations inside the period, so it must end before the next one.
            # Like the other settings of an external system, it is in the 'connection_info'.
            collection_window_seconds = rec_connection["connection_info"].get("collection_window_seconds")
            if isinstance(collection_window_seconds, bool) or not isinstance(collection_window_seconds, (int, float)) \
                    or collection_window_seconds <= 0:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'rec_bridge.connection_configuration.connection_info.collection_window_seconds'"
                    f" must be a number greater than zero."
                )
            if collection_window_seconds >= frequency_seconds:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'rec_bridge.connection_configuration.connection_info.collection_window_seconds'"
                    f" must be shorter than the 'frequency'."
                )

            if data_processing_to_rec_bridge_connection is None or rec_bridge_to_inference_connection is None:
                raise ValueError(
                    f"{self._LOG_PREFIX} 'rec_bridge' is configured, so 'inter_container_communication' needs "
                    f"'data_processing_to_rec_bridge' and 'rec_bridge_to_inference'."
                )

            rec_bridge = {
                "path": launcher_path,
                "connection_configuration": rec_connection,
                "collection_window_seconds": collection_window_seconds
            }

        self._content = content
        self._timezone = timezone
        self._frequency = frequency
        self._frequency_seconds = frequency_seconds
        self._grace_period = grace_period
        self._grace_period_seconds = grace_period_seconds
        self._data_issue_handling_approaches = data_issue_handling_approaches
        self._integrated_logic = integrated_logic
        self._schema_storage = schema_storage_connection
        self._runtime_data_storage = runtime_data_storage_connection
        self._telemetry_systems = telemetry_systems
        self._target_systems = target_systems
        self._ml_models = ml_models
        self._data_ingestion_to_data_processing = data_ingestion_output_connection
        self._data_processing_to_inference = data_processing_output_connection
        self._inference_to_decision_dispatching = inference_output_connection
        self._data_processing_to_rec_bridge = data_processing_to_rec_bridge_connection
        self._rec_bridge_to_inference = rec_bridge_to_inference_connection
        self._rec_bridge = rec_bridge
        self._application_launcher_path = application_launcher_path
        self._logging = logging_configuration

    @property
    def timezone(self) -> str:
        """str: Timezone of the instance. Defaults to "UTC"."""
        return self._timezone

    @property
    def frequency(self) -> dict:
        """dict: How often the data is collected, e.g., {"value": 15, "unit": "seconds"}."""
        return self._frequency

    @property
    def frequency_seconds(self) -> float:
        """float: The frequency in seconds, e.g., 900 for {"value": 15, "unit": "minutes"}."""
        return self._frequency_seconds

    @property
    def grace_period(self) -> dict | None:
        """dict | None: How long to wait for late data after a period ends, e.g., {"value": 2, "unit": "seconds"}.

        It is None if it is not configured. A value of zero or less also means that there is no grace period.
        """
        return self._grace_period

    @property
    def grace_period_seconds(self) -> float:
        """float: The grace period in seconds, e.g., 2 for {"value": 2, "unit": "seconds"}. Zero if there is none."""
        return max(self._grace_period_seconds, 0.0)

    @property
    def data_issue_handling_approaches(self) -> list:
        """list: Ids of the Data-Issue Handling Approaches, in the order in which they are applied. Empty if none.

        Expected Output Structure:
        --------------------------
        ["missing_data", "duplicated_data", "out_of_order_data", "frequency_harmonisation"]
        """
        return self._data_issue_handling_approaches

    @property
    def integrated_logic(self) -> dict:
        """dict: The paths of the integrated logic classes, by type of logic. Empty if there are none.

        Expected Output Structure:
        --------------------------
        {"combination_logic": "app.extension.logic.combination.CombinationLogic1"}
        """
        return self._integrated_logic

    @property
    def schema_storage(self) -> dict:
        """dict: Connection configuration of the Schema Storage.

        Expected Output Structure:
        --------------------------
        {"schema_storage_type_id": "json", "connection_info": {"file_path": "environment_files/environments.json"}}
        """
        return self._schema_storage

    @property
    def runtime_data_storage(self) -> dict | None:
        """dict | None: Connection configuration of the Runtime Data Storage, or None if it is not configured.

        Expected Output Structure:
        --------------------------
        {"runtime_data_storage_type_id": "mongodb", "connection_info": {"host": "localhost", "port": 27017}}
        """
        return self._runtime_data_storage

    @property
    def telemetry_systems(self) -> dict:
        """dict: The Telemetry Systems, by id. Each has a 'path' and a 'connection_configuration'."""
        return self._telemetry_systems

    @property
    def target_systems(self) -> dict:
        """dict: The Target Systems, by id. Empty if there are none."""
        return self._target_systems

    @property
    def ml_models(self) -> dict:
        """dict: The Machine Learning models, by id. Empty if there are none."""
        return self._ml_models

    @property
    def data_ingestion_to_data_processing(self) -> dict | None:
        """dict | None: Connection configuration of the link from Data Ingestion to Data Processing.

        It is None if it is not configured. Data Ingestion sends over it and Data Processing receives from it. A
        'protocol_id' of "AsyncLocal" means that both containers run in the same process.

        Expected Output Structure:
        --------------------------
        {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
        """
        return self._data_ingestion_to_data_processing

    @property
    def data_processing_to_inference(self) -> dict | None:
        """dict | None: Connection configuration of the link from Data Processing to Inference.

        It is None if it is not configured. Data Processing sends over it and Inference receives from it. A
        'protocol_id' of "AsyncLocal" means that both containers run in the same process.

        Expected Output Structure:
        --------------------------
        {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
        """
        return self._data_processing_to_inference

    @property
    def inference_to_decision_dispatching(self) -> dict | None:
        """dict | None: Connection configuration of the link from Inference to Decision Dispatching.

        It is None if it is not configured. Inference sends over it and Decision Dispatching receives from it. A
        'protocol_id' of "AsyncLocal" means that both containers run in the same process.

        Expected Output Structure:
        --------------------------
        {"protocol_id": "AsyncRabbitMQ", "connection_info": {}}
        """
        return self._inference_to_decision_dispatching

    @property
    def rec_bridge(self) -> dict | None:
        """dict | None: The settings of the REC Bridge container, or None if the instance does not use it.

        When it is not None, the messages go from Data Processing to the REC Bridge ('data_processing_to_rec_bridge')
        and from it to Inference ('rec_bridge_to_inference'), instead of directly through 'data_processing_to_inference'.

        Expected Output Structure:
        --------------------------
        {
            "path": "app.extension.rec_bridge.energaize.energaize_rec_bridge_launcher.EnergAIzeRECBridgeLauncher",
            "connection_configuration": {"protocol_id": "AsyncRabbitMQ", "connection_info": {"connection": {}, "topology": {}}},
            "collection_window_seconds": 5
        }
        """
        return self._rec_bridge

    @property
    def data_processing_to_rec_bridge(self) -> dict | None:
        """dict | None: Connection configuration of the link from Data Processing to the REC Bridge (None if not set)."""
        return self._data_processing_to_rec_bridge

    @property
    def rec_bridge_to_inference(self) -> dict | None:
        """dict | None: Connection configuration of the link from the REC Bridge to Inference (None if not set)."""
        return self._rec_bridge_to_inference

    @property
    def data_processing_output_link(self) -> dict | None:
        """dict | None: The link that Data Processing sends the unified message over.

        It is the link to the REC Bridge if the instance uses it, and the link to Inference otherwise.
        """
        return self._data_processing_to_rec_bridge if self._rec_bridge else self._data_processing_to_inference

    @property
    def inference_input_link(self) -> dict | None:
        """dict | None: The link that Inference receives the unified message from.

        It is the link from the REC Bridge if the instance uses it, and the link from Data Processing otherwise.
        """
        return self._rec_bridge_to_inference if self._rec_bridge else self._data_processing_to_inference

    @property
    def application_launcher_path(self) -> str:
        """str: Path of the class of the Application Launcher that runs this instance.

        It is the one in 'application_launcher.path', or the SingleProcessApplicationLauncher (all the containers) if
        the instance does not configure one.

        Expected Output Structure:
        --------------------------
        "app.core.launchers.application_launchers.single_process_application_launcher.SingleProcessApplicationLauncher"
        """
        return self._application_launcher_path

    @property
    def logging(self) -> dict:
        """dict: Settings of the logger. Empty if there are none."""
        return self._logging

    def _get_dict(self, parent: dict, key: str, parent_name: str = "", mandatory: bool = False) -> dict:
        """Returns a part of the configuration that must be a dictionary.

        Args:
            parent (dict): The dictionary that should contain the part.
            key (str): Name of the part.
            parent_name (str): Name of the parent (e.g., "external_systems"), only to build the name shown in the
                error messages.
            mandatory (bool): If True, the part must exist. Otherwise, an empty dictionary is returned when missing.

        Returns:
            dict: The part, or an empty dictionary if it is optional and missing.

        Raises:
            ValueError: If the part is mandatory and missing, or it is not a dictionary.
        """
        name = f"{parent_name}.{key}" if parent_name else key
        value = parent.get(key)
        if value is None:
            if mandatory:
                raise ValueError(f"{self._LOG_PREFIX} '{name}' is missing.")
            return {}
        if not isinstance(value, dict):
            raise ValueError(f"{self._LOG_PREFIX} '{name}' must be a dictionary.")
        return value

    def _optional_link(self, inter_container: dict, link_name: str) -> dict | None:
        """Validates one link between two containers, if it is configured.

        Args:
            inter_container (dict): The 'inter_container_communication' of the configuration.
            link_name (str): Name of the link (e.g., "data_processing_to_rec_bridge").

        Returns:
            dict | None: The connection configuration of the link, or None if it is not configured.
        """
        link = self._get_dict(inter_container, link_name, parent_name="inter_container_communication")
        if not link:
            return None
        return self._validate_connection_configuration(
            link.get("connection_configuration"),
            f"inter_container_communication.{link_name}.connection_configuration",
            valid_ids=OutputHandlerLabel
        )

    def _validate_duration(self, value: object, name: str, must_be_positive: bool) -> float:
        """Checks that a duration has a numeric 'value' and a known 'unit', and returns it in seconds.

        Expected Input Structure:
        -------------------------
        {"value": 15, "unit": "seconds"}      # The unit is "seconds", "minutes" or "hours"

        Args:
            value (object): The duration.
            name (str): Name of the duration, for the error messages (e.g., "frequency").
            must_be_positive (bool): If True, the value must be greater than zero. Otherwise, any number is accepted
                (the grace period may be zero or less, which means that there is none).

        Returns:
            float: The duration in seconds, e.g., {"value": 2, "unit": "minutes"} returns 120.

        Raises:
            ValueError: If it is missing, is not a dictionary, has a 'value' that is not a number (or not greater than
                zero, if required), or has an unknown 'unit'.
        """
        if not isinstance(value, dict):
            raise ValueError(f"{self._LOG_PREFIX} '{name}' is missing or is not a dictionary.")

        # A boolean is a number for Python (True == 1), so it is excluded explicitly
        number = value.get("value")
        if isinstance(number, bool) or not isinstance(number, (int, float)):
            raise ValueError(f"{self._LOG_PREFIX} '{name}.value' must be a number.")
        if must_be_positive and number <= 0:
            raise ValueError(f"{self._LOG_PREFIX} '{name}.value' must be greater than zero.")

        unit = value.get("unit")
        if unit not in self._SECONDS_PER_UNIT:
            raise ValueError(
                f"{self._LOG_PREFIX} '{name}.unit' is '{unit}', which is not a known unit. "
                f"Known units: {list(self._SECONDS_PER_UNIT)}."
            )
        return number * self._SECONDS_PER_UNIT[unit]

    def _validate_launcher_path(
            self, launcher_path: str, name: str, base_class: Optional[type] = None
    ) -> None:
        """Checks that the path of a launcher, or of an integrated logic, leads to a class that exists.

        Examples:
        ---------
        "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher.CWTelemetryLauncher"
        "app.extension.telemetry_systems.cleanwatts.cw_telemetry_launcher:CWTelemetryLauncher"

        Args:
            launcher_path (str): Module path and class name, separated by a dot or by a colon.
            name (str): Name of the value, for the error messages.
            base_class (type, optional): The class that the launcher must extend (e.g.,
                TelemetrySystemLauncherBase). If None, only the existence of the class is checked.

        Raises:
            ValueError: If the path is not in the format above, the module cannot be imported, the module has no
                such class, or the class does not extend the base class (if one is given).
        """
        module_path, _, class_name = (
            launcher_path.rpartition(":") if ":" in launcher_path else launcher_path.rpartition(".")
        )
        if not module_path or not class_name:
            raise ValueError(
                f"{self._LOG_PREFIX} '{name}' is '{launcher_path}', which is not in the format 'module.ClassName'."
            )

        try:
            module = importlib.import_module(module_path)
        except ImportError as err:
            raise ValueError(f"{self._LOG_PREFIX} '{name}': could not import the module '{module_path}': {err}") from err

        launcher_class = getattr(module, class_name, None)
        if launcher_class is None:
            raise ValueError(f"{self._LOG_PREFIX} '{name}': the module '{module_path}' has no class '{class_name}'.")

        if not isinstance(launcher_class, type):
            raise ValueError(f"{self._LOG_PREFIX} '{name}': '{class_name}' must be a class.")

        if base_class is not None and not issubclass(launcher_class, base_class):
            raise ValueError(f"{self._LOG_PREFIX} '{name}': '{class_name}' must extend {base_class.__name__}.")

    def _validate_connection_configuration(
            self, value: object, name: str, valid_ids: Type[Enum], id_key: str = "protocol_id"
    ) -> dict:
        """Checks that a connection configuration has a valid id and its 'connection_info'.

        What is inside 'connection_info' is not checked here.

        Expected Input Structure:
        -------------------------
        {"protocol_id": "AsyncHTTP", "connection_info": {}}

        Args:
            value (object): The connection configuration.
            name (str): Name of the connection configuration, for the error messages.
            valid_ids (Type[Enum]): The labels with the ids that are accepted (e.g., InputHandlerLabel).
            id_key (str): Name of the key with the id (e.g., "protocol_id", "schema_storage_type_id").

        Returns:
            dict: The connection configuration.

        Raises:
            ValueError: If it is not a dictionary, has no id or an id that is not one of the labels, or has no
                'connection_info' dictionary.
        """
        if not isinstance(value, dict):
            raise ValueError(f"{self._LOG_PREFIX} '{name}' is missing or is not a dictionary.")
        if not isinstance(value.get(id_key), str) or not value[id_key].strip():
            raise ValueError(f"{self._LOG_PREFIX} '{name}.{id_key}' must be a non-empty string.")
        accepted_ids = [label.value for label in valid_ids]
        if value[id_key] not in accepted_ids:
            raise ValueError(
                f"{self._LOG_PREFIX} '{name}.{id_key}' is '{value[id_key]}', which is not a known id. "
                f"Known ids: {accepted_ids}."
            )
        if not isinstance(value.get("connection_info"), dict):
            raise ValueError(f"{self._LOG_PREFIX} '{name}.connection_info' is missing or is not a dictionary.")
        return value
