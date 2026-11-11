# --- Standard Library Imports ---
import copy
from typing import Any, Dict, Iterable, List, Optional


class EnvironmentsConfiguration:
    """The environments of a Percepta instance, with their format validated.

    The environments are validated once, here, when the application starts. The containers and launchers that use an
    environment receive it already validated, so they do not need to check its format again. A wrong value stops the
    application at the start, before any container is launched, and the error says the environment, the system and the
    parameter (or actuator) where the problem is.

    This class does not know where the environments come from (a JSON file, a database...): it receives them as a
    Schema Storage returns them (see SchemaStorageBase.get_environments), so every type of storage is checked in the
    same way.

    What is checked:

    1. The format of each part: that it exists when it is mandatory, that it is of the right type, and that the text
       values are not empty.
    2. The relations between the parts of an environment: a parameter of a source mapping exists in the schema, in
       exactly one entity, and is provided by only one Telemetry System; a parameter that is derived from another
       exists and is derived from a parameter that a Telemetry System provides; an actuator that a Target System maps
       exists in an entity, and is mapped by only one Target System.
    3. The relations with the Percepta Instance Configuration: the Telemetry Systems, the Target Systems and the
       Machine Learning model that the environment uses are configured in the instance.

    What is NOT checked here:

    - What only a container needs. For example, that an environment has Telemetry Systems (Data Ingestion needs
      them), or an 'ml_model_id' (Inference needs it): the environment can be valid for the containers that are
      launched without needing them. Each container checks what it requires.
    - What is inside the 'temporal_behavior' of a parameter, or the values that a logic accepts (e.g., which
      'fill_operation' exist). That belongs to the approaches and logics that use them, as they are the ones that know
      the valid values.
    - Keys that are not listed below. They are kept as they are, since a Telemetry System or a logic may use its own.

    Expected `environments` Data Structure:
    ---------------------------------------
    {
        "R-H-01": {
            "group": "living lab",                                  # (optional) Text
            "entities": {                                           # (mandatory)
                "EVC01": {
                    "entity_type_id": "ev_charger",                 # (mandatory) Text
                    "parameters": {                                 # (optional)
                        "37507": {
                            "parameter_type_id": "energy_in",       # (mandatory) Text
                            "measurement_unit": "kWh",              # (optional) Text, empty if it has no unit
                            "necessary": True,                      # (optional) True or False
                            "rec_relevant": True,                   # (optional) True or False
                            "temporal_behavior": {...}              # (optional) Dictionary
                        },
                        "connected_vehicle": {
                            "parameter_type_id": "connected_vehicle",
                            "derived_from": "37507"                 # (optional) Id of the parameter it is built from
                        }
                    },
                    "actuators": {                                  # (optional)
                        "act_power_1": {
                            "actuator_type_id": "power",            # (mandatory) Text
                            "measurement_unit": "kW"                # (optional) Text
                        }
                    }
                }
            },
            "telemetry_systems": {                                  # (optional)
                "cleanwatts": {
                    "source_mapping": {                             # (mandatory, with at least one parameter)
                        "37507": {
                            "match_key": "37507",                   # (mandatory) Text
                            "fetch_key": "37507",                   # (optional) Text
                            "source_measurement_unit": "Wh"         # Mandatory if the schema gives it a unit
                        }
                    }
                }
            },
            "target_systems": {                                     # (optional)
                "cleanwatts": {
                    "actuator_mapping": {                           # (mandatory)
                        "act_power_1": {
                            "destination": "39052",                 # (mandatory) Text or integer
                            "destination_measurement_unit": "A",    # (mandatory) Text
                            "voltage": 230                          # (optional) Positive number
                        }
                    }
                }
            },
            "ml_model_id": "energaize_r_h_01",                      # (optional) Text
            "rec": {                                                # (optional) The Renewable Energy Community
                "community_id": "porto",                            # (mandatory) Text
                "installation_id": "R-H-01",                        # (mandatory) Text
                "peers": ["R-H-02"],                                # (optional) List of installation ids
                "derived_data_to_share": []                         # (optional) List of names
            }
        }
    }
    """

    _LOG_PREFIX: str = "Environments Configuration |"

    # The keys that a mapping of an actuator can have, so that a typo (e.g., "voltaje") does not go unnoticed
    _ACTUATOR_MAPPING_KEYS = ("destination", "destination_measurement_unit", "voltage")

    def __init__(
            self,
            environments: Dict[str, Dict[str, Any]],
            telemetry_system_ids: Iterable[str],
            target_system_ids: Iterable[str],
            ml_model_ids: Iterable[str]
    ) -> None:
        """Validates the environments.

        Args:
            environments (Dict[str, Dict[str, Any]]): The environments, by id, as a Schema Storage returns them (see
                the structure above).
            telemetry_system_ids (Iterable[str]): Ids of the Telemetry Systems configured in the instance.
            target_system_ids (Iterable[str]): Ids of the Target Systems configured in the instance.
            ml_model_ids (Iterable[str]): Ids of the Machine Learning models configured in the instance.

        Raises:
            ValueError: If there are no environments, or if anything in an environment is invalid (see above).
        """
        # --- Guard Clause: there must be at least one environment, as an instance with none has nothing to run ---
        if not isinstance(environments, dict) or not environments:
            raise ValueError(f"{self._LOG_PREFIX} There are no environments: it must be a non-empty dictionary.")

        # The ids that are configured in the instance, to check the ones that the environments refer to
        # Example: self._telemetry_system_ids = {"cleanwatts", "omie"}
        self._telemetry_system_ids = set(telemetry_system_ids)
        self._target_system_ids = set(target_system_ids)
        self._ml_model_ids = set(ml_model_ids)

        # Validates the environments one at a time. The first problem found is raised, as in the Instance Configuration
        for name, environment in environments.items():
            self._validate_environment(name, environment)

        # A copy, so that a change made by whoever receives an environment does not change the others
        self._environments = copy.deepcopy(environments)

    @property
    def environments(self) -> Dict[str, Dict[str, Any]]:
        """Dict[str, Dict[str, Any]]: The validated environments, by id."""
        return self._environments

    # ------------------------------------------------------------------------------------------------------------
    # Environment
    # ------------------------------------------------------------------------------------------------------------

    def _validate_environment(self, name: Any, environment: Any) -> None:
        """Validates one environment, one part at a time.

        The entities are validated first, as the other parts refer to their parameters and actuators.

        Args:
            name (Any): Id of the environment.
            environment (Any): The environment.

        Raises:
            ValueError: If anything in the environment is invalid.
        """
        # --- Guard Clause: the id of the environment must be a text that is not empty, and the environment a
        # dictionary. ---
        if not self._is_text(name):
            raise ValueError(f"{self._LOG_PREFIX} The id of an environment must be a non-empty string, got {name!r}.")
        if not isinstance(environment, dict):
            raise ValueError(f"{self._LOG_PREFIX} Environment '{name}' must be a dictionary.")

        # --- Guard Clause: the group is optional (only needed when the instance has a Runtime Data Storage, which
        # keeps the data of an environment under its group), but if it is there it must be a text. ---
        if "group" in environment and not self._is_text(environment["group"]):
            raise ValueError(f"{self._LOG_PREFIX} The 'group' of environment '{name}' must be a non-empty string.")

        # The parameters and the actuators of the environment, which the other parts refer to
        # Example: entities_by_parameter = {"37507": ["EVC01"], "37506": ["EVC01", "EVC02"]}
        #          derived_from_by_parameter = {"connected_vehicle": "37507"}
        #          actuator_ids = {"act_power_1", "act_power_2"}
        entities_by_parameter, derived_from_by_parameter, actuator_ids = self._validate_entities(
            name, environment.get("entities")
        )

        self._validate_telemetry_systems(name, environment, entities_by_parameter, derived_from_by_parameter)
        self._validate_target_systems(name, environment, actuator_ids)
        self._validate_ml_model_id(name, environment)
        self._validate_rec(name, environment)

    # ------------------------------------------------------------------------------------------------------------
    # Entities
    # ------------------------------------------------------------------------------------------------------------

    def _validate_entities(self, name: str, entities: Any) -> tuple:
        """Validates the entities of an environment, with their parameters and actuators.

        Args:
            name (str): Id of the environment, for the error messages.
            entities (Any): The 'entities' of the environment.

        Returns:
            tuple: Three things that the other parts of the environment need:
                - Dict[str, List[str]]: for each parameter id, the entities that have it.
                - Dict[str, Any]: for each derived parameter id, the 'derived_from' that the schema says (not checked
                  yet, see `_validate_derived_parameters`).
                - set: the ids of all the actuators.

        Raises:
            ValueError: If the entities, a parameter or an actuator are invalid.
        """
        # --- Guard Clause: 'entities' is mandatory and must be a dictionary. It may be empty here: whether an entity
        # is needed depends on the container. ---
        if not isinstance(entities, dict):
            raise ValueError(f"{self._LOG_PREFIX} Environment '{name}' must be a dictionary with 'entities'.")

        entities_by_parameter: Dict[str, List[str]] = {}
        derived_from_by_parameter: Dict[str, Any] = {}

        # For each actuator id, the entity that has it. An actuator id must belong to a single entity: the Target
        # Systems map the actuators by id only, so with two entities with the same id, the decisions of both would go
        # to the same destination.
        # Example: {"act_power_1": "EVC01", "act_power_2": "EVC02"}
        entity_by_actuator: Dict[str, str] = {}

        for entity_id, entity in entities.items():
            where_entity = f"entity '{entity_id}' of environment '{name}'"

            # --- Guard Clause: an entity is a dictionary with its type ---
            if not isinstance(entity, dict):
                raise ValueError(f"{self._LOG_PREFIX} The {where_entity} must be a dictionary.")
            if not self._is_text(entity.get("entity_type_id")):
                raise ValueError(f"{self._LOG_PREFIX} The {where_entity} needs a non-empty string 'entity_type_id'.")

            # The parameters and the actuators are optional (e.g., a battery may only have actuators)
            parameters = self._optional_dict(entity, "parameters", where_entity)
            actuators = self._optional_dict(entity, "actuators", where_entity)

            for parameter_id, parameter in parameters.items():
                self._validate_parameter(parameter, f"parameter '{parameter_id}' of {where_entity}")

                # Registers that this entity has this parameter. 'setdefault' creates the list the first time the
                # parameter id is seen. Example: after "EVC01" and "EVC02" -> {"37506": ["EVC01", "EVC02"]}
                entities_by_parameter.setdefault(parameter_id, []).append(entity_id)

                # If the parameter is derived, registers the parameter it is calculated from. Its value is only
                # checked later, when the Telemetry Systems are known.
                if "derived_from" in parameter:
                    derived_from_by_parameter[parameter_id] = parameter["derived_from"]

            for actuator_id, actuator in actuators.items():
                where_actuator = f"actuator '{actuator_id}' of {where_entity}"

                # --- Guard Clause: an actuator is a dictionary with its type, and a unit if it has one ---
                if not isinstance(actuator, dict):
                    raise ValueError(f"{self._LOG_PREFIX} The {where_actuator} must be a dictionary.")
                if not self._is_text(actuator.get("actuator_type_id")):
                    raise ValueError(
                        f"{self._LOG_PREFIX} The {where_actuator} needs a non-empty string 'actuator_type_id'."
                    )
                if "measurement_unit" in actuator and not self._is_text(actuator["measurement_unit"]):
                    raise ValueError(
                        f"{self._LOG_PREFIX} The 'measurement_unit' of the {where_actuator} must be a non-empty string."
                    )

                # --- Guard Clause: the actuator id must not be in another entity ---
                if actuator_id in entity_by_actuator:
                    raise ValueError(
                        f"{self._LOG_PREFIX} The {where_actuator} has the same id as an actuator of entity "
                        f"'{entity_by_actuator[actuator_id]}'. The id of an actuator must be unique in the "
                        f"environment, as the Target Systems map the actuators by id."
                    )
                entity_by_actuator[actuator_id] = entity_id

        return entities_by_parameter, derived_from_by_parameter, set(entity_by_actuator)

    def _validate_parameter(self, parameter: Any, where: str) -> None:
        """Validates the description of a parameter of the schema.

        Only the keys that Percepta itself uses are checked. 'derived_from' is checked later, with the Telemetry
        Systems, and the content of 'temporal_behavior' by whoever uses it.

        Args:
            parameter (Any): The description of the parameter.
            where (str): Where the parameter is, for the error messages.

        Raises:
            ValueError: If the parameter is not a dictionary, has no 'parameter_type_id', or a key has the wrong type.
        """
        # --- Guard Clause: a parameter is a dictionary with its type ---
        if not isinstance(parameter, dict):
            raise ValueError(f"{self._LOG_PREFIX} The {where} must be a dictionary.")
        if not self._is_text(parameter.get("parameter_type_id")):
            raise ValueError(f"{self._LOG_PREFIX} The {where} needs a non-empty string 'parameter_type_id'.")

        # --- Guard Clause: the unit is optional, and it is a text. An empty text means that the parameter has no unit
        # (e.g., a flag or a structure), the same as not having the key: nothing is converted. ---
        if "measurement_unit" in parameter and not isinstance(parameter["measurement_unit"], str):
            raise ValueError(
                f"{self._LOG_PREFIX} The 'measurement_unit' of the {where} must be a string (empty if it has no unit)."
            )

        # --- Guard Clause: the flags are true or false. Python treats 1 and 0 as numbers, and a text such as "false"
        # would be read as true by a careless check, so only a real true or false is accepted. ---
        for flag in ("necessary", "rec_relevant"):
            if flag in parameter and not isinstance(parameter[flag], bool):
                raise ValueError(f"{self._LOG_PREFIX} The '{flag}' of the {where} must be true or false.")

        # --- Guard Clause: the temporal behavior is a dictionary. What is inside is checked by whoever uses it. ---
        if "temporal_behavior" in parameter and not isinstance(parameter["temporal_behavior"], dict):
            raise ValueError(f"{self._LOG_PREFIX} The 'temporal_behavior' of the {where} must be a dictionary.")

    # ------------------------------------------------------------------------------------------------------------
    # Telemetry Systems
    # ------------------------------------------------------------------------------------------------------------

    def _validate_telemetry_systems(
            self,
            name: str,
            environment: dict,
            entities_by_parameter: Dict[str, List[str]],
            derived_from_by_parameter: Dict[str, Any]
    ) -> None:
        """Validates the Telemetry Systems of an environment, with the source mapping of each one.

        The source mapping of a Telemetry System says which parameters of the schema it provides and how to get and
        recognise the data of each one:
        - "match_key": how the data is recognised in the message that arrives. It is mandatory.
        - "fetch_key": where to get the data from (for example, a tag, an exchange or an environment). It is optional,
          as some Telemetry Systems do not need it (for example, PulseCharge, whose exchange comes from the topology).
        - "source_measurement_unit": the unit in which the source gives its readings. It is mandatory when the
          parameter has a 'measurement_unit' in the schema, as the Adapter converts the readings to it.
        A parameter that is derived from another is not in any source mapping: it is built by the Adapter of the
        Telemetry System that provides the parameter it is derived from.

        Expected Input Structure (`environment['telemetry_systems']`):
        --------------------------------------------------------------
        {
            "cleanwatts": {"source_mapping": {"37507": {"fetch_key": "37507", "match_key": "37507",
                                                        "source_measurement_unit": "Wh"}}},
            "pulsecharge": {"source_mapping": {"soc": {"match_key": "user@example.com"}}}
        }

        Args:
            name (str): Id of the environment, for the error messages.
            environment (dict): The environment.
            entities_by_parameter (Dict[str, List[str]]): For each parameter id, the entities that have it.
            derived_from_by_parameter (Dict[str, Any]): For each derived parameter id, its 'derived_from'.

        Raises:
            ValueError: If a Telemetry System is not configured, a source mapping or a rule is malformed, a parameter
                is not in the schema or is in more than one entity, two Telemetry Systems provide the same parameter,
                or a derived parameter is wrong.
        """
        systems = self._optional_dict(environment, "telemetry_systems", f"environment '{name}'")

        # For each parameter id that was already checked, the Telemetry System that provides it. It is used to find a
        # parameter that two Telemetry Systems claim, since the data of a parameter can only come from one of them.
        # Example: {"37507": "cleanwatts", "soc": "pulsecharge"}
        owner_by_parameter: Dict[str, str] = {}

        for system_id, system in systems.items():
            where_system = f"telemetry system '{system_id}' of environment '{name}'"

            # --- Guard Clause: the system must be configured in the instance. The environment says which systems
            # send data for it, but how to connect to each one is only in the Instance Configuration, so a system
            # that is not there cannot be launched (e.g., a typo in the id). ---
            if system_id not in self._telemetry_system_ids:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where_system} is not configured in the instance. Configured: "
                    f"{sorted(self._telemetry_system_ids)}."
                )

            # --- Guard Clause: the source mapping is what says which parameters the system provides. An empty one
            # would be a system that provides nothing for this environment, which makes no sense. ---
            source_mapping = system.get("source_mapping") if isinstance(system, dict) else None
            if not isinstance(source_mapping, dict) or not source_mapping:
                raise ValueError(f"{self._LOG_PREFIX} The {where_system} needs a non-empty 'source_mapping'.")

            for parameter_id, rule in source_mapping.items():
                where = f"parameter '{parameter_id}' of the {where_system}"

                # --- Guard Clause: the keys of the rule. 'match_key' is mandatory and must be a text that is not
                # empty (not even spaces). 'fetch_key' is optional but, if it is there, it must be a text that is not
                # empty either. A rule that is not a dictionary (e.g., None or a text) fails here too. A parameter
                # that is built from another is not in the source mapping, so a rule of None is not accepted. ---
                keys_are_valid = (
                    isinstance(rule, dict)
                    and self._is_text(rule.get("match_key"))
                    and ("fetch_key" not in rule or self._is_text(rule["fetch_key"]))
                )
                if not keys_are_valid:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Invalid rule for {where}. It must have a non-empty 'match_key' and, "
                        f"optionally, a non-empty 'fetch_key'. A parameter that is built from another is not listed "
                        f"in the source mapping: its 'derived_from' says so in the schema."
                    )

                # --- Guard Clause: the parameter must exist in the schema, and in exactly one entity. If it does
                # not exist, the source mapping talks about data that the environment does not have (e.g., a typo in
                # the id). If it is in several entities, it is not possible to know which one the data belongs to. ---
                entities = entities_by_parameter.get(parameter_id, [])
                if not entities:
                    raise ValueError(f"{self._LOG_PREFIX} The {where} is not in the schema.")
                if len(entities) > 1:
                    raise ValueError(
                        f"{self._LOG_PREFIX} The {where} is ambiguous, as it exists in several entities: {entities}."
                    )

                # --- Guard Clause: a derived parameter does not come from a Telemetry System, so it cannot be in a
                # source mapping. It would be both received and built, and it would not be known which to trust. ---
                if parameter_id in derived_from_by_parameter:
                    raise ValueError(
                        f"{self._LOG_PREFIX} The {where} is derived (it says 'derived_from' in the schema), so it "
                        f"cannot be in the source mapping."
                    )

                # --- Guard Clause: the unit of the source. It is mandatory when the schema has a unit for the
                # parameter, because the readings are converted from the unit of the source to the unit of the
                # schema, and it cannot be done without knowing the first. It is not checked that the two can be
                # converted into each other (e.g., "A" and "kW"), as an Adapter may translate a parameter of another
                # kind in its own way. Without a unit in the schema, nothing is converted and the key is optional. ---
                schema_unit = environment["entities"][entities[0]]["parameters"][parameter_id].get("measurement_unit")
                if "source_measurement_unit" in rule or schema_unit:
                    if not self._is_text(rule.get("source_measurement_unit")):
                        raise ValueError(
                            f"{self._LOG_PREFIX} The {where} needs a non-empty 'source_measurement_unit' in its "
                            f"source mapping" + (f", as the schema has the unit '{schema_unit}'." if schema_unit
                                                 else ".")
                        )

                # --- Guard Clause: a parameter can only be provided by one Telemetry System. If another system
                # already claimed it, two sources would send data for the same parameter and it would not be known
                # which one to trust. ---
                if parameter_id in owner_by_parameter:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Parameter '{parameter_id}' of environment '{name}' is claimed by "
                        f"telemetry systems '{owner_by_parameter[parameter_id]}' and '{system_id}'."
                    )

                # Records that this system provides the parameter, so that a later system that claims it is caught
                # by the check above.
                owner_by_parameter[parameter_id] = system_id

        # Checks the derived parameters. It is done after the loop above because it needs to know which parameters
        # are provided by the Telemetry Systems ('owner_by_parameter' is only complete now).
        self._validate_derived_parameters(name, entities_by_parameter, derived_from_by_parameter, owner_by_parameter)

    def _validate_derived_parameters(
            self,
            name: str,
            entities_by_parameter: Dict[str, List[str]],
            derived_from_by_parameter: Dict[str, Any],
            owner_by_parameter: Dict[str, str]
    ) -> None:
        """Validates the parameters that are built from other parameters.

        A derived parameter belongs to the Telemetry System that provides the parameter it is derived from, as it is
        the Adapter of that system that builds it. So, for each one, the parameter it derives from must exist, must
        not be derived itself, and must be provided by some Telemetry System.

        Args:
            name (str): Id of the environment, for the error messages.
            entities_by_parameter (Dict[str, List[str]]): For each parameter id, the entities that have it.
            derived_from_by_parameter (Dict[str, Any]): For each derived parameter id, its 'derived_from'.
            owner_by_parameter (Dict[str, str]): For each parameter that a Telemetry System provides, the system.

        Raises:
            ValueError: If a 'derived_from' is not the id of a parameter that a Telemetry System provides, or the
                derived parameter is in more than one entity.
        """
        for parameter_id, source_id in derived_from_by_parameter.items():
            where = f"derived parameter '{parameter_id}' of environment '{name}'"

            # --- Guard Clause: 'derived_from' must be the id of a parameter, so a text that is not empty. ---
            if not self._is_text(source_id):
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where} has an invalid 'derived_from'. It must be the id of a parameter "
                    f"of the environment."
                )

            # --- Guard Clause: the derived parameter must be in exactly one entity, like the others, so that it is
            # known which entity the built data belongs to. ---
            if len(entities_by_parameter[parameter_id]) > 1:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where} is ambiguous, as it exists in several entities: "
                    f"{entities_by_parameter[parameter_id]}."
                )

            # --- Guard Clause: the parameter that it derives from must exist in the schema (e.g., not a typo). ---
            if source_id not in entities_by_parameter:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where} derives from '{source_id}', which is not in the schema."
                )

            # --- Guard Clause: it cannot derive from another derived parameter. Which Telemetry System handles it is
            # found through the parameter it derives from, and a chain would need to be followed. ---
            if source_id in derived_from_by_parameter:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where} derives from '{source_id}', which is also derived. It must "
                    f"derive from a parameter that a Telemetry System provides."
                )

            # --- Guard Clause: some Telemetry System must provide the parameter that it derives from. Otherwise, no
            # system would handle it, and it would never be built. ---
            if source_id not in owner_by_parameter:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where} derives from '{source_id}', which no telemetry system of the "
                    f"environment provides."
                )

    # ------------------------------------------------------------------------------------------------------------
    # Target Systems
    # ------------------------------------------------------------------------------------------------------------

    def _validate_target_systems(self, name: str, environment: dict, actuator_ids: set) -> None:
        """Validates the Target Systems of an environment, with the actuator mapping of each one.

        The actuator mapping of a Target System says where the decisions of each actuator go. The Target Systems are
        optional: without them, the decisions of the environment are only logged.

        Expected Input Structure (`environment['target_systems']`):
        -----------------------------------------------------------
        {
            "cleanwatts": {"actuator_mapping": {"act_power_1": {"destination": "39052",
                                                                "destination_measurement_unit": "A",
                                                                "voltage": 230}}}
        }

        Args:
            name (str): Id of the environment, for the error messages.
            environment (dict): The environment.
            actuator_ids (set): The ids of the actuators of the entities.

        Raises:
            ValueError: If a Target System is not configured, an actuator mapping is malformed, it maps an actuator that
                no entity has, or two Target Systems map the same actuator.
        """
        targets = self._optional_dict(environment, "target_systems", f"environment '{name}'")

        # For each actuator id that was already checked, the Target System that maps it
        # Example: {"act_power_1": "cleanwatts", "act_power_2": "softcps"}
        target_by_actuator: Dict[str, str] = {}

        for target_id, target in targets.items():
            where_target = f"Target System '{target_id}' of environment '{name}'"

            # --- Guard Clause: the Target System must be configured in the instance, as it is there that how to
            # connect to it is written. ---
            if target_id not in self._target_system_ids:
                raise ValueError(
                    f"{self._LOG_PREFIX} The {where_target} is not configured in the instance. Configured: "
                    f"{sorted(self._target_system_ids)}."
                )

            # --- Guard Clause: the actuator mapping is mandatory, and may be empty ---
            actuator_mapping = target.get("actuator_mapping") if isinstance(target, dict) else None
            if not isinstance(actuator_mapping, dict):
                raise ValueError(f"{self._LOG_PREFIX} The {where_target} must be a dictionary with 'actuator_mapping'.")

            for actuator_id, mapping in actuator_mapping.items():
                where = f"actuator '{actuator_id}' of the {where_target}"
                self._validate_actuator_mapping(mapping, where)

                # --- Guard Clause: the actuator must exist in an entity. If not, the mapping is for an actuator that
                # the environment does not have (e.g., a typo in the id), and it would never be used. ---
                if actuator_id not in actuator_ids:
                    raise ValueError(f"{self._LOG_PREFIX} The {where} is not an actuator of any entity.")

                # --- Guard Clause: an actuator can only be mapped by one Target System, or it would not be known
                # which one to send its decisions to. ---
                if actuator_id in target_by_actuator:
                    raise ValueError(
                        f"{self._LOG_PREFIX} Actuator '{actuator_id}' of environment '{name}' is mapped by the Target "
                        f"Systems '{target_by_actuator[actuator_id]}' and '{target_id}'."
                    )
                target_by_actuator[actuator_id] = target_id

    def _validate_actuator_mapping(self, mapping: Any, where: str) -> None:
        """Validates what the 'actuator_mapping' of a Target System says about one actuator.

        Expected Input Structure:
        -------------------------
        {
            "destination": "39052",                     # Mandatory: where the decisions of the actuator go
            "destination_measurement_unit": "A",        # Mandatory: unit of the device. The decisions are in kW, and
                                                        # the Target System converts them. Which units are accepted
                                                        # depends on the Target System
            "voltage": 230                              # (optional) Volts, positive. To convert kW into amperes
        }

        Args:
            mapping (Any): The value of the actuator in the 'actuator_mapping'.
            where (str): Where the value is, for the error messages.

        Raises:
            ValueError: If the value is not a dictionary as above, has a key that is not known, or lacks the
                destination or the unit.
        """
        # --- Guard Clause: the mapping is a dictionary. The destination alone is not accepted: the unit of the
        # device is mandatory. ---
        if not isinstance(mapping, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of the {where} must be a dictionary with 'destination' and "
                f"'destination_measurement_unit', got {mapping!r}."
            )

        # --- Guard Clause: only the known keys, so that a typo (e.g., "voltaje") is not ignored without warning ---
        unknown = sorted(set(mapping) - set(self._ACTUATOR_MAPPING_KEYS))
        if unknown:
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of the {where} has unknown keys {unknown}. Known keys: "
                f"{list(self._ACTUATOR_MAPPING_KEYS)}."
            )

        # --- Guard Clause: the destination is a text or an integer (e.g., the id of a tag). A bool is an integer for
        # Python (True is 1), so it is refused on its own. ---
        destination = mapping.get("destination")
        destination_is_valid = self._is_text(destination) or (
            isinstance(destination, int) and not isinstance(destination, bool)
        )
        if not destination_is_valid:
            raise ValueError(f"{self._LOG_PREFIX} The mapping of the {where} needs a 'destination' (text or integer).")

        # --- Guard Clause: the unit of the device is mandatory ---
        if not self._is_text(mapping.get("destination_measurement_unit")):
            raise ValueError(
                f"{self._LOG_PREFIX} The mapping of the {where} needs a 'destination_measurement_unit' (non-empty "
                f"text)."
            )

        # --- Guard Clause: the voltage is optional, but it must be a number greater than zero ---
        voltage = mapping.get("voltage")
        voltage_is_valid = not isinstance(voltage, bool) and isinstance(voltage, (int, float)) and voltage > 0
        if "voltage" in mapping and not voltage_is_valid:
            raise ValueError(f"{self._LOG_PREFIX} The 'voltage' of the {where} must be a positive number.")

    # ------------------------------------------------------------------------------------------------------------
    # Machine Learning model and REC
    # ------------------------------------------------------------------------------------------------------------

    def _validate_ml_model_id(self, name: str, environment: dict) -> None:
        """Validates the Machine Learning model of an environment.

        The 'ml_model_id' is optional here, as only Inference needs it (and checks that it is there). If it is given,
        it must be a model that is configured in the instance.

        Args:
            name (str): Id of the environment, for the error messages.
            environment (dict): The environment.

        Raises:
            ValueError: If the 'ml_model_id' is given and is not a text, or is not configured in the instance.
        """
        # Nothing to check if the environment does not say which model it uses
        if "ml_model_id" not in environment:
            return

        ml_model_id = environment["ml_model_id"]

        # --- Guard Clause: it must be a text that is not empty ---
        if not self._is_text(ml_model_id):
            raise ValueError(
                f"{self._LOG_PREFIX} The 'ml_model_id' of environment '{name}' must be a non-empty string."
            )

        # --- Guard Clause: the model must be configured in the instance, as it is there that its launcher is ---
        if ml_model_id not in self._ml_model_ids:
            raise ValueError(
                f"{self._LOG_PREFIX} The 'ml_model_id' of environment '{name}' is '{ml_model_id}', which is not one "
                f"of the ML Models of the instance {sorted(self._ml_model_ids)}."
            )

    def _validate_rec(self, name: str, environment: dict) -> None:
        """Validates the Renewable Energy Community (REC) of an environment, if it has one.

        Expected Input Structure (`environment['rec']`):
        ------------------------------------------------
        {
            "community_id": "porto",                # Mandatory: id of the REC
            "installation_id": "R-H-01",            # Mandatory: id of this installation in the REC
            "peers": ["R-H-02", "R-H-03"],          # (optional) The other installations whose data is received
            "derived_data_to_share": []             # (optional) The names of the derived data that are shared
        }

        Args:
            name (str): Id of the environment, for the error messages.
            environment (dict): The environment.

        Raises:
            ValueError: If the 'rec' is given and any of its parts is invalid.
        """
        # The REC is optional: an environment without it does not take part in a community
        rec = environment.get("rec")
        if rec is None:
            return

        where = f"The 'rec' of environment '{name}'"

        # --- Guard Clause: it must be a dictionary with the ids of the community and of the installation ---
        if not isinstance(rec, dict):
            raise ValueError(f"{self._LOG_PREFIX} {where} must be a dictionary.")
        for key in ("community_id", "installation_id"):
            if not self._is_text(rec.get(key)):
                raise ValueError(f"{self._LOG_PREFIX} {where} needs a non-empty string '{key}'.")

        # --- Guard Clause: the peers are a list of installation ids, with no repeated one and without this
        # installation (it would receive its own data). ---
        peers = rec.get("peers", [])
        if not isinstance(peers, list) or any(not self._is_text(peer) for peer in peers):
            raise ValueError(f"{self._LOG_PREFIX} {where} needs 'peers' to be a list of installation ids.")
        if len(set(peers)) != len(peers) or rec["installation_id"] in peers:
            raise ValueError(f"{self._LOG_PREFIX} {where} has repeated peers, or has itself as a peer.")

        # --- Guard Clause: the derived data to share is a list of names ---
        derived_data = rec.get("derived_data_to_share", [])
        if not isinstance(derived_data, list) or any(not self._is_text(item) for item in derived_data):
            raise ValueError(f"{self._LOG_PREFIX} {where} needs 'derived_data_to_share' to be a list of names.")

    # ------------------------------------------------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------------------------------------------------

    @staticmethod
    def _is_text(value: Any) -> bool:
        """Tells if a value is a text with something besides spaces.

        Args:
            value (Any): The value to check.

        Returns:
            bool: True for "abc", False for "", "   ", None, or anything that is not a text.
        """
        return isinstance(value, str) and bool(value.strip())

    def _optional_dict(self, parent: dict, key: str, where: str) -> dict:
        """Returns a part that is optional but, if it is there, must be a dictionary.

        Args:
            parent (dict): The dictionary that may contain the part.
            key (str): Name of the part.
            where (str): Where the parent is, for the error message.

        Returns:
            dict: The part, or an empty dictionary if it is missing.

        Raises:
            ValueError: If the part is there and is not a dictionary.
        """
        value: Optional[Any] = parent.get(key, {})
        if not isinstance(value, dict):
            raise ValueError(f"{self._LOG_PREFIX} The '{key}' of {where} must be a dictionary.")
        return value
