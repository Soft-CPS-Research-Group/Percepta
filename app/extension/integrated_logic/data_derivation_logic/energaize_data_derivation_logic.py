# --- Standard Library Imports ---
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

# --- Internal Framework Imports ---
from app.core.data_processing.data_derivation_logic_base import DataDerivationLogicBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.unit_utils import convert
from app.core.utils.time_utils import format_timestamp
from app.extension.utils.labels import AggregationLabel, Label, ParamLabel


class EnergAIzeDataDerivationLogic(DataDerivationLogicBase):
    """Data-Derivation Logic of the EnergAIze instance.

    It takes the message built by the Combination Logic and adds the two parameters that the EnergAIze algorithm needs
    and that no device measures directly:

        - "solar_generation"  -> the sum of the energy produced by all the PV panels of the installation in the
                                 period (with only one panel, it is the value of that panel).
        - "non_shiftable_load" -> the energy consumed by the installation that cannot be shifted in time, i.e., what
                                 is left of the consumption once the energy exchanged with the grid, the solar
                                 generation and the energy exchanged with the batteries are accounted for:

                                 grid_in + solar_generation - grid_out + battery_discharge - battery_charge

                                 It is never negative (measurement noise can make the balance slightly negative).

    The derived data go in the key "derived_data" of the message. They are worked out from the 'final_values' of the
    parameters, so each one has a single list 'values' (and not 'original_values' and 'final_values'), followed by
    the other fields of the parameters of the entities. The rest of the message is not changed.

    Expected Input Structure:
    -------------------------
    {
        "timestamp": "2026-10-13 16:35:45 +0000",                       # The end of the period
        "entities": {
            "PV01": {
                "entity_type_id": "pv_panel",
                "parameters": {
                    "33711": {
                        "parameter_type_id": "energy",
                        "original_values": [0.05, 0.07],
                        "final_values": [0.12],
                        "measurement_unit": "kWh",
                        "aggregation_method": "sum",
                        "start_date": "2026-10-13 16:35:30 +0000",
                        "frequency_seconds": 15,
                        "generated": False
                    }
                }
            }
        }
    }

    Expected Output Structure:
    --------------------------
    {
        "timestamp": "2026-10-13 16:35:45 +0000",
        "entities": {...},                                              # As received
        "derived_data": {
            "solar_generation": {
                "values": [0.12],
                "measurement_unit": "kWh",
                "aggregation_method": "sum",
                "start_date": "2026-10-13 16:35:30 +0000",
                "frequency_seconds": 15,
                "generated": False                                      # True if any value used was synthesised or missing
            },
            "non_shiftable_load": {...}                                 # Same fields
        }
    }
    """

    _MEASUREMENT_UNIT: str = "kWh"

    def __init__(self, environment: Dict[str, Any], settings: Dict[str, Any], logger: LoggingUtils) -> None:
        """Initializes the logic.

        Expected Input Structure:
        -------------------------
        environment = {
            "group": "group_1",
            "entities": {
                "PV01": {
                    "entity_type_id": "pv_panel",
                    "parameters": {
                        "33711": {
                            "parameter_type_id": "energy",
                            "measurement_unit": "kWh",
                            "temporal_behavior": {
                                "cumulative": True,
                                "periodicity": {"value": 5, "unit": "min"},
                                "aggregation_operation": "sum",
                                "fill_operation": "linear"
                            },
                            "necessary": True
                        }
                    }
                }
            },
            ...                                 # The rest of the environment, as returned by the Schema Storage
        }
        settings = {
            "timezone": "UTC",                  # The timezone of the periods
            "frequency_seconds": 15,            # The length of a period, in seconds
            "grace_period_seconds": 2           # Wait after the end of a period before processing it
        }

        Args:
            environment (Dict[str, Any]): The environment this logic works for, as returned by the Schema Storage.
            settings (Dict[str, Any]): The settings of the Data Processing container. This logic needs
                'frequency_seconds', the length of the period.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the environment has no 'entities', or if 'frequency_seconds' is missing or not positive.
        """
        super().__init__(environment, settings, logger)

        # Guard Clause 1: Checks that the environment has the 'entities' dictionary.
        if not isinstance(environment.get("entities"), dict):
            raise ValueError("The environment must have an 'entities' dictionary.")

        # Guard Clause 2: Checks that 'frequency_seconds' is a positive number.
        frequency_seconds = settings.get("frequency_seconds")
        if isinstance(frequency_seconds, bool) or not isinstance(frequency_seconds, (int, float)) \
                or frequency_seconds <= 0:
            raise ValueError(f"'frequency_seconds' must be a positive number, and it is {frequency_seconds!r}.")

        self._frequency_seconds: float = frequency_seconds

    def derive(self, message: Dict[str, Any], period_end: datetime) -> Dict[str, Any]:
        """Adds the derived data to the message of the period.

        Expected Input Structure:
        -------------------------
        message = {
            "timestamp": "2026-10-13 16:35:45 +0000",
            "entities": {
                "GR01": {"entity_type_id": "grid_meter", "parameters": {
                    "energy_in_total": {"final_values": [0.30], "generated": False, ...},
                    "energy_out_total": {"final_values": [0.05], "generated": False, ...}}},
                "PV01": {"entity_type_id": "pv_panel", "parameters": {
                    "33711": {"parameter_type_id": "energy", "final_values": [0.12], "generated": False, ...}}}
            }
        }

        Expected Output Structure:
        --------------------------
        The same message, plus "derived_data" (see the class documentation). For the example above:
        solar_generation = 0.12 and non_shiftable_load = 0.30 + 0.12 - 0.05 = 0.37.

        Args:
            message (Dict[str, Any]): The message built by the Combination Logic.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            Dict[str, Any]: The message with the derived data.

        Raises:
            ValueError: If 'period_end' has no timezone.
        """
        # Guard Clause 1: Checks that 'period_end' has a timezone.
        if period_end.tzinfo is None:
            raise ValueError("'period_end' must have a timezone.")

        entities: Dict[str, Dict[str, Any]] = message.get("entities", {})
        period_start: datetime = period_end - timedelta(seconds=self._frequency_seconds)

        solar_generation, solar_generated = self._sum_parameter(entities, Label.PV_PANEL, ParamLabel.ENERGY)
        grid_in, grid_in_generated = self._sum_parameter(entities, Label.GRID_METER, ParamLabel.ENERGY_IN_TOTAL)
        grid_out, grid_out_generated = self._sum_parameter(entities, Label.GRID_METER, ParamLabel.ENERGY_OUT_TOTAL)
        battery_charge, charge_generated = self._sum_parameter(entities, Label.BATTERY, ParamLabel.ENERGY_IN)
        battery_discharge, discharge_generated = self._sum_parameter(entities, Label.BATTERY, ParamLabel.ENERGY_OUT)

        non_shiftable_load = grid_in + solar_generation - grid_out + battery_discharge - battery_charge

        # The balance can be slightly negative because of measurement noise, but a load cannot be negative.
        if non_shiftable_load < 0:
            self._logger.debug(
                f"{self._LOG_PREFIX} Negative balance ({non_shiftable_load}) set to 0."
            )
            non_shiftable_load = 0.0

        derived_data = {
            "solar_generation": self._build_parameter(solar_generation, period_start, solar_generated),
            "non_shiftable_load": self._build_parameter(
                non_shiftable_load, period_start,
                solar_generated or grid_in_generated or grid_out_generated or charge_generated or discharge_generated
            ),
        }

        return {**message, "derived_data": derived_data}

    def _build_parameter(self, value: float, period_start: datetime, generated: bool) -> Dict[str, Any]:
        """Builds a piece of derived data with the fields of the parameters of the entities."""
        return {
            # A derived datum is worked out from the 'final_values' of the parameters, so it has a single list
            # 'values', and not 'original_values' and 'final_values'.
            "values": [value],
            "measurement_unit": self._MEASUREMENT_UNIT,
            "aggregation_method": AggregationLabel.SUM.value,
            "start_date": format_timestamp(period_start),
            "frequency_seconds": self._frequency_seconds,
            "generated": generated,
        }

    def _sum_parameter(
            self, entities: Dict[str, Dict[str, Any]], entity_type: Label, parameter_type: ParamLabel
    ) -> tuple:
        """Sums the first value of every parameter of the given type, in every entity of the given type.

        The sum is in kWh. Before adding, each value is checked: if the 'measurement_unit' of its parameter is not
        kWh, the value is converted to kWh (e.g., 500 Wh -> 0.5 kWh). A parameter without 'measurement_unit' is taken
        as already in kWh. If the unit cannot be converted to kWh (e.g., "kW"), the value is not used and a warning is
        logged.

        A parameter that has no usable value (e.g., its 'final_values' is None because the entity sent no data, or its
        unit cannot be converted) adds nothing to the sum, and it makes the result 'generated': the sum is not made
        only of measured data.

        Returns:
            tuple: (the sum, True if any value used was synthesised or any parameter had no usable value).
        """
        total: float = 0.0
        generated: bool = False
        for entity_id, entity in entities.items():
            if entity.get("entity_type_id") != entity_type.value:
                continue
            for parameter_id, parameter in entity.get("parameters", {}).items():
                if parameter.get("parameter_type_id") != parameter_type.value:
                    continue
                values = parameter.get("final_values") or []
                if values and isinstance(values[0], (int, float)) and not isinstance(values[0], bool):
                    # Every value is added in kWh
                    unit = parameter.get("measurement_unit")
                    try:
                        value = values[0] if unit is None else convert(values[0], unit, self._MEASUREMENT_UNIT)
                    except ValueError as err:
                        self._logger.warning(
                            f"{self._LOG_PREFIX} The parameter '{parameter_id}' of '{entity_id}' is in '{unit}', which "
                            f"cannot be converted to {self._MEASUREMENT_UNIT}, so it is not used: {err}"
                        )
                        generated = True
                        continue
                    total += value
                    generated = generated or bool(parameter.get("generated", False))
                else:
                    generated = True
        return total, generated
