from enum import Enum
from typing import Any, Dict, Type

class Label(Enum):
    GRID_METER = "grid_meter"
    PV_PANEL = "pv_panel"
    BATTERY = "battery"
    ELECTRIC_VEHICLE_CHARGER = "ev_charger"
    ELECTRIC_VEHICLE = "electric_vehicle"
    ENERGY_MARKET = "energy_market"
    FORECAST_SERVICE = "forecast_service"

class ParamLabel(Enum):
    ENERGY_PRICE = "energy_price"
    CONSUMPTION_TOTAL = "consumption_total"
    PRODUCTION_TOTAL = "production_total"
    ENERGY_IN = "energy_in"
    ENERGY_OUT = "energy_out"
    ENERGY_IN_L1 = "energy_in_l1"
    ENERGY_IN_L2 = "energy_in_l2"
    ENERGY_IN_L3 = "energy_in_l3"
    ENERGY_IN_TOTAL = "energy_in_total"
    ENERGY_OUT_L1 = "energy_out_l1"
    ENERGY_OUT_L2 = "energy_out_l2"
    ENERGY_OUT_L3 = "energy_out_l3"
    ENERGY_OUT_TOTAL = "energy_out_total"
    POWER = "power"
    ELECTRIC_VEHICLE = "electric_vehicle"
    ENERGY = "energy"
    STATE_OF_CHARGE = "state_of_charge"
    SESSION_STATUS = "session_status"
    FLEXIBILITY = "flexibility"
    ENERGY_CONSUMPTION_PREDICTION = "energy_consumption_prediction"
    SOLAR_ENERGY_PREDICTION = "solar_energy_prediction"

class AggregationLabel(Enum):
    """How the readings of a period are reduced to one value (the 'aggregation_method' of the message)."""
    SUM = "sum"
    AVERAGE = "average"
    LAST_VALUE = "last_value"

class ActLabel(Enum):
    POWER = "power"

PARAMETER_TYPE_EXPECTED_TYPES: Dict[str, Type] = {
    ParamLabel.ENERGY_PRICE.value: float,
    ParamLabel.CONSUMPTION_TOTAL.value: float,
    ParamLabel.PRODUCTION_TOTAL.value: float,
    ParamLabel.ENERGY_IN.value: float,
    ParamLabel.ENERGY_OUT.value: float,
    ParamLabel.ENERGY_IN_L1.value: float,
    ParamLabel.ENERGY_IN_L2.value: float,
    ParamLabel.ENERGY_IN_L3.value: float,
    ParamLabel.ENERGY_IN_TOTAL.value: float,
    ParamLabel.ENERGY_OUT_L1.value: float,
    ParamLabel.ENERGY_OUT_L2.value: float,
    ParamLabel.ENERGY_OUT_L3.value: float,
    ParamLabel.ENERGY_OUT_TOTAL.value: float,
    ParamLabel.POWER.value: float,
    ParamLabel.ELECTRIC_VEHICLE.value: str,
    ParamLabel.ENERGY.value: float,
    ParamLabel.STATE_OF_CHARGE.value: float,
    ParamLabel.SESSION_STATUS.value: str,
}
