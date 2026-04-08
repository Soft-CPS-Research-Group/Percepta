from enum import Enum

class Label(Enum):
    GRID_METER = "grid_meter"
    PV_PANEL = "pv_panel"
    BATTERY = "battery"
    EV_CHARGER = "ev_charger"
    EV = "electric_vehicle"
    ENERGY_PRICE = "energy_price"
    CONSUMPTION_FORECAST_SERVICE = "consumption_forecast_service"
    PRODUCTION_FORECAST_SERVICE = "production_forecast_service"
