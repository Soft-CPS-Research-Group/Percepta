import time
from app.entity_handlers.entity_handler_base import EntityHandlerBase

class GridMeterHandler(EntityHandlerBase):
    label = "grid_meter"

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        # Initialize variables to store total non-shiftable load and battery energy
        non_shiftable_load = 0
        battery_energy = 0

        # Retrieve the list of grid meter entities and battery entities from the input data
        grid_meter_entities = self._entity_ids.get('grid_meter')
        batteries_entities = self._entity_ids.get('battery')

        # Sum up the energy consumption (energy_in) from each grid meter entity
        if grid_meter_entities:
            for grid_meter_entity in grid_meter_entities:
                grid_meter_values = all_data[grid_meter_entity]
                non_shiftable_load += grid_meter_values.get('data', 0).get('energy_in', 0)
                if grid_meter_values.get('generated') == 1:
                    message["generated"] = 1

        # Sum up the charging energy from each battery entity
        if batteries_entities:
            for battery_entity in batteries_entities:
                battery_values = all_data[battery_entity]
                battery_energy += battery_values.get('data', 0).get('battery_charging_energy', 0)

        # Calculate the net non-shiftable load by subtracting battery charging energy
        message["non_shiftable_load"] = non_shiftable_load - battery_energy

    def fallback(self, device_id, substitute_dict):
        # Try to get substitute data for the device
        device_substitute = substitute_dict.get(device_id)

        if device_substitute:
            return device_substitute

        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # If not valid or no substitute found, return fallback default
        return {
            'timestamp': 0,
            'data': {
                        "energy_in": 0,
                    },
            'generated': 1
        }