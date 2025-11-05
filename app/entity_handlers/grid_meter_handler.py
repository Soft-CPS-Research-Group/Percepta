import copy
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

class GridMeterHandler(EntityHandlerBase):
    label = Label.GRID_METER.value

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):

        # Retrieve the list of grid meter entities from the input data
        grid_meter_entities = self._entities_ids.get('grid_meter')

        grid_meters = {}

        # Sum up the energy consumption (energy_in) from each grid meter entity
        if grid_meter_entities:
            for grid_meter_entity in grid_meter_entities:
                grid_meter_values = all_data[grid_meter_entity]
                energy_in = grid_meter_values.get('data', 0).get('energy_in', [])

                sum_aux = 0

                if isinstance(energy_in, list):
                    for ei in energy_in:
                        sum_aux += ei.get("value", 0)

                    grid_meters.update({grid_meter_entity: {"energy_in" : sum_aux}})

        message["grid_meters"] = grid_meters

    def fallback(self, device_id, substitute_dict):
        # Try to get substitute data for the device
        device_substitute = copy.deepcopy(substitute_dict.get(device_id))

        if device_substitute:
            return device_substitute

        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # If not valid or no substitute found, return fallback default
        return {
            'timestamp': 0,
            'data': {
                        'energy_in': None,
                    }
        }