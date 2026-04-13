import copy
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

GRID_METER_PARAMETERS = ['energy_in_l1', 'energy_in_l2', "energy_in_l3", "energy_in_total", "energy_out_l1", "energy_out_l2", "energy_out_l3", "energy_out_total"]

class GridMeterHandler(EntityHandlerBase):
    label = Label.GRID_METER.value

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, environment_specs, configurations, logger):
        super().__init__(repository, entities_ids, environment_specs, configurations, logger)

    def process(self, message, all_data):

        # Retrieve the list of grid meter entities from the input data
        grid_meter_entities = self._entities_ids.get('grid_meter', [])

        grid_meters = {}

        if grid_meter_entities:
            # Sum up the energy consumption (energy_in) from each grid meter entity
            for grid_meter_id in grid_meter_entities:
                grid_meter_values = all_data.get(grid_meter_id, {})
                data = grid_meter_values.get('data', {})

                if data:
                    entity_summary = {}
                    for param in GRID_METER_PARAMETERS:
                        # Sum the values if the parameter exists and is a list
                        values_list = data.get(param, [])
                        if isinstance(values_list, list):
                            accumulated_value = sum(item.get("value", 0) for item in values_list)
                            entity_summary[param] = accumulated_value
                        else:
                            entity_summary[param] = 0

                    entity_summary["generated"] = grid_meter_values.get('generated', True)
                    grid_meters[grid_meter_id] = entity_summary


        message["observations"]["grid_meters"] = grid_meters

    def fallback(self, device_id, substitute_dict):
        # Try to get substitute data for the device
        device_substitute = copy.deepcopy(substitute_dict.get(device_id))

        if device_substitute:
            return device_substitute

        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # If not valid or no substitute found, return fallback default
        return {
            'timestamp': 0,
            'data': {param: 0.0 for param in GRID_METER_PARAMETERS},
            "generated": True
        }