import copy
import datetime
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

ENERGY_TARIFFS_PARAMETERS = ["energy_price"]

class EnergyPriceHandler(EntityHandlerBase):
    label = Label.ENERGY_PRICE.value

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, environment_specs, configurations, logger):
        super().__init__(repository, entities_ids, environment_specs, configurations, logger)

    def process(self, message, all_data):
        # Retrieve the list of grid meter entities from the input data
        energy_price_entities = self._entities_ids.get(Label.ENERGY_PRICE.value, []) # Retrieves all the entities with the "energy_price" label

        energy_tariffs = {}

        if energy_price_entities:

            for energy_price_id in energy_price_entities:
                energy_price_values = all_data.get(energy_price_id, {})
                data = energy_price_values.get('data', {})

                if data:
                    entity_summary = {}
                    for param in ENERGY_TARIFFS_PARAMETERS:
                        # Sum the values if the parameter exists and is a list
                        values_list = data.get(param, [])
                        if isinstance(values_list, list):
                            values_list.sort(key=lambda x: x.get("timestamp", 0))
                            final_list = [item.get("value", 0) for item in values_list]
                        else:
                            final_list = []

                        entity_summary[param] = {"values": final_list, "measurement_unit": "€/kWh",  "horizon_seconds": len(final_list)*15*60, "frequency_seconds": 15*60} # TODO alterar para usar dados do ficheiro de configuração

                    entity_summary['generated'] = energy_price_values.get('generated', True)
                    energy_tariffs[energy_price_id] = entity_summary

        message["observations"]["energy_tariffs"] = energy_tariffs

    def fallback(self, device_id, substitute_dict):
        device_substitute = copy.deepcopy(substitute_dict.get(device_id))

        if not device_substitute:
            self._logger.warning(f"Device not found in substitute_dict: {device_id}.")
            return self._get_default_fallback()

        periodicity_min = 15  # Default
        entity_specs = self._environment_specs.get('entities', {}).get(device_id, {})
        parameters = entity_specs.get('parameters', {})

        target_param_data = {}
        for param_name, param_info in parameters.items():
            if param_info.get('label') == Label.ENERGY_PRICE.value:
                target_param_data = param_info
                break

        if target_param_data:
            periodicity_info = target_param_data.get('temporal_behavior', {}).get('periodicity', {})
            if periodicity_info.get('unit') == 'min':
                periodicity_min = periodicity_info.get('value', 15)

        data_list = device_substitute.get('data', {}).get('energy_price', [])
        now = datetime.datetime.now(self._tz)

        if data_list:
            data_list.sort(key=lambda x: x.get("timestamp", 0))

            first_entry_time = data_list[0].get('timestamp')


            expiry_time = first_entry_time + datetime.timedelta(minutes=periodicity_min)

            if now < expiry_time:
                device_substitute['generated'] = False
            else:
                remaining_data = [
                    item for item in data_list
                    if item.get('timestamp') + datetime.timedelta(minutes=periodicity_min) > now
                ]
                device_substitute['data']['energy_price'] = remaining_data
                device_substitute['generated'] = True
        else:
            device_substitute['generated'] = True

        return device_substitute

    def _get_default_fallback(self):
        return {
            'timestamp': datetime.datetime.now(self._tz),
            'data': {'energy_price': []},
            "generated": True
        }