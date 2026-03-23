import copy
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

ENERGY_TARIFFS_PARAMETERS = ["energy_price"]

class EnergyPriceHandler(EntityHandlerBase):
    label = Label.ENERGY_PRICE.value

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        # Retrieve the list of grid meter entities from the input data
        energy_price_entities = self._entities_ids.get(Label.ENERGY_PRICE.value, []) # Retrieves all the entities with the "energy_price" label
        energy_tariffs = {}

        for energy_price_entity_id in energy_price_entities:
            energy_price_values = all_data.get(energy_price_entity_id, {})
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

                energy_tariffs[energy_price_entity_id] = entity_summary

        message["observations"]["energy_tariffs"] = energy_tariffs


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
                        'energy_price': [],
                    }
        }