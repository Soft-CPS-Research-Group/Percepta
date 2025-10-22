import copy
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

class EnergyPriceHandler(EntityHandlerBase):
    label = Label.ENERGY_PRICE.value

    # TODO perceber onde meter isto, secalhar faz mais sentido por dispositivo em concreto e não por label
    data_deadline_seconds = 7200  # 2 hours

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):

        # Retrieve the list of grid meter entities from the input data
        energy_price_entities = self._entities_ids.get('energy_price')
        energy_price = 0

        if energy_price_entities:
            energy_price_values = all_data[energy_price_entities[0]]
            energy_price_list = energy_price_values.get('data', 0).get('energy_price', [])

            if isinstance(energy_price_list, list):
                energy_price = energy_price_list[0].get("value", 0)
        message["energy_price"] = energy_price


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
                        "energy_price": 0,
                    }
        }