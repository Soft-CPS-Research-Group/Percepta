import copy
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

class EVHandler(EntityHandlerBase):
    label = Label.EV.value

    # TODO: Consider relocating this threshold—might make more sense to define it per device, not globally by label
    data_deadline_seconds = 7200  # Time limit in seconds (2 hours)

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        ev_entities = self._entities_ids.get(EVHandler.label)
        # List to collect data from ev entities
        evs = {}

        if ev_entities:
            # Loop through all configured ev IDs
            for ev_id in ev_entities:
                ev = all_data.get(ev_id)

                if ev:
                    # Add ev data to the results list
                    evs.update({ev_id: ev.get('data')})

        # Attach ev data to the outgoing message
        message["electric_vehicles"] = evs


    def fallback(self, device_id, substitute_dict):
        # Capture current timestamp in seconds

        # Attempt to retrieve substitute data for the given device
        device_substitute = copy.deepcopy(substitute_dict.get(device_id))

        if device_substitute:
            return device_substitute

        # Log a warning if fallback data is unavailable or outdated
        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # Provide default fallback structure if no suitable substitute exists
        return {
            'timestamp': 0,
            'data': {
                'SoC': None,
                'flexibility': {
                    'estimated_soc_at_arrival': None,
                    'estimated_soc_at_departure': None,
                    'estimated_time_at_arrival': '',
                    'estimated_time_at_departure': '',
                    'charger': '',
                    'mode': ''
                },
            }
        }
