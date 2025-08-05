import time
from app.entity_handlers.entity_handler_base import EntityHandlerBase

class PVPanelHandler(EntityHandlerBase):
    label = "pv_panel"

    # TODO: Consider relocating this threshold—might make more sense to define it per device, not globally by label
    data_deadline_seconds = 7200  # Time limit in seconds (2 hours)

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        # Variable to accumulate total solar generation across all panels
        total_solar_generation = 0

        # Retrieve the list of PV panel entities assigned to this handler
        pv_panel_entities = self._entity_ids.get('pv_panel')

        # Iterate through each panel and add its solar generation value
        if pv_panel_entities:
            for pv_panel_entity in pv_panel_entities:
                pv_panel_values = all_data.get(pv_panel_entity, {})
                total_solar_generation += pv_panel_values.get('data', {}).get('solar_generation', 0)

        # Include the final solar generation total in the message payload
        message["solar_generation"] = total_solar_generation

    def fallback(self, device_id, substitute_dict):
        # Capture current timestamp in seconds
        current_time = int(time.time())

        # Attempt to retrieve substitute data for the given device
        device_substitute = substitute_dict.get(device_id)

        if device_substitute:
            timestamp = device_substitute.get('timestamp', 0)

            # Validate if the substitute data falls within the acceptable freshness window
            if (current_time - timestamp) <= PVPanelHandler.data_deadline_seconds:
                return device_substitute

        # Log a warning if fallback data is unavailable or outdated
        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # Provide default fallback structure if no suitable substitute exists
        return {
            'timestamp': 0,
            'data': {
                "solar_generation": 0,
            },
            'generated': 1
        }
