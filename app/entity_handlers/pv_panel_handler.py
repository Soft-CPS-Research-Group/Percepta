from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

class PVPanelHandler(EntityHandlerBase):
    label = Label.PV_PANEL.value

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        # Variable to accumulate total solar generation across all panels
        total_solar_generation = 0

        # Retrieve the list of PV panel entities assigned to this handler
        pv_panel_entities = self._entities_ids.get('pv_panel')

        pv_panels = {}

        # Iterate through each panel and add its solar generation value
        if pv_panel_entities:
            for pv_panel_entity in pv_panel_entities:
                sum_aux = 0
                pv_panel_values = all_data.get(pv_panel_entity, {})
                solar_generation = pv_panel_values.get('data', {}).get('energy', [])
                if isinstance(solar_generation, list):

                    for sg in solar_generation:
                        sum_aux += sg.get('value', 0)

                    total_solar_generation += sum_aux

                pv_panels.update({pv_panel_entity: {"energy": total_solar_generation}})

        message['pv_panels'] = pv_panels
        # Include the final solar generation total in the message payload
        message["solar_generation"] = total_solar_generation

    def fallback(self, device_id, substitute_dict):

        # Attempt to retrieve substitute data for the given device
        device_substitute = substitute_dict.get(device_id)

        if device_substitute:
            return device_substitute

        # Log a warning if fallback data is unavailable or outdated
        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Default data will be used instead.")

        # Provide default fallback structure if no suitable substitute exists
        return {
            'timestamp': 0,
            'data': {
                "solar_generation": 0,
            }
        }
