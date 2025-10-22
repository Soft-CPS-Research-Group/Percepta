import copy
from datetime import datetime
from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label

class BatteryHandler(EntityHandlerBase):
    label = Label.BATTERY.value

    # TODO: Consider relocating this setting—might make more sense to associate with a specific device rather than a general label
    data_deadline_seconds = 7200  # Threshold in seconds (2 hours)

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        # Retrieve the list of battery entities from the input data
        batteries_entities = self._entities_ids.get(BatteryHandler.label)
        # List to collect data from battery entities
        batteries = {}

        if batteries_entities:
            # Loop through all configured battery IDs
            for battery_id in batteries_entities:
                battery = all_data.get(battery_id)

                total_battery_changing_energy = 0.0
                last_soc = 0

                if battery:
                    # Add battery data to the results list
                    battery_data = battery.get('data')
                    battery_energy_array = battery_data.get('battery_charging_energy')
                    if battery_energy_array and isinstance(battery_energy_array, list):
                        for be in battery_energy_array:
                            total_battery_changing_energy += be.get('value')

                    soc_array = battery_data.get('state_of_charge')
                    if soc_array and isinstance(soc_array, list):
                        last_soc = max(
                            soc_array,
                            key=lambda x: datetime.strptime(x['timestamp'], "%Y-%m-%d %H:%M:%S %z")
                        ).get('value')


                batteries.update({battery_id: {
                    "energy_in": total_battery_changing_energy,
                    "last_soc": last_soc,
                }})

        # Attach battery data to the outgoing message
        message["batteries"] = batteries


    def fallback(self, device_id, substitute_dict):
        # Attempt to retrieve substitute data for the missing device
        device_substitute = copy.deepcopy(substitute_dict.get(device_id))

        if device_substitute:
           return device_substitute

        # Log warning if no substitute found or if data is outdated
        self._logger.warning(f"Device not found in substitute_dict: {device_id}. Using default fallback data.")

        # Return default fallback data if substitute is unavailable or expired
        return {
            'timestamp': 0,
            'data': {
                "battery_charging_energy": 0.0,
                "state_of_charge": -1
            }
        }

    # TODO: For batteries, does it make sense to send values like 0,0? Even if the data is a day old,
    # would using older values be more meaningful than defaulting to zero?
