from functools import total_ordering

from app.entity_handlers.entity_handler_base import EntityHandlerBase
from app.utils.labels import Label


class EVChargerHandler(EntityHandlerBase):
    label = Label.EV_CHARGER.value

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        ev_chargers_entities = self._entities_ids.get(EVChargerHandler.label)

        # List to collect data from EV charging sessions
        charging_sessions = {}

        # Process each EV charger entity if available
        if ev_chargers_entities:
            for ev_charger_id in ev_chargers_entities:
                # Retrieve the EV charger data from the overall dataset
                ev_charger = all_data.get(ev_charger_id)

                power = 0
                electric_vehicle = ''

                if ev_charger:
                    # Access the 'data' payload for this EV charger
                    ev_charger_data = ev_charger.get('data')

                    power_array = ev_charger_data.get('power')

                    if power_array and isinstance(power_array, list):
                        power_list = ev_charger_data.get('power')

                        values = [p.get('value') for p in power_list if isinstance(p, dict) and 'value' in p]

                        if values:
                            power = sum(values) / len(values)

                    energy_in_array = ev_charger_data.pop('energy_in',[])

                    # If energy input data is present
                    if energy_in_array and isinstance(energy_in_array, list):
                        total_energy_in = 0
                        # Convert time interval from seconds to hours
                        time_interval_in_hours = self._time_interval / 3600

                        for ei in energy_in_array:
                            total_energy_in += ei.get('value', 0)

                        # Compute average power: energy divided by time
                        power = total_energy_in / time_interval_in_hours


                    if ev_charger_data.get('electric_vehicle'):
                        electric_vehicle = ev_charger_data.get('electric_vehicle')


                # Append processed charger session to the results list
                charging_sessions.update({ev_charger_id : {
                    'power' : power,
                    'electric_vehicle' : electric_vehicle,
                }})

        # Attach processed EV charger sessions to the message
        message["charging_sessions"] = charging_sessions

        # TODO: Implement logic to associate flexibility data with charging sessions.
        # Each session should contain user_id or VIN, and flexibility data must be mapped using this identifier.

    # TODO: If no charger data is found, should we retrieve the latest known session
    # and attach the flexibility entry associated with the user linked to that session?
    def fallback(self, entity_id, last_known_data):
        # Return a default EV charger session if no valid data is available
        return {
            'timestamp': 0,
            'data': {
                "power": 0.0,
                "electric_vehicle": ''
            }
        }
