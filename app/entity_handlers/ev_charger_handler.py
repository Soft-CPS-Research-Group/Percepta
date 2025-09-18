from app.entity_handlers.entity_handler_base import EntityHandlerBase


class EVChargerHandler(EntityHandlerBase):
    label = "ev_charger"

    def __init__(self, repository, entities_ids, configurations, logger):
        super().__init__(repository, entities_ids, configurations, logger)

    def process(self, message, all_data):
        ev_chargers_entities = self._entities_ids.get(EVChargerHandler.label)

        # List to collect data from EV charging sessions
        charging_sessions = []

        # Process each EV charger entity if available
        if ev_chargers_entities:
            for ev_charger_id in ev_chargers_entities:
                # Retrieve the EV charger data from the overall dataset
                ev_charger = all_data.get(ev_charger_id)

                if ev_charger:
                    # Access the 'data' payload for this EV charger
                    ev_charger_data = ev_charger.get('data')

                    # If energy input data is present
                    if ev_charger_data.get('energy_in'):
                        # Convert time interval from seconds to hours
                        time_interval_in_hours = self._time_interval / 3600

                        # Compute average power: energy divided by time
                        power = ev_charger_data.pop('energy_in') / time_interval_in_hours

                        # Store the calculated power in the charger data
                        ev_charger_data['power'] = power

                    # Append processed charger session to the results list
                    charging_sessions.append(ev_charger_data)

                    # Mark the message as 'generated' if data was not directly measured
                    if ev_charger.get('generated') == 1:
                        message["generated"] = 1

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
                "power": 0,
                "user_id": ""
            },
            'generated': 1
        }
