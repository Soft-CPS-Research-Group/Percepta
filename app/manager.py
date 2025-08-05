import json
import datetime
import copy
import time
from apscheduler.schedulers.background import BackgroundScheduler
from app.predictor import Predictor
from threading import Lock
from app.energy_price import EnergyPrice
from app.utils.data import DataSet

#
class Manager():
    def __init__(self, environment, environment_specs, entities_ids_by_label, time_series_repository, predictor, entities_handlers, configurations, logger):
        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))
        self._start_sched()
        self._environment = environment
        self._entities = environment_specs.get('entities')
        self._entities_ids_by_label = entities_ids_by_label
        self._predictor = predictor
        self._substitute_dict = {}
        self._dict = {}
        self._entities_handlers = entities_handlers
        self._time_series_repository = time_series_repository
        self._algorithm_format = configurations.get('AlgorithmAtributes')
        self._charger_session_format = configurations.get('ChargingSessionsFormat')
        self._logger = logger

        self._timer_ended = Lock()

        self._message = ''

        # TODO: Remove from here, use case specific
        self._energy_price_service = EnergyPrice(configurations, logger)

    # TODO: Remove from here, use case specific
    def energy_price(self):
        energy_price = self._energy_price_service.get_energy_price()
        self._message['electricity_pricing'] = energy_price

    def new_message(self, messages):
        # Decode the incoming message bytes to a UTF-8 string
        messages_decode = messages.decode('utf-8')

        # Parse the JSON string into a Python list
        messages_json = json.loads(messages_decode)
        try:
            with self._timer_ended:
                # Iterate over each message in the decoded JSON list
                for message in messages_json:

                    entity_id = str(message['id'])  # Extract and convert the message ID to string
                    timestamp = message['timestamp']  # Extract the timestamp
                    value = message['value']  # Extract the value

                    # Store the data in the dictionary using the ID as key
                    self._dict[entity_id] = {'timestamp': timestamp, 'data': value, 'generated': 0}
                    print(f"{entity_id} : {json.dumps(self._dict[entity_id], indent=4)}")


            return True  # Return True if the operation succeeds
        except Exception as e:
            # Log any exception that occurs and return False
            print(f"An unexpected error occurred: {e}")
            return False

    def _send(self):
        timeout = self._time_interval / 4  # Maximum time to wait for data (25% of the interval)
        poll_interval = 0.5  # How often to check if all data is received
        waited = 0  # Time already waited

        # TODO: Improve this waiting mechanism (e.g., make it event-driven instead of polling)
        while waited < timeout:
            # Check if self._dict contains data for all expected entities
            if all(entity_id in self._dict for entity_id in self._entities):
                break  # Exit loop if data is ready for all entities
            time.sleep(poll_interval)
            waited += poll_interval

        # Acquire the lock or condition to safely proceed with processing
        self._timer_ended.acquire()

        # Fill in missing data if necessary
        self._verify_and_replace_missing_data()

        # Format data for the prediction model
        self._format_data()

        # Perform prediction
        self._predictor.predict(self._message)

        # Clear the dictionary for the next cycle
        self._dict.clear()

        # Release the lock or condition
        self._timer_ended.release()


    def _verify_and_replace_missing_data(self):
        # Iterate over all known entities
        for entity_id, values in self._entities.items():
            label = values.get("label")
            handler = self._entities_handlers.get(label)

            # If no data was found for this entity in the current data dict
            if entity_id not in self._dict.keys():
                self._logger.warning(f"Entity {entity_id} was not found.")
                # Use fallback data provided by the appropriate handler
                self._dict[entity_id] = handler.fallback(entity_id, self._substitute_dict)
            else:
                # Store a copy of valid data for potential substitution later
                self._substitute_dict[entity_id] = copy.deepcopy(self._dict[entity_id])

                # Mark this data as not generated (i.e., valid real data)
                self._substitute_dict[entity_id]['generated'] = 1


    def _format_data(self):
        # Get the current timestamp in UTC without microseconds
        timestamp = datetime.datetime.now(datetime.UTC).replace(microsecond=0)

        # Deep copy the algorithm format template to prepare the message
        self._message = copy.deepcopy(self._algorithm_format)

        # Set the current timestamp in the message
        self._message['timestamp'] = timestamp

        # Iterate over all labels in the pre-built label-to-IDs mapping
        for label in self._entities_ids_by_label.keys():
            if label not in self._message:
                # Get the corresponding handler for the label
                handler = self._entities_handlers.get(label)

                # Use the handler to process and update the message
                # Pass the current message, the data dictionary, and the list of entity IDs for this label
                handler.process(self._message, self._dict)

        # TODO: Remove from here, use case specific
        self.energy_price()

        # Print the final message prepared for the AI model (for debugging)
        print(f"Message to the AI Model: {self._message}\n")


    def stop(self):
        # TODO: Remove from here, use case specific
        self._energy_price_service.stop()
        # Shutdown the scheduler to stop any scheduled jobs gracefully
        self._scheduler.shutdown()


    def _start_sched(self):
        # Initialize the background scheduler for periodic task execution
        self._scheduler = BackgroundScheduler()

        # Calculate the interval in minutes based on the configured time interval (assumed in seconds)
        interval_minutes = self._time_interval // 60

        # Add a cron job to the scheduler that triggers the _send method at every interval_minutes
        self._scheduler.add_job(
            self._send,
            'cron',
            minute=f'*/{interval_minutes}',  # Run every 'interval_minutes' minutes
            second=0,  # Run at the start of the minute
            misfire_grace_time=10,  # Allow a 10-second window to catch missed jobs
            coalesce=True  # Combine missed job runs into one if delayed
        )

        # Start the scheduler to begin executing jobs
        self._scheduler.start()

