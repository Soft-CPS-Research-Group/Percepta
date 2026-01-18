import json
import datetime
import copy
import threading
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Condition
from app.utils.data import DataSet
from zoneinfo import ZoneInfo
from app.manager.harmonizer import Harmonizer


class Manager:
    def __init__(self, environment, environment_specs, entities_ids_by_label, time_series_repository, aggregator, predictor, entities_handlers, configurations, logger):
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
        self._configurations = configurations
        self._tz = self._set_time_zone()
        self._send_event = threading.Event()
        self._send_event.set()
        self._aggregator = aggregator

        self._logger = logger
        self._harmonizer = Harmonizer(self._time_interval, self._logger)

        #self._logger.info("ENTREI AQUI!\n")

        self._timer_ended = Condition()

    def new_message(self, messages):

        #self._logger.info("ENTREI AQUIIIII\n")
        # Decode the incoming message bytes to a UTF-8 string
        messages_decode = messages.decode('utf-8')

        # Parse the JSON string into a Python list
        messages_json = json.loads(messages_decode)

        try:

            with self._timer_ended:

                while not self._send_event.is_set():
                    self._timer_ended.wait(timeout=10)

                # Iterate over each message in the decoded JSON list
                for message in messages_json:
                    entity_id = str(message['id'])  # Extract and convert the message ID to string
                    timestamp = message['timestamp']  # Extract the timestamp
                    value = message['value']  # Extract the value

                    # Store the data in the dictionary using the ID as key
                    self._dict[entity_id] = {'timestamp': timestamp, 'data': value}
                    self._logger.info(f"{entity_id} : {json.dumps(self._dict[entity_id], indent=4)}")


            return True  # Return True if the operation succeeds
        except Exception as e:
            # Log any exception that occurs and return False
            self._logger.error(f"An unexpected error occurred: {e} {messages_json}")
            return False

    def _send(self):

        self._send_event.clear()

        # Acquire the lock or condition to safely proceed with processing
        with self._timer_ended:
            # Format timestamp using the configured timezone
            self._timestamp = datetime.datetime.now(self._tz)
            self._period_harmonizer(self._dict)

            # Fill in missing data if necessary
            self._verify_and_replace_missing_data(self._dict)

            # Format data for the prediction model
            message = self._format_data(self._dict)

            self._aggregator.aggregate(message)
            # Perform prediction
            self._predictor.predict(message)

            # Print the final message prepared for the AI model (for debugging)
            self._logger.info(f"Message to the AI Model: {message}\n")
            # Clear the dictionary for the next cycle
            self._dict.clear()
            self._send_event.set()

            self._timer_ended.notify_all()

    def _period_harmonizer(self, data):

        period_start_time = self._timestamp.replace(second=0, microsecond=0)
        period_end_time = period_start_time + datetime.timedelta(seconds=self._time_interval)

        for entity_id, entity_values in self._entities.items():

            if data.get(entity_id) is None:
                continue

            entity_params = data.get(entity_id).get('data')
            print(f"ENTITY PARAMS: {entity_params}\n")
            # TODO: Isto serve para não dar erro quando o parâmetro está a NaN
            for param, param_data in entity_params.items():
                if isinstance(param_data, list):
                    temporal_behavior = entity_values.get('parameters').get(param).get('temporal_behavior')

                    if temporal_behavior is not None:

                        # Replace the original list in the dictionary
                        data[entity_id]['data'][param] = self._harmonizer.period_harmonizer(f"{entity_id}_{param}",period_start_time, period_end_time, temporal_behavior, param_data)


    #TODO meter isto num ficheiro para reutilizar pois também é usado por pelo menos um tradutor
    def _set_time_zone(self) -> ZoneInfo:
        # Get the current timestamp in UTC without microseconds
        tz_name = self._configurations.get("timezone", "UTC")
        try:
            return ZoneInfo(tz_name)
        except Exception:
            self._logger.warning(f"Invalid timezone '{tz_name}', falling back to UTC")
            return ZoneInfo("UTC")

    def _verify_and_replace_missing_data(self, data):
        # Iterate over all known entities
        for entity_id, entity_values in self._entities.items():
            label = entity_values.get("label")
            handler = self._entities_handlers.get(label)

            if not handler:
                self._logger.warning(f"No handler found for entity {entity_id}, skipping fallback.")
                continue

            # If no data was found for this entity in the current data dict
            if entity_id not in data.keys():
                self._logger.warning(f"Entity {entity_id} was not found.")
                # Use fallback data provided by the appropriate handler
                data[entity_id] = handler.fallback(entity_id, self._substitute_dict)
            else:
                # Store a copy of valid data for potential substitution later
                self._substitute_dict[entity_id] = copy.deepcopy(data[entity_id])

    #TODO se o handler não existir fazer formatação normal
    def _format_data(self, data) -> dict:
        # Set the current timestamp in the message
        message = {
            'timestamp' : self._timestamp
        }

        # Iterate over all labels in the pre-built label-to-IDs mapping
        for handler in self._entities_handlers.values():
            # Use the handler to process and update the message
            # Pass the current message, the data dictionary, and the list of entity IDs for this label
            handler.process(message, data)

        return message

    def stop(self):
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
            second=10,  # Run 10 seconds after the start of the minute
            misfire_grace_time=10,  # Allow a 10-second window to catch missed jobs
            coalesce=True  # Combine missed job runs into one if delayed
        )
        self._scheduler.configure(wakeup_interval=0.1)

        # Start the scheduler to begin executing jobs
        self._scheduler.start()
