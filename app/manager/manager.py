import json
import datetime
import copy
import threading
import time
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Condition
from app.utils.data import DataSet
from zoneinfo import ZoneInfo
from app.manager.harmonizer import Harmonizer


class Manager:
    def __init__(self, environment, environment_specs, entities_ids_by_label, time_series_repository, aggregator, output_handler, entities_handlers, configurations, logger):
        self._time_interval = DataSet.calculate_interval(configurations.get('frequency'))
        self._grace_period = DataSet.calculate_interval(configurations.get('grace_period')) # TODO definir default considerando o time_interval
        self._start_sched()
        self._environment = environment
        self._entities = environment_specs.get('entities')
        self._entities_ids_by_label = entities_ids_by_label
        self._output_handler = output_handler
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

        self._timer_ended = Condition()

    def new_message(self, messages):
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
                    value = message['value']

                    if isinstance(value, dict):
                        for param, readings in value.items():
                            if isinstance(readings, list):
                                for reading in readings:
                                    if 'timestamp' in reading:
                                        ts_str = reading['timestamp']
                                        reading['timestamp'] = datetime.datetime.strptime(ts_str, "%Y-%m-%d %H:%M:%S %z")

                    self._dict[entity_id] = {
                        'timestamp': timestamp,
                        'data': value,
                        'generated': False
                    }
                    # {"entity_id" : {"timestamp": timestamp, "data": {"param_1" : [{"timestamp": timestamp, "value" : value}, {}...]}, {}...}

            return True  # Return True if the operation succeeds
        except Exception as e:
            # Log any exception that occurs and return False
            self._logger.error(f"An unexpected error occurred: {e} {messages_json}")
            return False

    def _send(self, scheduled_run_time=None):
        if scheduled_run_time is None:
            return

        self._timestamp = scheduled_run_time

        if self._grace_period > 0:
            time.sleep(self._grace_period) # Isto só funciona bem se o processamento demorar menos que o time_interval - grace_period

        self._send_event.clear()

        # Acquire the lock or condition to safely proceed with processing
        with self._timer_ended:
            operation_start_timestamp = datetime.datetime.now()
            self._period_harmonizer(self._dict)

            # Fill in missing data if necessary
            self._verify_and_replace_missing_data(self._dict)

            # Format data for the prediction model
            message = self._format_data(self._dict)

            self._aggregator.aggregate(message)
            # Perform prediction
            self._output_handler.message_handler(message)

            # Clear the dictionary for the next cycle
            self._dict.clear()
            operation_end_timestamp = datetime.datetime.now()

            '''self._logger.info(
                f"\nStart: {operation_start_timestamp} End: {operation_end_timestamp}\n"
                f"Operation duration: {operation_end_timestamp - operation_start_timestamp}"
            )'''
            self._send_event.set()

            self._timer_ended.notify_all()

    def _period_harmonizer(self, data):

        period_start_time = self._timestamp - datetime.timedelta(seconds=self._time_interval)

        for entity_id, entity_values in self._entities.items():

            if data.get(entity_id) is None:
                continue

            entity_params = data.get(entity_id).get('data')
            #self._logger.info(f"ENTITY PARAMS: {entity_params}")
            # TODO: Isto serve para não dar erro quando o parâmetro está a NaN
            for param, param_data in entity_params.items():
                if isinstance(param_data, list):
                    temporal_behavior = entity_values.get('parameters', {}).get(param, {}).get('temporal_behavior', None)

                    if temporal_behavior is not None:

                        # Replace the original list in the dictionary
                        data[entity_id]['data'][param] = self._harmonizer.period_harmonizer(f"{entity_id}_{param}",period_start_time, self._timestamp, temporal_behavior, param_data)


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
                # Store a copy of valid data for potential substitution later and change generated to True
                self._substitute_dict[entity_id] = copy.deepcopy(data[entity_id])
                self._substitute_dict[entity_id]['generated'] = True

    #TODO se o handler não existir fazer formatação normal
    def _format_data(self, data) -> dict:
        # Set the current timestamp in the message
        message = {
            'timestamp' : self._timestamp,
            'observations' : {},
            'forecasts' : {}
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

        def job_wrapper():
            """
            Wrapper to extract precise scheduled execution time.
            Subtracts interval from next_run_time to find current intended trigger time.
            """
            # Retrieve specific job instance via unique identifier
            job = self._scheduler.get_job(job_id='send_data_job')

            if job and job.next_run_time:
                # Calculate time this specific run was meant to occur
                # e.g. if next run is 10:00:10, current intended run is 10:00:05
                next_run_tz = job.next_run_time.astimezone(self._tz)
                run_time = next_run_tz - datetime.timedelta(seconds=self._time_interval)
            else:
                # Fallback to current clock time if job metadata is inaccessible
                run_time = datetime.datetime.now(self._tz)

            # Execute main send logic with synchronised timestamp
            self._send(scheduled_run_time=run_time)

        cron_kwargs = DataSet.get_cron_expressions(self._time_interval)
        # Add a cron job to the scheduler that triggers the _send method at every interval_minutes
        self._scheduler.add_job(
            job_wrapper,
            'cron', # It does not work with values lower than 1 second. TODO ver alternativas que o permitam mas ter atenção ao tempo que o processo demora
            coalesce=True,  # Combine missed job runs into one if delayed
            id='send_data_job',
            **cron_kwargs
        )

        self._scheduler.configure(wakeup_interval=0.1)

        # Start the scheduler to begin executing jobs
        self._scheduler.start()
