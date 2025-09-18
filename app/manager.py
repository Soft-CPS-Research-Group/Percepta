import json
import datetime
import copy
import time
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Lock
from app.utils.data import DataSet

class Manager:
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

        # TODO será que isto é mesmo necessário? Isto garante que todos os campos estão presentes mesmo que a zero
        self._algorithm_format = configurations.get('algorithm_attributes')
        self._logger = logger

        self._timer_ended = Lock()

        self._message = {}

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
        # Acquire the lock or condition to safely proceed with processing
        self._timer_ended.acquire()
        self._logger.info(f"Lock acquired.")

        # Fill in missing data if necessary
        self._verify_and_replace_missing_data()
        self._logger.info(f"Data verification completed.")

        # Format data for the prediction model
        self._format_data()
        self._logger.info(f"Data formatting completed.")

        # TODO publicar no RabbitMQ?
        # Perform prediction
        self._predictor.predict(self._message)

        # Clear the dictionary for the next cycle
        self._dict.clear()

        # Release the lock or condition
        self._timer_ended.release()


    #TODO se o handler não existir fazer substituição normal
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

    #TODO se o handler não existir fazer formatação normal
    def _format_data(self):
        # Get the current timestamp in UTC without microseconds
        timestamp = datetime.datetime.now(datetime.timezone.utc).replace(microsecond=0)

        # Deep copy the algorithm format template to prepare the message
        self._message = copy.deepcopy(self._algorithm_format)

        # Set the current timestamp in the message
        self._message['timestamp'] = timestamp
        # Iterate over all labels in the pre-built label-to-IDs mapping
        for label in self._entities_ids_by_label.keys():
            if label not in self._message:
                # Get the corresponding handler for the label
                handler = self._entities_handlers.get(label)
                print(f"{label}, {handler}")

                # Use the handler to process and update the message
                # Pass the current message, the data dictionary, and the list of entity IDs for this label
                handler.process(self._message, self._dict)

        # Print the final message prepared for the AI model (for debugging)
        print(f"Message to the AI Model: {self._message}\n")


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

        # Start the scheduler to begin executing jobs
        self._scheduler.start()

