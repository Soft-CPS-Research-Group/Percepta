import json
import datetime
import copy
import threading
import time
from threading import Condition

from app.core.data_quality_handlers.data_quality_handler_base import DataQualityHandlerBase
from app.core.utils import DataSet


class Manager:
    def __init__(self, data_quality_handlers: list[DataQualityHandlerBase], configurations, logger):
        self._grace_period = DataSet.calculate_interval(configurations.get('grace_period'))
        self._dict = {}
        self._send_event = threading.Event()
        self._send_event.set()

        self._logger = logger
        # Store the pipeline of handlers injected by the Launcher
        self._data_quality_handlers = data_quality_handlers

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
                                        reading['timestamp'] = datetime.datetime.strptime(ts_str,
                                                                                          "%Y-%m-%d %H:%M:%S %z")

                    self._dict[entity_id] = {
                        'timestamp': timestamp,
                        'data': value,
                        'generated': False
                    }

            return True  # Return True if the operation succeeds
        except Exception as e:
            # Log any exception that occurs and return False
            self._logger.error(f"An unexpected error occurred: {e} {messages_json}")
            return False

    def process_window(self, scheduled_run_time=None):
        if scheduled_run_time is None:
            return None

        self._timestamp = scheduled_run_time

        # Wait for the grace period to elapse before locking the state
        if self._grace_period > 0:
            time.sleep(self._grace_period)

        self._send_event.clear()

        # Acquire the lock or condition to safely proceed with batch processing
        with self._timer_ended:

            # 1. DATA QUALITY PIPELINE (Chain of Responsibility)
            # Pass the accumulated data sequentially through each registered handler
            for handler in self._data_quality_handlers:
                try:
                    # Assumes handlers modify the internal dictionary in-place via 'handle'
                    handler.handle(self._dict)
                except Exception as e:
                    self._logger.error(
                        f"Manager | Error running data quality handler {handler.__class__.__name__}: {e}")

            # 2. STATE ISOLATION
            # Create a deep copy to safely return a static snapshot of the processed data
            to_send = copy.deepcopy(self._dict)

            # 3. INTERNAL STATE RESET
            # Clear the dictionary to prepare the buffer for the next time window
            self._dict.clear()

            # Release the event to allow the Accumulator thread to resume writing
            self._send_event.set()
            self._timer_ended.notify_all()

        return to_send