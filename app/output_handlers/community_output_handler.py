import copy
import threading
import json
import time
import datetime
from app.output_handlers.output_handler_base import OutputHandlerBase
from app.connectors.rabbitmq_connector import RabbitMQConnector
from app.rabbitMQ_publisher import RabbitMQPublisher


class CommunityOutputHandler(OutputHandlerBase):

    def __init__(self, environment, environment_specs, predictor, configurations, logger):
        OutputHandlerBase.__init__(self, environment, environment_specs, configurations, logger)
        self._id = environment

        self._dict = {}
        self._substitute_dict = {}
        self._send_event = threading.Event()
        self._send_event.set()
        self._timer_ended = threading.Condition()

        self._community_handler_configs = configurations.get('community')

        self._other_environments_ids = self._community_handler_configs.get('environments')
        self._other_environments_ids = [env for env in  self._other_environments_ids  if env != self._id]

        self._predictor = predictor

        self._publisher_server = self._community_handler_configs.get('publisher_settings')
        self._consumer_server = self._community_handler_configs.get('receiver_server')

        print(f"Other environments IDs: {self._other_environments_ids}")
        self._rabbitmq_connector = RabbitMQConnector(self._consumer_server)

        self._timeout = 3 #seconds

        self._setup_mq_publisher()

        # Threading control
        self._messaging_thread = None

        self._setup_mq_publisher()

        # Start the consumer service in a separate thread
        self._start_background_messaging()  # TODO ver como faço isto nos outros sitios

    def _start_background_messaging(self):
        """
        Initializes and starts the messaging thread.
        """
        self._logger.info("Starting background messaging service...")
        self._messaging_thread = threading.Thread(
            target=self._start_messaging_service,
            name=f"CommunityThread_{self._id}",
            daemon=True
        )
        self._messaging_thread.start()

    def stop(self):
        """
        Gracefully stops the messaging service and joins the thread.
        """
        self._logger.info("Stopping CommunityOutputHandler messaging service...")

        try:
            # Tell the connector to stop the consumer loop and disconnect
            if hasattr(self._rabbitmq_connector, 'stop_consuming_safely'):
                self._rabbitmq_connector.stop_consuming_safely()

        except Exception as e:
            self._logger.error(f"Error during RabbitMQ disconnection: {e}")

        # Wait for the thread to finish
        if self._messaging_thread and self._messaging_thread.is_alive():
            self._messaging_thread.join(timeout=5)
            self._logger.info("Messaging thread successfully joined.")

    def _setup_mq_publisher(self):
        """
        Prepara o publisher com o ID do ambiente injetado no nome da exchange.
        """
        try:
            # Deep copy para não estragar a config original
            mq_config = copy.deepcopy(self._community_handler_configs.get("publisher_settings", {}))

            # Dinamizar o nome da exchange: predictor_SaoMamede por exemplo
            topology = mq_config.get("topology", {})
            raw_name = topology.get("exchange_name", "")
            topology["exchange_name"] = raw_name.format(environment_id=self._id)

            self._rabbitmq_publisher = RabbitMQPublisher(mq_config, self._logger)
            self._logger.info(f"Output Handler MQ Publisher initialized for exchange: {topology['exchange_name']}")
        except Exception as e:
            self._logger.error(f"Failed to initialize Output Handler MQ Publisher: {e}")
            self._rabbitmq_publisher = None

    def _start_messaging_service(self):
        """
           Establish connection to RabbitMQ with retry logic.
           Declares the environment-specific queue and logs connection status.
           Raises an exception if maximum reconnection attempts are reached.
        """
        # Initialize the RabbitMQ connector with the internal message hub server

        self._rabbitmq_connector.connect()

        queue_name = f"queue_community_{self._id}"

        for ex_name in self._other_environments_ids:
            community_name = f"community_{ex_name}"

            self._rabbitmq_connector.declare_exchange(community_name)

            real_queue_name = self._rabbitmq_connector.declare_queue(
                queue_name,
                community_name
            )

            self._rabbitmq_connector.setup_consumer(real_queue_name, self._callback)

        self._rabbitmq_connector.start_listening()

    def _callback(self, ch, method, properties, body):
        """
        Rabbitmq callback.
        """
        with self._timer_ended:
            while not self._send_event.is_set():
                self._timer_ended.wait(timeout=10)

            body = json.loads(body)
            body["timestamp"] = datetime.datetime.now().isoformat()
            installation_id = body.pop("installation_id")

            self._dict.update({installation_id : body})
            self._substitute_dict.update({installation_id : body})

            self._logger.info(f"{self._id}: Received message: {body}")

            self._rabbitmq_connector.ack(method.delivery_tag)

    def _create_message(self, message):
        energy_in_total = 0
        energy_out_total = 0

        if "grid_meters" in message:
            grid_meters = message["grid_meters"]
            for gm in grid_meters.values():
                energy_in_total += gm["energy_in_total"]
                energy_out_total += gm["energy_out_total"]

        message_to_the_community = {
            "installation_id": self._id,
            "energy_in_total": energy_in_total,
            "energy_out_total": energy_out_total
        }

        return message_to_the_community

    def _recover_missing_data(self, reference_time):
        """Attempts to recover missing environment data from the substitute dictionary."""
        two_hours_ago = reference_time - datetime.timedelta(hours=2)

        for env_id in self._other_environments_ids:
            if env_id not in self._dict:
                if env_id in self._substitute_dict:
                    sub_data = self._substitute_dict[env_id]
                    sub_time = datetime.datetime.fromisoformat(sub_data["timestamp"])

                    if sub_time > two_hours_ago:
                        self._dict[env_id] = sub_data
                        self._logger.info(f"Recovered {env_id} from substitute (Time: {sub_time})")
                    else:
                        self._substitute_dict.pop(env_id)
                        self._logger.warning(f"Substitute data for {env_id} expired (>2h). Removed.")

    def _calculate_community_totals(self, own_data):
        """Summates energy totals from all available environments including its own."""
        total_in = float(own_data.get("energy_in_total", 0.0))
        total_out = float(own_data.get("energy_out_total", 0.0))

        for env_id in self._other_environments_ids:
            if env_id in self._dict:
                total_in += float(self._dict[env_id].get("energy_in_total", 0.0))
                total_out += float(self._dict[env_id].get("energy_out_total", 0.0))

        return total_in, total_out

    def _handler(self, message):
        """Main handler triggered for every new message from the local environment."""
        # 1. Prepare and send local data to others
        own_community_msg = self._create_message(message)
        if self._rabbitmq_publisher:
            self._rabbitmq_publisher.send_message(own_community_msg)

        # 2. Wait for incoming messages from other environments
        time.sleep(self._timeout)

        # 3. Synchronized processing block
        self._send_event.clear()
        with self._timer_ended:
            start_time = datetime.datetime.now()

            # Attempt to fill gaps with history
            self._recover_missing_data(start_time)

            # Aggregate community values
            total_in, total_out = self._calculate_community_totals(own_community_msg)

            # Update the local message and run prediction
            message["community"] = {
                "energy_in_total": total_in,
                "energy_out_total": total_out
            }
            self._predictor.predict(message)

            # Cleanup and release callback block
            self._dict.clear()
            self._send_event.set()
            self._timer_ended.notify_all()

            duration = datetime.datetime.now() - start_time
            self._logger.info(f"Community process finished in {duration}. Total In: {total_in} Total Out: {total_out}")