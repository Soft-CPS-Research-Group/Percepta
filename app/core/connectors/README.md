# Connectors Subsystem

In the Percepta framework, **Connectors** are low-level network components purely dedicated to managing transport drivers, physical socket links, client sessions, and raw communication protocols (AMQP, HTTP, etc.). They act as the physical networking layer that powers the higher-level Input and Output Handlers.

---

## 1. System Architecture

The subsystem provides concrete transport wrappers split across two fundamental execution paradigms:

### Core Transport Concepts

* **Sync Connectors**: Implemented using blocking, thread-dependent socket libraries (such as `pika` for AMQP or `requests` for HTTP). They execute network I/O synchronously and are meant to be offloaded to dedicated OS background threads.
* **Async Connectors**: Implemented using non-blocking, asynchronous drivers (such as `aio-pika` for AMQP or `aiohttp` for HTTP). They register non-blocking socket tasks directly onto the active `asyncio` Event Loop. (TENHO DE ENTENDER O QUE É ISTO DO EVENT LOOP)
* **Connection Lifecycle**: Every connector exposes an operational contract to manage its underlying transport link:
* `.connect()`: Establishes physical sockets, performs handshakes, and initializes session pools.
* `.is_connected()`: Validates the live health state of active sockets and channels.
* `.close()`: Performs a clean, graceful teardown releasing socket buffers and persistent session pools.

---

## 2. Supported Connectors Reference Directory

### 2.1. AMQP Connectors (RabbitMQ)

#### 2.1.1. Synchronous Version (`SyncRabbitMQConnector`)

* **Driver**: `pika` (BlockingConnection & BlockingChannel)
* **Execution**: Blocking I/O (requires thread isolation)

```python
from app.core.connectors import SyncRabbitMQConnector

# 1. Define configuration & property dictionaries
exchange_properties = {
    "exchange_type": "topic",
    "durable": True,
    "auto_delete": False
}

queue_properties = {
    "durable": True,
    "exclusive": False
}

publish_properties = {
    "mandatory": False,
    "properties": {
        "delivery_mode": 2,
        "content_type": "application/json"
    }
}

consume_properties = {
    "auto_ack": False
}

# 2. Instantiate and connect
connector = SyncRabbitMQConnector(server_configurations=connection_dict, logger=logger)
connector.connect()

# 3. Declare infrastructure using property dictionaries
connector.declare_exchange(
    exchange_name="telemetry_exchange",
    **exchange_properties
)

queue_name = connector.declare_queue(
    queue_name="db_queue",
    exchange_name="telemetry_exchange",
    routing_key="telemetry.#",
    **queue_properties
)

# 4. Publish a message
payload = {"sensor_id": "meter_01", "voltage": 230.5}
connector.publish(
    routing_key="telemetry.data",
    message=payload,
    exchange_name="telemetry_exchange",
    **publish_properties
)

# 5. Register consumer callback and start listening loop (Blocking)
connector.setup_consumer(
    queue_name=queue_name,
    callback=on_message_callback,
    **consume_properties
)

connector.start_listening()  # Blocks the calling thread

# 6. Teardown
connector.close()

```

---

#### 2.1.2. Asynchronous Version (`AsyncRabbitMQConnector`)

* **Driver**: `aio-pika` (connect_robust & AbstractChannel)
* **Execution**: Non-blocking coroutines managed via `asyncio`

```python
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector

# 1. Define configuration & property dictionaries
exchange_properties = {
    "exchange_type": "topic",
    "durable": True,
    "auto_delete": False
}

queue_properties = {
    "durable": True,
    "exclusive": False
}

publish_properties = {
    "mandatory": False,
    "properties": {
        "delivery_mode": 2
    }
}

consume_properties = {
    "no_ack": False
}

# 2. Instantiate and connect asynchronously
connector = AsyncRabbitMQConnector(server_configurations=connection_dict, logger=logger)
await connector.connect()

# 3. Declare infrastructure asynchronously using property dictionaries
await connector.declare_exchange(
    exchange_name="telemetry_exchange",
    **exchange_properties
)

queue_name = await connector.declare_queue(
    queue_name="db_queue",
    exchange_name="telemetry_exchange",
    routing_key="telemetry.#",
    **queue_properties
)

# 4. Publish a message asynchronously
payload = {"sensor_id": "meter_01", "voltage": 230.5}
await connector.publish(
    routing_key="telemetry.data",
    message=payload,
    exchange_name="telemetry_exchange",
    **publish_properties
)

# 5. Register consumer callback asynchronously
consumer_tag = await connector.setup_consumer(
    queue_name=queue_name,
    callback=on_message_coroutine,
    **consume_properties
)

# 6. Teardown
await connector.close()

```

> Note on Protocol & Message Properties:
> The property dictionaries (exchange_properties, queue_properties, publish_properties, and consume_properties) accept any standard AMQP options supported natively by the underlying drivers (pika for synchronous execution or aio-pika for asynchronous execution). You can pass any additional keyword parameters recognized by these libraries (such as custom headers, expiration, reply_to, or plugin arguments) directly through these dictionaries.

#### 2.1.3. Server Connection Configuration Schema (`server_configurations`)

Both synchronous and asynchronous AMQP connectors accept the following configuration dictionary:

```json
{
  // [Optional] IP address or hostname of the RabbitMQ broker.
  // Default if omitted: None
  "host": "192.168.1.50",

  // [Optional] Network port for AMQP communication.
  // Default if omitted: 5672 (or 5671 when SSL/TLS is enabled)
  "port": 5672,

  // [Optional] Virtual Host used for message isolation on the broker.
  // Default if omitted: "/"
  "vhost": "/porto_hub",

  // [Optional] Heartbeat ping interval (in seconds) to keep the TCP connection alive.
  // Default if omitted: 60 (Sync/pika) or 600 (Async/aio-pika)
  "heartbeat": 600,

  // [Optional] Socket connection establishment timeout threshold (in seconds).
  // Default if omitted: 10
  "timeout": 10,

  // [Optional] Maximum number of unacknowledged messages delivered concurrently per worker (QoS).
  // Default if omitted: 10
  "prefetch_count": 10,

  // [Optional] Authentication credentials block.
  // Defaults if omitted: username=None, password=None (falls back to broker's guest/guest if unconfigured)
  "auth": {
    // [Optional] AMQP authentication username.
    // Default if omitted: None
    "username": "porto_consumer",

    // [Optional] AMQP authentication password.
    // Default if omitted: None
    "password": "portopasswordmq"
  },

  // [Optional] TLS/SSL transport security configuration block.
  // Default if omitted: Disabled (enabled=false)
  "ssl_conf": {
    // [Optional] Toggles SSL/TLS encrypted transport.
    // Default if omitted: false
    "enabled": false,

    // [Optional] Validates if the target hostname matches the server SSL certificate.
    // Default if omitted: false
    "check_hostname": false,

    // [Optional] Certificate verification strategy ("required" for valid certs, "none"/null to ignore/self-signed).
    // Default if omitted: "none" (CERT_NONE)
    "verify_mode": "none"
  }
}

```

---

### 2.2. HTTP Connectors

#### 2.2.1. Synchronous Version (`HTTPConnector`)

* **Driver**: `requests` (requests.Session pool)
* **Execution**: Synchronous blocking network calls

```python
from app.core.connectors import HTTPConnector

# 1. Instantiate and connect
connector = HTTPConnector(server_configurations=connection_dict, logger=logger)
connector.connect()

# 2. Fetch data (GET) via request
response = connector.request(
    method="GET",
    endpoint="/v1/telemetry/meter_01",
    timeout=4.0
)

# 3. Publish / Send data (POST / PUT) via request with payload configuration
payload = {"active_power": 115.4, "status": "OK"}
post_config = {
    "json": payload,
    "timeout": 5.0
}

post_response = connector.request(
    method="POST",
    endpoint="/v1/telemetry/ingress",
    **post_config
)

# 4. Teardown
connector.close()

```

#### 2.2.2. Asynchronous Version (`AsyncHTTPConnector`)

* **Driver**: `aiohttp` (ClientSession & TCPConnector pool)
* **Execution**: Non-blocking coroutines managed via `asyncio`

```python
from app.core.connectors.async_http_connector import AsyncHTTPConnector

# 1. Instantiate and connect asynchronously
connector = AsyncHTTPConnector(server_configurations=connection_dict, logger=logger)
await connector.connect()

# 2. Fetch data asynchronously (GET) via request
payload_or_text = await connector.request(
    method="GET",
    endpoint="/v1/telemetry/meter_01",
    timeout=4.0
)

# 3. Publish / Send data asynchronously (POST / PUT) via request with payload configuration
payload = {"active_power": 115.4, "status": "OK"}
post_config = {
    "json": payload,
    "timeout": 5.0
}

post_response = await connector.request(
    method="POST",
    endpoint="/v1/telemetry/ingress",
    **post_config
)

# 4. Teardown
await connector.close()

```

#### 2.2.3. Server Connection Configuration Schema (`server_configurations`)

Both synchronous and asynchronous HTTP connectors accept the following configuration dictionary:

```json
{
  // [Optional] Protocol scheme to construct base URL if 'url' is omitted.
  // Default if omitted: "http"
  "schema": "https",

  // [Optional*] Target server IP address or hostname. 
  // *Required if 'url' is not provided.
  // Default if omitted: None
  "host": "api.cleanwatts.energy",

  // [Optional] Target server network port.
  // Default if omitted: None (falls back to default HTTP/HTTPS ports)
  "port": 443,

  // [Optional*] Explicit base server URL (overrides 'schema', 'host', and 'port').
  // *Required if 'host' is not provided.
  // Default if omitted: Dynamically constructed from schema://host:port
  "url": "https://api.cleanwatts.energy",

  // [Optional] Toggles TLS/SSL server certificate verification.
  // Default if omitted: false
  "verify_ssl": true,

  // [Optional] Basic HTTP Authentication credentials block.
  // Default if omitted: None (no authentication attached)
  "auth": {
    // [Optional] Username for HTTP Basic Auth.
    // Default if omitted: None
    "username": "api_user",

    // [Optional] Password for HTTP Basic Auth.
    // Default if omitted: None
    "password": "api_password"
  },

  // [Optional] Default global headers applied to every HTTP request session.
  // Default if omitted: {"Content-Type": "application/json", "Accept": "application/json"} (Sync) / {"Accept": "application/json"} (Async)
  "headers": {
    "Authorization": "Bearer token_hash_abc",
    "Accept": "application/json"
  }
}

```




## 3. Implementing a New Connector

When adding support for a new protocol or network driver (e.g., MQTT, Kafka), follow these guidelines:

### Design Guidelines

1. **Self-Contained Protocol Logic**: All driver-specific imports (e.g., `aiomqtt`, `confluent_kafka`) and physical connection parameters must remain isolated inside the connector.
2. **Explicit Connection Management**: Implement `.connect()`, `.is_connected()`, and `.close()` to allow higher-level orchestrators (like the `InputHandlersLauncher`) to track and teardown sockets gracefully during system shutdown.
3. **Parity with Execution Paradigms**: If the protocol supports both blocking and non-blocking operations, create distinct Sync and Async classes (e.g., `SyncMQTTConnector` and `AsyncMQTTConnector`).
4. **Resilient Error Wrapping**: Catch underlying driver exceptions (such as network drops or socket timeouts) and raise custom subsystem exceptions (e.g., `MQTTErrorWrapper`) with clear diagnostic context logs.

---

### Step-by-Step Implementation Template (Async MQTT Example)

```python
import asyncio
from typing import Optional, Any
from app.core.utils.logger import LoggingUtils


class MQTTErrorWrapper(Exception):
    """Custom exception boundary for MQTT transmission errors."""
    pass


class AsyncMQTTConnector:
    """
    Asynchronous transport wrapper managing an MQTT client session.
    """

    _LOG_PREFIX: str = "Async MQTT |"

    def __init__(self, server_configurations: dict, logger: LoggingUtils) -> None:
        self._logger = logger
        self._host = server_configurations.get("host", "localhost")
        self._port = server_configurations.get("port", 1883)
        self._client: Optional[Any] = None

    async def connect(self) -> None:
        """Establishes the non-blocking client connection."""
        self._logger.info(f"{self._LOG_PREFIX} Connecting to MQTT broker at {self._host}:{self._port}...")
        try:
            # 1. Initialize client driver session here
            # self._client = aiomqtt.Client(hostname=self._host, port=self._port)
            # await self._client.connect()
            self._logger.info(f"{self._LOG_PREFIX} MQTT connection established successfully.")
        except Exception as e:
            error_msg = f"Failed to connect to MQTT broker: {e}"
            self._logger.error(f"{self._LOG_PREFIX} {error_msg}")
            raise MQTTErrorWrapper(error_msg) from e

    def is_connected(self) -> bool:
        """Returns True if the underlying client session is alive."""
        return self._client is not None

    async def close(self) -> None:
        """Gracefully disconnects and releases network sockets."""
        if self._client:
            self._logger.info(f"{self._LOG_PREFIX} Closing MQTT connection...")
            try:
                # await self._client.disconnect()
                self._logger.info(f"{self._LOG_PREFIX} MQTT connection closed cleanly.")
            except Exception as e:
                self._logger.error(f"{self._LOG_PREFIX} Error while closing MQTT connection: {e}")
            finally:
                self._client = None

```