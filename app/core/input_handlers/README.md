# Input Handlers Subsystem

In the Percepta framework, **Input Handlers** are the entry modules that allow data into the system. They act as adapters for specific protocols: they connect to a network source, listen for incoming messages or pull them periodically, and pass the raw data directly to the application core for processing.

---

## 1. System Architecture

The subsystem consists of four main architectural building blocks:

### Core Ingress Concepts

* **`InputHandlersLauncher`**: The master orchestrator for all input handlers within the system. It maintains a unified registry of every available handler across all supported communication protocols. Passing a specific identifier string allows the launcher to dynamically resolve, configure, and return the correct input handler to ingest data. Additionally, it tracks these sessions to close all physical connections and sockets centralized during system shutdown.
* **`InputHandlerBase`**: The core interface and abstract contract that all technology-specific ingress handlers must inherit from.
* **`Connectors`**: Low-level network components purely dedicated to managing drivers, sessions, client states, and physical transport links.
* **Sync Connectors**: Uses blocking socket operations (such as `pika` for AMQP or `requests` for HTTP).
* **Async Connectors**: Uses non-blocking sockets (such as `aio-pika` for AMQP or `aiohttp` for HTTP).
* **`PayloadNormalizer`**: A utility that takes raw data coming from different sources and converts it into a single, standardized format used by the rest of the application. This ensures that core business logic doesn't need to care where the data came from or how the underlying protocol formatted it.
---

## 2. Initializing and Using an Existing Protocol


Depending on whether you are running a **Synchronous** or an **Asynchronous** pipeline, the initialization and execution lifecycle will differ as follows.

### 2.1. The Synchronous Paradigm (Sync)

The initialization is driven by a blocking call to the launcher. Once the handler is returned, its execution must be offloaded to a dedicated OS thread to prevent locking the application's primary thread.

#### Initialization & Execution (Sync)

```python
# 1. Build the synchronous input handler
input_handler_sync = launcher.build_input_handler(
    handler_type="<ProtocolIdentifierString>",     # e.g., "AsyncRabbitMQ" or "SyncHTTP"
    topology_setup=topology_dict,     # Passes structural routing rules, metadata, and infrastructure layouts
    callback=domain_processing_callback,     # Injects the domain processing strategy triggered upon frame arrival
    server_configurations=connection_dict     # Supplies the network credentials and physical session attributes
)

# 2. Offload the blocking execution loop to a dedicated OS background thread
import threading

consumer_thread = threading.Thread(
    target=input_handler_sync.start,
    daemon=True,
    name="ingress_sync_stream_worker"
)
consumer_thread.start()

```

#### Graceful Termination Sequence (Sync)

```python
# 1. Stop the logical processing loop for all created handlers
# This triggers internal shutdown signals 
input_handler.stop() 

# 2. Block and synchronize threads to allow background loops to dismantle cleanly
# This ensures the scheduler or network worker completely releases the CPU context
consumer_thread.join(timeout=5.0)

# 3. Command the launcher to close all physical connection sockets it owns at once
# Doing this after the threads have stopped prevents mid-flight socket crashes
launcher.shutdown_infrastructure()

```

---

### 2.2. The Asynchronous Paradigm (Async)

In the asynchronous paradigm, we no longer spin up blocking OS-level threads manually. Instead, handlers register their listening loops or polling intervals directly onto the active **`asyncio` Event Loop** as non-blocking coroutines.

#### Initialization & Execution (Async)

```python
# 1. Build the asynchronous input handler (using non-blocking coroutines)
input_handler_async = await launcher.build_input_handler(
    handler_type="<ProtocolIdentifierString>", # e.g., "RabbitMQ" or "HTTP"
    topology_setup=topology_dict,     # Passes structural routing rules, metadata, and infrastructure layouts
    callback=domain_processing_callback,     # Injects the domain processing strategy triggered upon frame arrival
    server_configurations=connection_dict     # Supplies the network credentials and physical session attributes
)

# 2. Schedule the background ingestion tasks directly onto the active Event Loop
await input_handler_async.start()

```

#### Graceful Termination Sequence (Async)

```python
# 1. Stop the logical processing loop and signal internal shutdown flags
input_handler_async.stop() 

# 2. Command the launcher to close all physical connections and client sessions asynchronously
await launcher.shutdown_infrastructure()

```

---

## 3. Supported Protocol Reference Directory

### 3.1. RabbitMQ Ingress Adapters

* **Protocol**: AMQP v0-9-1

#### Execution Variants

* **`SyncRabbitMQInputHandler`** (`Identifier`: `SyncRabbitMQ`): Blocking socket loop via `pika` (locks an OS thread running `channel.start_consuming()`).
* **`AsyncRabbitMQInputHandler`** (`Identifier`: `AsyncRabbitMQ`): Non-blocking coroutines via `aio-pika` scheduled on the active `asyncio` event loop.

#### 3.1.1. Server Connection Configuration (`server_configurations`)

Both synchronous and asynchronous RabbitMQ adapters share the exact same connection structure:

```json
{
  "connection": {
    // IP address or hostname of the RabbitMQ broker.
    // Default if omitted: None
    "host": "192.168.1.50",
    // Network port for AMQP communication.
    // Default if omitted: 5672 (or 5671 when SSL/TLS is enabled)
    "port": 5672,
    // Virtual Host used for message isolation on the broker.
    // Default if omitted: "/"
    "vhost": "/porto_hub",
    // Heartbeat ping interval (in seconds) to keep the TCP connection alive.
    // Default if omitted: 60 (Sync/pika) or 600 (Async/aio-pika)
    "heartbeat": 600,
    // Socket connection establishment timeout threshold (in seconds).
    // Default if omitted: 10
    "timeout": 10,
    // Maximum number of unacknowledged messages delivered concurrently per worker (QoS).
    // Default if omitted: 10
    "prefetch_count": 10,
    // Authentication credentials block.
    // Defaults if omitted: username="guest", password="guest"
    "auth": {
      "username": "porto_consumer",
      "password": "portopasswordmq"
    },
    // TLS/SSL transport security configuration block.
    // Default if omitted: Disabled (enabled=false)
    "ssl_conf": {
      // Toggles SSL/TLS encrypted transport.
      // Default if omitted: false
      "enabled": false,
      // Validates if the target hostname matches the server SSL certificate.
      // Default if omitted: false
      "check_hostname": false,
      // Certificate verification strategy ("required" for valid certs, "none"/null to ignore/self-signed).
      // Default if omitted: "none" (CERT_NONE)
      "verify_mode": "none"
    }
  }
}

```

#### 3.1.2. Architecture Layout Matrix (`topology_setup`)

* **`exchanges`** *(list[dict])*: Array of AMQP exchange configurations to be declared on the broker.
  * **`exchange_name`** *(string)*: Name of the exchange.
  * **`exchange_properties`** *(dict)*: Infrastructure settings for the exchange (e.g., `durable`, `auto_delete`, `arguments`).
    * **`exchange_type`** *(string)*: Routing strategy type (e.g., `fanout`, `direct`, `topic`, `headers`).
  * **`queues`** *(list[dict])*: Array of queues bound to this specific exchange.
    * **`queue_name`** *(string)*: Name of the target queue.
    * **`routing_key`** *(string)*: Binding key pattern used to filter/route messages from the exchange.
    * **`queue_properties`** *(dict)*: Additional queue-level parameters (e.g., `durable`, `exclusive`, `auto_delete`, `arguments`).
    * **`consume_properties`** *(dict)*: Ingestion behavior rules applied during message consumption (e.g., `no_ack`, `requeue`, `prefetch_count`).

> **Note on Protocol Properties (`exchange_properties`, `queue_properties`, `consume_properties`):**  
> These dictionaries accept any standard RabbitMQ options supported by the underlying driver. Depending on whether your implementation uses a synchronous or asynchronous execution strategy, you can pass any keyword arguments natively recognized by **`pika`** or **`aio-pika`**.

```json
{
  "topology": {
    "exchanges": [
      {
        "exchange_name": "community_porto_telemetry",
        "exchange_properties": { "exchange_type": "fanout", "durable": true },
        "queues": [
          {
            "queue_name": "percepta_db_persistence_porto",
            "routing_key": "",
            "consume_properties": { "no_ack": false,
                                    "requeue": false },
            "queue_properties": {}
          }
        ]
      }
    ]
  }
}

```

#### 3.1.3. Message Lifecycle & Execution Dynamics

##### Acknowledge & Requeue Strategy (`no_ack` & `requeue`)

The message lifecycle is tightly coupled to the outcome of the injected `on_message_callback` and the consumption rules defined in `consume_properties`:

* **Manual Acknowledgments (`no_ack: false` - Recommended):**
* **Successful Execution (`callback` returns `True`):** The handler sends a formal `ACK` to the broker, permanently clearing the frame from the queue.
* **Domain / Business Failure (`callback` returns `False` or raises an exception):** The handler sends a `NACK` / `Reject`. The broker behavior then strictly follows the `requeue` boolean parameter:
* **`requeue: true`:** The frame is immediately returned to the queue.
> **⚠️ Poison Pill Warning:** Deterministic business logic errors (e.g., malformed JSON, invalid data validation) will fail every time. Using `requeue: true` for business failures causes an infinite re-delivery loop that consumes 100% CPU. Business errors should be caught inside the domain callback and return `True` (or be routed to a Dead Letter Queue) to allow an `ACK`.

# TODO PERMITIR DE ALGUMA FORMA A CONFIGURAÇÃO DA DLQ
* **`requeue: false`:** The message is rejected and dropped from the queue (or automatically routed to an exchange's configured Dead Letter Queue - DLQ). This is the safest default for unhandled domain errors.

* **Automatic Acknowledgments (`no_ack: true`):**
* The broker removes the message from the queue the exact instant it is delivered over the socket, **before** the callback even executes. If the application crashes during business processing, the message is permanently lost.

##### Concurrency, Callback Latency & `prefetch_count`

The `prefetch_count` setting (configured in `server_configurations`) acts as a rate-limiter for unacknowledged frames:

* **Low `prefetch_count` with Heavy Callbacks:** If `prefetch_count` is set to a low value (e.g., `1` or `2`) and the `on_message_callback` performs long-running I/O or external network calls, the worker rapidly hits its limit. RabbitMQ will hold back remaining messages on the broker until existing messages receive an `ACK`, significantly throttling overall system throughput.
* **High Throughput Recommendation:** For latency-heavy callbacks, increase `prefetch_count` (e.g., `10` to `50`) so the `asyncio` event loop can dispatch multiple incoming frames concurrently. 

---

### 3.2. HTTP Pull-Based Ingress Adapters

* **Protocol**: HTTP/1.1 Poller

#### Execution Variants

* **`SyncHTTPInputHandler`** (`Identifier`: `SyncHTTP`): Uses blocking `requests` inside a threaded `time.sleep` polling loop.
* **`AsyncHTTPInputHandler`** (`Identifier`: `AsyncHTTP`): Uses non-blocking `aiohttp` and `asyncio.gather` for parallel requests via a periodic scheduler.

#### 3.2.1. Server Connection Configuration (`server_configurations`)

Both synchronous and asynchronous HTTP adapters share the same schema connection definition:

```json
{
  "connection": {
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
}

```

#### 3.2.2. Architecture Layout Matrix (`topology_setup`)

* **`endpoints`**: List of relative resource paths.
  * **`path`** *(string)*: Relative URL path or resource endpoint to be called.
  * **`method`** *(string)*: HTTP request method (e.g., `GET`, `POST`).
  * **`timeout_ms`** *(integer)*: Maximum allowed execution time, in milliseconds, before the HTTP request times out.
  * **`headers`** *(dict)*: Key-value pairs representing custom HTTP request headers (such as Authorization tokens or Content-Type definitions).

> **Note on Protocol Headers (`headers`):**  
> This dictionary accepts any standard or custom HTTP headers supported by the underlying driver. Depending on whether your implementation uses a synchronous or asynchronous execution strategy, you can pass any header key-value pairs natively recognized by **`requests`** and **`aiohttp`**.
 
```json
{
  "topology": {
    "endpoints": [
      {
        "path": "/v1/telemetry/meter_01",
        "method": "GET",
        "timeout_ms": 4000,
        "headers": { "Authorization": "Bearer token_hash_abc" }
      },
      {
        "path": "/v1/telemetry/meter_02",
        "method": "GET",
        "timeout_ms": 4000,
        "headers": { "Authorization": "Bearer token_hash_abc" }
      }
    ]
  }
}

```

---

## 4. Standardizing Incoming Data

To keep the core application independent of specific protocols (AMQP, HTTP, etc.) or execution modes (Sync vs. Async), input handlers do not send raw network data directly to business logic. 

Instead, every incoming message is wrapped by the `PayloadNormalizer` into a standard dictionary called the **Ingress Envelope**. This envelope captures essential metadata—such as origin, status, and content type—while keeping the payload as raw bytes, leaving any data parsing to downstream processing.

### 4.1. The Ingress Envelope

Any callback registered in a Percepta Input Handler will receive a dictionary formatted with the following five top-level keys:

| Field Name | Data Type           | Description | Example                                                                                                                      |
| --- |---------------------| --- |------------------------------------------------------------------------------------------------------------------------------|
| **`protocol`** | `str`               | Identifies the physical network protocol used to ingest the frame. | e.g., `"AMQP"` or `"HTTP"`                                                                                                   |
| **`source`** | `str`               | A unified location identifier string mapping the message origin. | e.g., `"community_porto_telemetry/percepta_db_persistence_porto"` or `"https://api.cleanwatts.energy/v1/telemetry/meter_01"` |
| **`status`** | `Optional[int]`     | Operational status/transaction code of the transmission (applicable to pull-based HTTP systems). | e.g., `200` (or `None` for message queuing protocols like AMQP)                                                              |
| **`content_type`** | `Optional[str]` | The payload encoding/mime standard resolved from network metadata headers. Defaults to "application/octet-stream" if missing. | e.g., `"application/json"`, `"text/plain"`, or `"application/octet-stream"`                                                  |
| **`payload`** | `Union[bytes, str]`                 | The raw byte stream read directly from the network socket (bytes). In network error scenarios (e.g., HTTP exception), it encapsulates the plain error traceback message (str). | `b'{"active_power": 115.4, "voltage": 230.1}'`                                                                               |

---

### 4.2. Normalizer Implementations

```python
class PayloadNormalizer:
    """
    Utility class designed to normalize disparate network protocol messages
    (AMQP, HTTP, etc.) into an Ingress Message Envelope.
    """

    @staticmethod
    async def from_rabbitmq(
            message: aio_pika.abc.AbstractIncomingMessage,
            queue_name: str
    ) -> Dict[str, Any]:
        """
        Parses an incoming RabbitMQ message and constructs a normalized envelope dictionary
        with raw payloads, maintaining architectural symmetry with the HTTP pipeline.
        """
        # 1. Resolve content-type safely from AMQP metadata properties, defaulting to binary stream
        content_type = message.content_type if (
                    hasattr(message, "content_type") and message.content_type) else "application/octet-stream"

        # 2. Extract Exchange source context to map the message origin trajectory
        exchange_source = message.exchange if message.exchange else "AMQP-Default"
        source_identifier = f"{exchange_source}/{queue_name}"

        # 3. Extract the raw body bytes directly from the AMQP message frame without parsing
        payload: bytes = message.body

        # 4. Construct and return the technology-agnostic Canonical Message Envelope
        return {
            "protocol": "AMQP",
            "source": source_identifier,
            "status": None,  # Message queuing protocols do not feature traditional HTTP operational status codes
            "content_type": content_type,
            "payload": payload  # Delivered as raw 'bytes' to be interpreted by the core application layers
        }

    @staticmethod
    async def from_http(
            response: Optional[aiohttp.ClientResponse] = None,
            endpoint: str = "",
            exception: Optional[Exception] = None
    ) -> Dict[str, Any]:
        """
        Parses an incoming HTTP ClientResponse or captures a network execution exception
        to construct a resilient, normalized Ingress Envelope.
        """
        # --- Scenario A: Physical Network Error (No response object available) ---
        if exception is not None:
            return {
                "protocol": "HTTP",
                "source": endpoint,
                "status": 503,  # 503 Service Unavailable represents the network/connectivity breakdown
                "content_type": "text/plain",
                "payload": f"Network Error: {str(exception)}"  # Encapsulate the raw exception traceback message
            }

        # --- Scenario B: Successful Network Roundtrip (Response object available) ---
        if response is None:
            raise ValueError("PayloadNormalizer.from_http requires either a response or an exception instance.")

        # 1. Capture whatever content-type the server sent (e.g., application/json, application/xml, text/csv)
        content_type = response.headers.get("Content-Type", "application/octet-stream")
        base_url = str(response.url).rstrip("/")
        source_identifier = f"{base_url}/{endpoint.lstrip('/')}" if endpoint else base_url

        try:
            # 2. Extract the raw body bytes directly from the network socket stream
            raw_body: bytes = await response.read()
            payload: Any = raw_body

        except Exception as read_err:
            # Guard against mid-flight socket drops during data streaming phases
            payload = f"Streaming Read Error: {str(read_err)}"
            content_type = "text/plain"

        # 3. Assemble and return the completely technology-agnostic envelope
        return {
            "protocol": "HTTP",
            "source": source_identifier,
            "status": response.status,
            "content_type": content_type,
            "payload": payload  # This is delivered as raw 'bytes' to the core callback
        }
```

---

## 5. Adding a New Protocol REVER ESTA PARTE

To integrate a new messaging protocol (e.g., MQTT, Kafka), follow this 4-step workflow:

### Step 5.1: Create the Transport Connector

Implement the low-level network driver wrapper that manages physical transport links, client sessions, and socket connections.

> **Note:** For specific guidelines, base classes, and examples on how to implement low-level transport drivers, refer to the **`README.md`** inside the `connectors/` directory.

### Step 5.2: Implement the Concrete Input Handler

1. Inherit from **`InputHandlerBase`**.
2. Implement `.start()` (the listening or polling loop) and `.stop()` (the cleanup routine).

### Step 5.3: Add a Normalizer Method

Add a static method to `PayloadNormalizer` to wrap raw incoming network frames into the standard **Ingress Envelope**:

```python
@staticmethod
async def from_mqtt(client_message, topic: str) -> Dict[str, Any]:
    """
    Parses an incoming MQTT message payload into a normalized Ingress Envelope.
    """
    return {
        "protocol": "MQTT",
        "source": topic,
        "status": None,  # Pub/Sub messaging does not use HTTP-style status codes
        "content_type": getattr(client_message, "content_type", "application/octet-stream"),
        "payload": client_message.payload  # Delivered as raw bytes
    }

```

### Step 5.4: Extend the Launcher and Register the Class

Decorate your custom builder function with `@register_input_handler("NAME")` to expose it to the system launcher:

```python
from app.core.input_handlers.input_handlers_launcher import register_input_handler, InputHandlersLauncher
from app.core.input_handlers import MQTTInputHandler
from app.core.connectors import MQTTConnector


@register_input_handler("MQTT")
def _build_mqtt_input_handler(
        self: InputHandlersLauncher,
        topology_setup: dict,
        callback,
        server_configurations: dict
):
  # 1. Instantiate the transport connector
  connector = MQTTConnector(server_configurations, self.logger)
  connector.connect()

  # 2. Append teardown closure for graceful system shutdown
  self._teardown_hooks.append(lambda: connector.disconnect())

  # 3. Return the concrete handler instance
  return MQTTInputHandler(connector=connector, callback=callback, topology=topology_setup)

```