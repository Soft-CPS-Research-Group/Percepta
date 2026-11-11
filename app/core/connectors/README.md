# Connectors Subsystem

In the Percepta framework, **Connectors** are low-level network components purely dedicated to managing transport drivers, physical socket links, client sessions, and raw communication protocols (AMQP, HTTP, in-memory, etc.). They act as the physical networking layer that powers the higher-level Input and Output Handlers.

---

## 1. System Architecture

The subsystem provides concrete transport wrappers that are all asynchronous (they run on the `asyncio` event loop):

### Core Transport Concepts

* **Async Connectors**: All the connectors are implemented using non-blocking, asynchronous drivers (such as `aio-pika` for AMQP or `aiohttp` for HTTP). They register non-blocking socket tasks directly onto the active `asyncio` Event Loop. (TENHO DE ENTENDER O QUE É ISTO DO EVENT LOOP)
* **Connection Lifecycle**: Every connector exposes an operational contract to manage its underlying transport link:
* `.connect()`: Establishes physical sockets, performs handshakes, and initializes session pools. (The in-memory `AsyncLocalConnector` has no `.connect()`: it is ready as soon as it is created.)
* `.is_connected()`: Validates the live health state of active sockets and channels.
* `.close()`: Performs a clean, graceful teardown releasing socket buffers and persistent session pools.

---

## 2. Supported Connectors Reference Directory

### 2.1. AMQP Connectors (RabbitMQ)

#### 2.1.1. `AsyncRabbitMQConnector`

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
    "no_ack": False,
    # Not an option of aio-pika: used only by reject_message(), which gives it to message.nack()
    "acknowledgement_properties": {"requeue": False}
}

# 2. Instantiate and connect asynchronously
connector = AsyncRabbitMQConnector(server_configurations=connection_dict)
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

# 6. Stop that consumer only (the connection and the other consumers stay open)
await connector.cancel_consumer(consumer_tag)

# 7. Teardown (it also stops every consumer that is still active)
await connector.close()

```

> Note on Protocol & Message Properties:
> The property dictionaries (exchange_properties, queue_properties, publish_properties, and consume_properties) accept any standard AMQP options supported natively by the underlying driver (aio-pika). You can pass any additional keyword parameters recognized by these libraries (such as custom headers, expiration, reply_to, or plugin arguments) directly through these dictionaries.

#### 2.1.2. Server Connection Configuration Schema (`server_configurations`)

The AMQP connector (`AsyncRabbitMQConnector`) accepts the following configuration dictionary:

```json
{
  // [Required] IP address or hostname of the RabbitMQ broker.
  "host": "192.168.1.50",

  // [Optional] Network port for AMQP communication (1 to 65535).
  // Default if omitted: 5672, whether SSL/TLS is enabled or not. The port does not turn TLS on: for a broker with TLS
  // (usually on 5671) set the port and enable 'ssl_conf' both.
  "port": 5672,

  // [Optional] Virtual Host used for message isolation on the broker.
  // Default if omitted: "/"
  "vhost": "/porto_hub",

  // [Optional] Heartbeat ping interval (in seconds) to keep the TCP connection alive (0 to 65534).
  // Default if omitted: 60 (aio-pika). 0 turns the heartbeats off.
  "heartbeat": 600,

  // [Optional] Socket connection establishment timeout threshold (in seconds, greater than 0).
  // Default if omitted: none (aio-pika waits for the broker without a time limit)
  "timeout": 10,

  // [Optional] Maximum number of unacknowledged messages delivered concurrently per worker (QoS).
  // Default if omitted: 0 (aio-pika), i.e., no limit (the broker sends all the messages of the queue at once)
  "prefetch_count": 10,

  // [Optional] Authentication credentials block. 'username' and 'password' must be given together, or neither.
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

    // [Optional] Validates if the target hostname matches the server SSL certificate. It needs "verify_mode" to be
    // "required".
    // Default if omitted: false
    "check_hostname": false,

    // [Optional] Certificate verification strategy: "required" for valid certs, "none" (or null) to ignore the
    // certificate (e.g., self-signed). Any other text is refused.
    // Default if omitted: "none" (CERT_NONE)
    "verify_mode": "none"
  }
}

```

---

### 2.2. HTTP Connectors

#### 2.2.1. `AsyncHTTPConnector`

* **Driver**: `aiohttp` (ClientSession & TCPConnector pool)
* **Execution**: Non-blocking coroutines managed via `asyncio`

```python
from app.core.connectors.async_http_connector import AsyncHTTPConnector

# 1. Instantiate and connect asynchronously
connector = AsyncHTTPConnector(server_configurations=connection_dict)
await connector.connect()

# 2. Fetch data asynchronously (GET) via request
response = await connector.request(
    method="GET",
    endpoint="/v1/telemetry/meter_01",
    timeout=4.0
)
# The response is returned open: reading the body frees its connection (or call response.release() if the body
# is not needed). A status that is not 2xx raises aiohttp.ClientResponseError instead of returning.
data = await response.json()

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
post_response.release()  # the body is not needed, so the connection is freed without reading it

# 4. Teardown
await connector.close()

```

#### 2.2.2. Server Connection Configuration Schema (`server_configurations`)

The HTTP connector (`AsyncHTTPConnector`) accepts the following configuration dictionary:

```json
{
  // [Optional] Protocol scheme to construct base URL if 'url' is omitted.
  // Default if omitted: "http"
  "schema": "https",

  // [Optional*] Target server IP address or hostname. 
  // *Required if 'url' is not provided.
  // Default if omitted: None
  "host": "api.cleanwatts.energy",

  // [Optional] Target server network port (1 to 65535).
  // Default if omitted: None (falls back to default HTTP/HTTPS ports)
  "port": 443,

  // [Optional*] Explicit base server URL (overrides 'schema', 'host', and 'port').
  // *Required if 'host' is not provided.
  // Default if omitted: Dynamically constructed from schema://host:port
  "url": "https://api.cleanwatts.energy",

  // [Optional] Toggles TLS/SSL server certificate verification.
  // Default if omitted: false
  "verify_ssl": true,

  // [Optional] Basic HTTP Authentication credentials block. 'username' (without ":") and 'password' must be given
  // together, or neither.
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
  // Default if omitted: {} (no default headers: set the ones you need, e.g., "Accept")
  "headers": {
    "Authorization": "Bearer token_hash_abc",
    "Accept": "application/json"
  }
}

```




### 2.3. In-Memory Connector (Local)

#### 2.3.1. `AsyncLocalConnector`

* **Driver**: none (`asyncio.Queue` and direct function calls, inside the same process)
* **Execution**: Non-blocking, on the `asyncio` event loop. No network is involved.

One class with two roles, used by the Local Input and Output Handlers:

* **Queue (input):** a buffer in memory. A producer calls `push()` and the consumer (the Input Handler) calls `await get()`. The first payload to enter is the first to leave.
* **Direct dispatch (output):** `dispatch()` calls a function of the next container (the `target_callable`) and returns what it returns.

The connector does not log, and it has no `connect()`: it is ready as soon as it is created. It raises standard errors and whoever calls it decides what to do with them.

```python
from app.core.connectors.async_local_connector import AsyncLocalConnector

# --- Input role: the queue ---
queue_connector = AsyncLocalConnector({"max_queue_size": 10000})

queue_connector.push({"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0}})  # no await: it never waits
payload = await queue_connector.get()      # waits if the queue is empty
try:
    ...                                    # process the payload
finally:
    queue_connector.task_done()            # once per get(), after processing

# --- Output role: direct dispatch ---
async def receive(payload):                # a function of the next container, e.g., the 'on_message' of its Input Handler
    ...

dispatch_connector = AsyncLocalConnector({"target_callable": receive})
result = await dispatch_connector.dispatch({"temperature": 21.5})   # same as: await receive({...})

# --- Teardown ---
await queue_connector.close()
await dispatch_connector.close()
```

#### 2.3.2. Server Connection Configuration Schema (`server_configurations`)

```json
{
  // [Optional, output role] The function that 'dispatch()' calls. A function cannot be written in a JSON file, so it is
  // given in code by whoever creates the connector (e.g., a launcher). Prefer an 'async def' (see the notes below).
  // Default if omitted: None ('dispatch()' then raises a RuntimeError)
  "target_callable": "<function>",

  // [Optional, input role] Maximum number of payloads waiting in the queue (an integer, not negative).
  // Default if omitted: no limit (the default of asyncio.Queue). 0 also means no limit.
  "max_queue_size": 10000
}
```

A `target_callable` that is not callable raises `TypeError`.

#### 2.3.3. Usage Notes

* **Give each connector only what its role needs:** the input, `max_queue_size`; the output, `target_callable`. The connector does not check the role, so an output without a `target_callable` only fails in the first `dispatch()`. Each connector belongs to one handler (both would close it in their `stop()`).
* **Errors:** `push()` raises `RuntimeError` (connector closed) and `asyncio.QueueFull` (queue full); `get()` raises `RuntimeError` (connector closed, or closed while it was waiting); `dispatch()` raises `RuntimeError` (closed, or no `target_callable`) and the errors of the `target_callable`. The payload is not kept or retried, so the caller has to handle the error.
* **Set `max_queue_size`:** without it the queue has no limit, and the memory grows if the consumer is slower than the producer.
* **Event loop and threads:** the queue is not thread-safe, so `push()` and `get()` must be called from the thread of the event loop it belongs to. The `target_callable` should be a coroutine function (`async def`), which `dispatch()` awaits in the loop; a normal function runs in another thread, where it must not use the queue. The in-memory link between two containers only works if both run in the same event loop, so it cannot be used between containers that run in different threads (as in the single-process launcher).
* **One consumer per queue:** each payload is taken by a single `get()`.
* **The connector does not copy the payload:** the sender must not change it after `push()` or `dispatch()`.
* **Memory only:** if the process stops, the payloads in the queue are lost. There is no acknowledgement or retry.
* **Closing:** stop the consumer first (cancel its task), then call `close()`. `close()` wakes up a `get()` that is waiting, which raises a `RuntimeError` (so a consumer never waits forever), and a new `get()` raises the same error; the payloads still in the queue are not delivered any more. `task_done()` does not check if the connector is closed, so that a payload taken before the `close()` can still be marked as done.

---

## 3. Implementing a New Connector

When adding support for a new protocol or network driver (e.g., MQTT, Kafka), follow these guidelines:

### Design Guidelines

1. **Self-Contained Protocol Logic**: All driver-specific imports (e.g., `aiomqtt`, `confluent_kafka`) and physical connection parameters must remain isolated inside the connector.
2. **Explicit Connection Management**: Implement `.connect()`, `.is_connected()`, and `.close()` to allow higher-level orchestrators (like the `InputHandlersLauncher`) to track and teardown sockets gracefully during system shutdown.
3. **Asynchronous Only**: Connectors are non-blocking and use an asynchronous driver (e.g., `AsyncMQTTConnector`), so they run on the `asyncio` event loop with the rest of the framework.
4. **Errors Without Logging**: A connector does not log and has no exceptions of its own. It validates its configuration in `__init__` and raises standard exceptions (`ValueError`, `TypeError`, `RuntimeError`) or the ones of the driver, with a message that says what failed. Whoever calls the connector decides what to do with the error and logs it, so that each error is logged only once.

---

### Step-by-Step Implementation Template (Async MQTT Example)

```python
import asyncio
from typing import Optional, Any


class AsyncMQTTConnector:
    """
    Asynchronous transport wrapper managing an MQTT client session.

    The connector does not log. Errors are raised and whoever calls it decides what to do with them.
    """

    _LOG_PREFIX: str = "Async MQTT Connector |"

    def __init__(self, server_configurations: dict) -> None:
        # The configuration is checked first, so that a wrong value fails here and not in the middle of the connection.
        if not server_configurations.get("host"):
            raise ValueError(f"{self._LOG_PREFIX} Error initializing MQTT setup: 'host' must be provided.")

        self._host = server_configurations["host"]
        # An option that is not configured stays None, and 'connect()' leaves it out so that the driver uses its default.
        self._port = server_configurations.get("port")
        self._client: Optional[Any] = None

    async def connect(self) -> None:
        """Establishes the non-blocking client connection."""
        if self.is_connected():
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot connect: Connector is already connected.")

        # 1. Initialize the client driver session here. The errors of the driver go up as they are.
        # self._client = aiomqtt.Client(hostname=self._host, **({"port": self._port} if self._port else {}))
        # await self._client.connect()

    def is_connected(self) -> bool:
        """Returns True if the underlying client session is alive."""
        return self._client is not None

    async def close(self) -> None:
        """Gracefully disconnects and releases network sockets. Errors while closing are ignored."""
        if self._client:
            try:
                # await self._client.disconnect()
                pass
            except Exception:
                pass
            finally:
                self._client = None

```