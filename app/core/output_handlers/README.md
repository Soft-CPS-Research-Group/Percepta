# Output Handlers Subsystem

In the Percepta framework, **Output Handlers** are the egress modules responsible for dispatching processed data and commands out of the system. They act as adapters for specific protocols: they connect to network targets, publish messages or issue HTTP requests, and manage dynamic target routing alongside RPC correlation responses before returning execution control to the application core.

---

## 1. System Architecture

The subsystem consists of four main architectural building blocks:

### Core Egress Concepts

* **`OutputHandlersLauncher`**: The master orchestrator for all output handlers within the system. It maintains a unified registry of every available handler across all supported communication protocols. Passing a specific identifier string allows the launcher to dynamically resolve, configure, and return the correct output handler to dispatch data. Additionally, it tracks these sessions to close all physical connections and sockets centralized during system shutdown.
* **`OutputHandlerBase`**: The core interface and abstract contract that all technology-specific egress handlers must inherit from.
* **`Connectors`**: Low-level network components purely dedicated to managing drivers, sessions, client states, and physical transport links.
* **Sync Connectors**: Uses blocking socket operations (such as `pika` for AMQP or `requests` for HTTP).
* **Async Connectors**: Uses non-blocking sockets (such as `aio-pika` for AMQP or `aiohttp` for HTTP).
* **`Publishing Modes` (`MqPublishMode` / `HttpPublishMode`)**: Execution strategies (`STATIC`, `OVERRIDE`, `ADDITIVE`) that dictate how dynamically injected runtime targets are merged with or prioritized over pre-configured static network topologies.

---

## 2. Initializing and Using an Existing Protocol

Unlike Input Handlers—which require dedicated background OS threads in Synchronous mode versus Event Loop coroutines in Asynchronous mode—**Output Handlers share a similar operational workflow across both paradigms**.

Execution simply involves dispatching payloads via `.send()`. However, when **RPC Synchronization** is enabled, the factory returns a tuple `(output_handler, rpc_input_handler)` requiring an internal response listener to be started and stopped alongside the publisher.

### 2.1. The Synchronous Paradigm (Sync)

The initialization is driven by a blocking call to the launcher. If an RPC response handler is returned, its execution loop is offloaded to a dedicated OS thread.

#### Initialization & Execution (Sync)

```python
# 1. Build the synchronous output handler
output_handler_sync, rpc_input_handler_sync = launcher.build_output_handler(
    handler_type="<ProtocolIdentifierString>",     # e.g., "SyncRabbitMQ" or "SyncHTTP"
    topology_setup=topology_dict,                  # Passes routing rules, metadata, and infrastructure layouts
    server_configurations=connection_dict          # Supplies network credentials and physical session attributes
)

# 2. If RPC is enabled, offload the blocking RPC reply listener to a dedicated OS thread
if rpc_input_handler_sync:
    import threading
    rpc_thread = threading.Thread(
        target=rpc_input_handler_sync.start,
        daemon=True,
        name="egress_sync_rpc_reply_worker"
    )
    rpc_thread.start()

# 3. Dispatch payload synchronously (blocking call awaiting response or returning None)
response = output_handler_sync.send(
    message={"telemetry_id": "sensor_01", "active_power": 115.4},
    dynamic_exchanges=None,                        # [Optional] Dynamic targeting overrides
    mode=MqPublishMode.STATIC                      # MqPublishMode.STATIC | OVERRIDE | ADDITIVE
)

```

#### Graceful Termination Sequence (Sync)

```python
# 1. Stop the logical processing loop for the output handler
output_handler_sync.stop()

# 2. Stop and synchronize the RPC reply listener thread if present
if rpc_input_handler_sync:
    rpc_input_handler_sync.stop()
    if 'rpc_thread' in locals():
        rpc_thread.join(timeout=5.0)

# 3. Command the launcher to execute teardown hooks and close physical connection sockets
launcher.shutdown_infrastructure()

```

---

### 2.2. The Asynchronous Paradigm (Async)

In the asynchronous paradigm, we no longer spin up blocking OS-level threads manually. Instead, handlers and any returned RPC listeners register directly onto the active **`asyncio` Event Loop** as non-blocking coroutines.

#### Initialization & Execution (Async)

```python
# 1. Build the asynchronous output handler (using non-blocking coroutines)
output_handler_async, rpc_input_handler_async = await launcher.build_output_handler(
    handler_type="<ProtocolIdentifierString>",     # e.g., "AsyncRabbitMQ" or "AsyncHTTP"
    topology_setup=topology_dict,                  # Passes routing rules, metadata, and infrastructure layouts
    server_configurations=connection_dict          # Supplies network credentials and physical session attributes
)

# 2. If RPC is enabled, schedule the background response listener onto the Event Loop
if rpc_input_handler_async:
    await rpc_input_handler_async.start()

# 3. Dispatch payload asynchronously without blocking the event loop
response = await output_handler_async.send(
    message={"telemetry_id": "sensor_01", "active_power": 115.4},
    dynamic_endpoints=None,                        # [Optional] Dynamic targeting overrides
    mode=HttpPublishMode.STATIC                    # HttpPublishMode.STATIC | OVERRIDE | ADDITIVE
)

```

#### Graceful Termination Sequence (Async)

```python
# 1. Stop the logical processing loop for the output handler
output_handler_async.stop()

# 2. Stop the RPC reply listener if present
if rpc_input_handler_async:
    rpc_input_handler_async.stop()

# 3. Command the launcher to close all physical connections and client sessions asynchronously
await launcher.shutdown_infrastructure()

```

---

## 3. Supported Protocol Reference Directory

### 3.1. RabbitMQ Egress Adapters

* **Protocol**: AMQP v0-9-1

#### Execution Variants

* **`SyncRabbitMQOutputHandler`** (`Identifier`: `SyncRabbitMQ`): Synchronous message publishing via `pika` with `threading.Event` synchronization for RPC response escrow.
* **`AsyncRabbitMQOutputHandler`** (`Identifier`: `AsyncRabbitMQ`): Non-blocking coroutine publishing via `aio-pika` scheduled on the active `asyncio` event loop.

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
    // Authentication credentials block.
    // Defaults if omitted: username="guest", password="guest"
    "auth": {
      "username": "porto_publisher",
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
  * **`exchange_properties`** *(dict)*: Infrastructure settings for auto-declaring the exchange (e.g., `durable`, `auto_delete`).
      * **`exchange_type`** *(string)*: Routing strategy type (e.g., `fanout`, `direct`, `topic`, `headers`).
  * **`routing_key`** *(string)*: Binding key pattern used to route published messages.
  * **`message_properties`** *(dict)*: AMQP metadata headers (e.g., `delivery_mode`, `content_type`, `content_encoding`).
  * **`rpc_properties`** *(dict)*: Asynchronous RPC synchronization settings.
    * **`enabled`** *(boolean)*: Toggles RPC reply tracking and correlation escrow.
    * **`timeout_ms`** *(integer)*: Maximum threshold (in ms) to wait for a matching response frame.
    * **`reply_to`** *(dict)*: Defines the return exchange and queue topology for reply frames.


> **Note on Protocol Properties (`exchange_properties`, `message_properties`, `rpc_properties`):**  
> These dictionaries accept any standard RabbitMQ options supported by the underlying driver. Depending on whether your implementation uses a synchronous or asynchronous execution strategy, you can pass any keyword arguments natively recognized by **`pika`** or **`aio-pika`**.

> **Note on `reply_to` Topology Schema:**  
> The `reply_to` configuration object uses the exact same topology structural format (`exchanges`, `queues`, `queue_properties`, and `consume_properties`) as the **Input Handlers**. For detailed property field definitions and schema examples, please refer to the **`README.md`** in the Input Handlers subsystem documentation.

```json
{
  "topology": {
    "exchanges": [
      {
        "exchange_name": "exchange_name_1_telemetry",
        "routing_key": "energy.porto.nodes.*",
        "exchange_properties": {
          "exchange_type": "topic",
          "durable": true,
          "auto_delete": false
        },
        "message_properties": {
          "delivery_mode": 2,
          "content_type": "application/json",
          "content_encoding": "utf-8"
        },
        "rpc_properties": {
          "enabled": true,
          "timeout_ms": 5000,
          "reply_to": {
            "exchanges": [
              {
                "exchange_name": "rpc_reply_exchange_porto",
                "exchange_properties": {
                  "exchange_type": "direct",
                  "durable": true,
                  "auto_delete": false
                },
                "queues": [
                  {
                    "queue_name": "rpc_reply_queue_porto",
                    "routing_key": "rpc.reply.porto",
                    "queue_properties": {
                      "durable": false,
                      "exclusive": true,
                      "auto_delete": true
                    },
                    "consume_properties": {
                      "no_ack": true
                    }
                  }
                ]
              }
            ]
          }
        }
      }
    ]
  }
}

```

---

### 3.2. HTTP Push-Based Egress Adapters

* **Protocol**: HTTP/1.1 Egress Publisher (POST / PUT)

#### Execution Variants

* **`HTTPOutputHandler`** (`Identifier`: `SyncHTTP`): Uses blocking `requests` calls with optional in-place synchronous RPC response extraction.
* **`AsyncHTTPOutputHandler`** (`Identifier`: `AsyncHTTP`): Uses non-blocking `aiohttp` and `asyncio.gather` for parallel requests. Supports background fire-and-forget execution when RPC is disabled.

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
      "username": "api_user",
      "password": "api_password"
    },

    // [Optional] Default global headers applied to every HTTP request session.
    // Default if omitted: {"Content-Type": "application/json"}
    "headers": {
      "Authorization": "Bearer token_hash_abc",
      "Content-Type": "application/json"
    }
  }
}

```

#### 3.2.2. Architecture Layout Matrix (`topology_setup`)

* **`endpoints`**: List of relative resource paths.
* **`path`** *(string)*: Relative URL path or resource endpoint to call.
* **`method`** *(string)*: HTTP request method (`POST` or `PUT`).
* **`timeout_ms`** *(integer)*: Maximum allowed execution time in milliseconds before request timeout.
* **`headers`** *(dict)*: Endpoint-specific HTTP headers (determines payload formatting, e.g., JSON, XML, Form).
* **`rpc_properties`** *(dict)*: Egress response collection settings.
* **`enabled`** *(boolean)*: When `true`, collects and returns server response data. When `false` (Async), delegates to background fire-and-forget tasks.





```json
{
  "topology": {
    "endpoints": [
      {
        "path": "/v1/control/actuate_node_01",
        "method": "POST",
        "timeout_ms": 5000,
        "headers": {
          "Content-Type": "application/json",
          "Authorization": "Bearer token_hash_abc"
        },
        "rpc_properties": {
          "enabled": true
        }
      },
      {
        "path": "/v1/logs/audit_trail",
        "method": "PUT",
        "timeout_ms": 3000,
        "headers": {
          "Content-Type": "application/x-www-form-urlencoded"
        },
        "rpc_properties": {
          "enabled": false
        }
      }
    ]
  }
}

```

### 3.3. In-Memory Local Egress Adapters
**Protocol:** In-Memory Direct Python Pipeline (Callable Inter-Process Dispatcher)

#### Execution Variants
* **`AsyncLocalOutputHandler` (Identifier: `AsyncLocal`)**: Dispatches standardized payloads directly to in-memory Python receiver callables without passing through network brokers or transport protocols. Supports both synchronized (`rpc_properties.enabled: true`) and fire-and-forget background execution (`rpc_properties.enabled: false`).

#### 3.3.1. Server Connection Configuration (`server_configurations`)
The local in-memory adapter receives executable function references directly within its configuration object:

```python
server_configurations = {
    # Reference to a valid Python executable target (coroutine function 'async def' or synchronous 'def').
    # Required. Raises ValueError if missing or non-callable.
    "target_callable": my_local_data_processor_function
}
```

#### 3.3.2. Architecture Layout Matrix (`topology_setup`)

* **`rpc_properties` (dict)**: In-memory dispatch execution strategy settings.
* **`enabled` (boolean)**:
  * `false` *(Default - Fire-and-Forget)*: Spawns an asynchronous background task (`asyncio.create_task`) via `_safe_background_dispatch` and returns `None` instantly to the caller without blocking execution.
  * `true` *(RPC Enabled / Synchronized)*: Directly awaits the callable (or offloads synchronous blocking callables to thread pool executor via `asyncio.to_thread`), returning the target function's output result back to the caller.


```json
{
  "topology": {
    "rpc_properties": {
      "enabled": false
    }
  }
}
```
---

## 4. Adding a New Protocol Egress Adapter

To integrate a new outbound messaging protocol (e.g., MQTT, Kafka), follow this 4-step workflow:

### Step 4.1: Create the Transport Connector

Implement the low-level network driver wrapper that manages physical transport links, client sessions, and egress publication calls.

> **Note:** For specific guidelines, base classes, and examples on how to implement low-level transport drivers, refer to the **`README.md`** inside the `connectors/` directory.

### Step 4.2: Implement the Concrete Output Handler

1. Inherit from **`OutputHandlerBase`**.
2. Implement `.send()` (the outbound publication method) and `.stop()` (the cleanup routine).

### Step 4.3: Extend the Launcher and Register the Class

Decorate your custom builder function with `@register_output_handler("NAME")` to expose it to the system launcher:

```python
from typing import Tuple, Optional
from app.core.output_handlers import register_output_handler, OutputHandlersLauncher
from app.core.output_handlers import KafkaOutputHandler
from app.core.input_handlers import KafkaInputHandler
from app.core.connectors import KafkaConnector


@register_output_handler("Kafka")
def _build_kafka_output_handler(
        self: OutputHandlersLauncher,
        topology_setup: dict,
        server_configurations: dict
) -> Tuple[KafkaOutputHandler, Optional[KafkaInputHandler]]:
  # 1. Instantiate the transport connector
  connector = KafkaConnector(server_configurations, self.logger)
  connector.connect()

  # 2. Append teardown closure for graceful system shutdown
  self._teardown_hooks.append(lambda: connector.disconnect())

  # 3. Instantiate output handler (and optional RPC input listener)
  output_handler = KafkaOutputHandler(publisher_connector=connector, topology_setup=topology_setup, logger=self.logger)
  rpc_input_handler = None  # Populated if RPC properties are enabled in topology

  return output_handler, rpc_input_handler

```