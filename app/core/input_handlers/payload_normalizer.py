# --- Standard Library Imports ---
import asyncio
import copy
import json
from typing import Any, Dict, Optional

# --- Third-Party Imports ---
import aio_pika
import aiohttp


class PayloadNormalizer:
    """
    Utility class designed to normalize disparate network protocol messages
    (AMQP, HTTP, etc.) into an Ingress Envelope.
    """

    @staticmethod
    async def from_rabbitmq(
            message: aio_pika.abc.AbstractIncomingMessage,
            queue_name: str,
            metadata: dict = None
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

        # 4. Extract AMQP metadata properties (e.g. correlation_id)
        resolved_metadata: Dict[str, Any] = dict(metadata) if metadata else {}
        if hasattr(message, "correlation_id") and message.correlation_id:
            resolved_metadata["correlation_id"] = message.correlation_id

        # 5. Construct and return the technology-agnostic Canonical Message Envelope
        return {
            "protocol": "AMQP",
            "source": source_identifier,
            "status": None,  # Message queuing protocols do not feature traditional HTTP operational status codes
            "content_type": content_type,
            "metadata": resolved_metadata,
            "payload": payload  # Delivered as raw 'bytes' to be interpreted by the core application layers
        }

    @staticmethod
    async def from_http(
            response: Optional[aiohttp.ClientResponse] = None,
            endpoint: str = "",
            exception: Optional[Exception] = None,
            metadata: dict = None
    ) -> Dict[str, Any]:
        """
        Parses an incoming HTTP ClientResponse or captures a network execution exception
        to construct a resilient, normalized Canonical Message Envelope.
        """
        # --- Scenario A: Physical Network Error (No response object available) ---
        if exception is not None:
            return {
                "protocol": "HTTP",
                "source": endpoint,
                "status": 503,  # 503 Service Unavailable represents the network/connectivity breakdown
                "content_type": "text/plain",
                "metadata": metadata,
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
            "metadata": metadata,
            "payload": payload  # This is delivered as raw 'bytes' to the core callback
        }

    @staticmethod
    async def from_local(
            payload: Any,
            source: str = "local",
            metadata: dict = None
    ) -> Dict[str, Any]:
        """
        Wraps a payload received via a direct in-process (IPC) invocation into the
        Canonical Integration Envelope, maintaining architectural symmetry with the
        network-based protocol pipelines.

        The payload is deep-copied, so that the receiver of the envelope never shares objects with the sender (see
        step 3 below).
        """
        # 1. There is no transport-level content type for an in-process call, since the
        #    payload is already a native Python object rather than a serialized byte stream
        content_type = "application/x-python-object"

        # 2. The source is fixed at construction time, since a local handler always
        #    receives data from the same upstream container, with no concept of a
        #    per-message origin (such as an endpoint or a queue)
        source_identifier = source

        # 3. The payload needs no parsing or byte decoding, but it is COPIED. The payload is a native Python object
        #    (e.g., a list of entities with dictionaries inside), and in an in-process call the sender and the
        #    receiver would otherwise hold the SAME object: if the receiver changed it (e.g., a Data-Issue Handling
        #    Approach that changes a reading), the sender's data would change too, and if the sender changed it after
        #    sending, the message would change before it was processed. The network protocols do not have this problem,
        #    because they deliver 'bytes' (or text), which cannot be changed, so each receiver gets its own copy when it
        #    parses them. A deep copy is needed (not 'dict(payload)' or 'list(payload)'): a shallow copy would still
        #    share the dictionaries inside the list.
        #    Example: payload = [{"id": "EV1", "parameters": {"power": {"values": [r1]}}}]
        #    -> resolved_payload has its own list, its own dictionaries and its own readings, equal to the original.
        resolved_payload: Any = copy.deepcopy(payload)

        # 4. Resolve metadata safely, defaulting to an empty dictionary when none is provided
        resolved_metadata: Dict[str, Any] = dict(metadata) if metadata else {}

        # 5. Construct and return the technology-agnostic Canonical Message Envelope
        return {
            "protocol": "LOCAL",
            "source": source_identifier,
            "status": None,  # In-process calls have no transport-level status code
            "content_type": content_type,
            "metadata": resolved_metadata,
            "payload": resolved_payload,
        }