# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

# --- Third-Party Imports ---
import pytest

# --- Internal Framework Imports ---
from app.core.input_handlers.async_http_input_handler import HTTPInputHandler as AsyncHTTPInputHandler
from app.core.input_handlers.async_rabbitmq_input_handler import AsyncRabbitMQInputHandler
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.async_local_output_handler import AsyncLocalOutputHandler
from app.core.output_handlers.async_rabbitmq_output_handler import AsyncRabbitMQOutputHandler
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils
from others.launchers import CWLauncher
from app.extension.endpoints_builders import CWEndpointBuilder
from app.extension import CWReceiver
from others.cw_translator import CWTranslator


# ============================================================================
# INTEGRATION TEST FIXTURES
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a configured LoggingUtils instance for integration tests."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    return LoggingUtils(
        component_name="cw_launcher_integration_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def sample_environment_specs():
    """Provides a realistic single-environment entity specifications dataset."""
    return {
        "group": "living lab",
        "entities": {
            "PV01": {
                "label": "pv_panel",
                "parameters": {
                    "energy": {
                        "id": "33711",
                        "label": "energy",
                        "type": "PV energy",
                        "measurementUnit": "kWh"
                    }
                }
            },
            "B01": {
                "label": "battery",
                "parameters": {
                    "energy_in": {
                        "id": "35927",
                        "label": "energy_in",
                        "type": "Battery charging energy",
                        "measurementUnit": "kWh"
                    },
                    "SoC": {
                        "id": "33690",
                        "label": "state_of_charge",
                        "type": "Battery State of charge",
                        "measurementUnit": "%"
                    }
                }
            }
        }
    }


@pytest.fixture
def base_configurations():
    """Provides a master configuration footprint with Local In-Memory egress."""
    return {
        "timezone": "Europe/Lisbon",
        "frequency": {
            "value": 15,
            "unit": "s"
        },
        "cleanwatts": {
            "login_server": {
                "url": "https://innov.cleanwattsdigital.com/api/2.0",
                "auth": {
                    "username": "user-demo",
                    "password": "secret_password"
                },
                "resources": {
                    "login": "/sessions",
                    "refresh": "/sessions/refresh?"
                }
            }
        },
        "ingress": {
            "handler_type": "AsyncHTTP",
            "connection": {
                "url": "https://innov.cleanwattsdigital.com/api/2.0"
            },
            "topology": {
                "endpoints": [
                    {
                        "path": "/data/request/Instant?instantType=avg&tags={param_id}&from={from}&to={to}",
                        "method": "GET",
                        "timeout_ms": 5000,
                        "headers": {"Authorization": "CW {token}"}
                    }
                ]
            }
        },
        "egress": {
            "handler_type": "AsyncLocal",
            "connection": {
                "target_callable": MagicMock()
            },
            "topology": {
                "rpc_properties": {"enabled": False}
            }
        },
        "source_mapping": {
            "R-H-01": {
                "strategy": "parameter",
                "rules": {
                    "energy": "33711",
                    "energy_in": "35927",
                    "SoC": "33690"
                }
            }
        },
        "log_files": {
            "max_size": 10485760
        }
    }


# ============================================================================
# 1. BOOTSTRAP & PIPELINE WIRING INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("app.core.connectors.async_http_connector.AsyncHTTPConnector.connect", new_callable=AsyncMock)
@patch("app.core.utils.cwlogin.CWSession.start_token_refresher_service")
async def test_cw_launcher_pipeline_wiring_integration_test(
    mock_token_refresher,
    mock_http_connect,
    base_configurations,
    sample_environment_specs,
    mock_logger
):
    """Verifies that CWLauncher connects the full pipeline:
    InputHandler -> Receiver -> Translator -> OutputHandler.
    """
    input_launcher = InputHandlersLauncher(mock_logger)
    output_launcher = OutputHandlersLauncher(mock_logger)

    # 1. Initialize launcher (verifies auth thread trigger)
    launcher = CWLauncher(base_configurations, input_launcher, output_launcher)
    mock_token_refresher.assert_called_once()

    # 2. Launch single environment pipeline
    input_handler, output_handler, rpc_listener = await launcher.launch_environment(
        environment_name="R-H-01",
        environment_specs=sample_environment_specs
    )

    # 3. Assert correct materialization and types
    assert isinstance(input_handler, AsyncHTTPInputHandler)
    assert isinstance(output_handler, AsyncLocalOutputHandler)
    assert rpc_listener is None

    # 4. Verify internal pipeline component bindings
    assert input_handler._endpoint_builder is not None
    assert isinstance(input_handler._endpoint_builder, CWEndpointBuilder)

    # Check callback hook on input handler matches receiver's _callback
    callback_target = input_handler._on_message_callback
    assert hasattr(callback_target, "__self__")
    assert isinstance(callback_target.__self__, CWReceiver)

    receiver_instance: CWReceiver = callback_target.__self__
    assert isinstance(receiver_instance._translator, CWTranslator)
    assert receiver_instance._translator._output_handler == output_handler

    # 5. Clean up
    await input_handler.stop()
    await output_handler.stop()
    launcher.stop()


# ============================================================================
# 2. END-TO-END DATAFLOW DISPATCH INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("app.core.connectors.async_http_connector.AsyncHTTPConnector.connect", new_callable=AsyncMock)
@patch("app.core.utils.cwlogin.CWSession.start_token_refresher_service")
async def test_cw_launcher_dataflow_e2e_integration_test(
    mock_token_refresher,
    mock_http_connect,
    base_configurations,
    sample_environment_specs,
    mock_logger
):
    """Verifies the complete end-to-end data execution path:
    Simulates raw inbound reading packets received by the InputHandler callback,
    validates aggregation in CWReceiver, transformation in CWTranslator,
    and arrival at the target callable in AsyncLocalOutputHandler.
    """
    local_sink_mock = MagicMock()
    base_configurations["egress"]["connection"]["target_callable"] = local_sink_mock

    input_launcher = InputHandlersLauncher(mock_logger)
    output_launcher = OutputHandlersLauncher(mock_logger)

    launcher = CWLauncher(base_configurations, input_launcher, output_launcher)

    input_handler, output_handler, _ = await launcher.launch_environment(
        environment_name="R-H-01",
        environment_specs=sample_environment_specs
    )

    # Inbound payload received from polling HTTP connector
    raw_inbound_envelopes = [
        {
            "source": "https://innov.cleanwattsdigital.com/api/2.0/data/request/Instant?tags=33711",
            "metadata": {
                "environment": "R-H-01",
                "entity_id": "PV01",
                "entity_label": "pv_panel",
                "param_name": "energy",
                "param_label": "energy"
            },
            "payload": [
                {
                    "Date": "2026-08-18T10:00:00Z",
                    "Read": 45.2
                }
            ]
        }
    ]

    # Trigger message arrival at InputHandler entrypoint callback
    input_handler._on_message_callback(raw_inbound_envelopes)

    # Allow asyncio tasks (Translator & Output Handler) to complete
    await asyncio.sleep(0.05)

    # Verify message passed through Translator and arrived at destination sink
    local_sink_mock.assert_called_once()
    delivered_payload = local_sink_mock.call_args[0][0]

    assert isinstance(delivered_payload, list)
    assert len(delivered_payload) == 1
    assert delivered_payload[0]["id"] == "PV01"
    assert "value" in delivered_payload[0]
    assert "energy" in delivered_payload[0]["value"]
    assert delivered_payload[0]["value"]["energy"][0]["value"] == 45.2

    # Clean up
    await input_handler.stop()
    await output_handler.stop()
    launcher.stop()


# ============================================================================
# 3. RABBITMQ RPC LISTENER INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("app.core.connectors.async_rabbitmq_connector.AsyncRabbitMQConnector.connect", new_callable=AsyncMock)
@patch("app.core.connectors.async_http_connector.AsyncHTTPConnector.connect", new_callable=AsyncMock)
@patch("app.core.endpoints_builders.async_rabbitmq_input_handler.AsyncRabbitMQInputHandler.start", new_callable=AsyncMock)
@patch("app.core.utils.cwlogin.CWSession.start_token_refresher_service")
async def test_cw_launcher_with_rabbitmq_rpc_listener_integration_test(
    mock_token_refresher,
    mock_rabbitmq_handler_start,
    mock_http_connect,
    mock_rabbit_connect,
    base_configurations,
    sample_environment_specs,
    mock_logger
):
    """Verifies that an AsyncRabbitMQ egress configured with RPC reply_to
    automatically materializes and starts the corresponding RPC listener InputHandler.
    """
    base_configurations["egress"] = {
        "handler_type": "AsyncRabbitMQ",
        "connection": {
            "host": "localhost",
            "port": 5672,
            "virtual_host": "/"
        },
        "topology": {
            "exchanges": [
                {
                    "exchange_name": "cleanwatts_telemetry_tx",
                    "routing_key": "cleanwatts.data",
                    "rpc_properties": {
                        "enabled": True,
                        "reply_to": {
                            "exchanges": [
                                {
                                    "exchange_name": "cleanwatts_telemetry_rx",
                                    "queues": [
                                        {
                                            "queue_name": "cleanwatts_replies_queue",
                                            "routing_key": "cleanwatts.replies"
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

    input_launcher = InputHandlersLauncher(mock_logger)
    output_launcher = OutputHandlersLauncher(mock_logger)

    launcher = CWLauncher(base_configurations, input_launcher, output_launcher)

    input_handler, output_handler, rpc_listener = await launcher.launch_environment(
        environment_name="R-H-01",
        environment_specs=sample_environment_specs
    )

    # 1. Output handler must be RabbitMQ
    assert isinstance(output_handler, AsyncRabbitMQOutputHandler)

    # 2. RPC listener must be instantiated and bound
    assert rpc_listener is not None
    assert isinstance(rpc_listener, AsyncRabbitMQInputHandler)
    mock_rabbitmq_handler_start.assert_called_once()
    assert rpc_listener._on_message_callback == output_handler.on_rpc_response

    # Clean up
    await input_handler.stop()
    await output_handler.stop()
    await rpc_listener.stop()
    launcher.stop()


# ============================================================================
# 4. LIFECYCLE & SHUTDOWN INTEGRATION TESTS
# ============================================================================

@pytest.mark.asyncio
@patch("app.core.utils.cwlogin.CWSession.stop_token_refresher_service")
@patch("app.core.utils.cwlogin.CWSession.start_token_refresher_service")
async def test_cw_launcher_stop_lifecycle_integration_test(
    mock_start_auth,
    mock_stop_auth,
    base_configurations,
    mock_logger
):
    """Verifies that launcher stop invokes CWSession cleanup and terminates background thread."""
    input_launcher = InputHandlersLauncher(mock_logger)
    output_launcher = OutputHandlersLauncher(mock_logger)

    launcher = CWLauncher(base_configurations, input_launcher, output_launcher)

    launcher.stop(timeout=1.0)

    mock_stop_auth.assert_called_once()
    assert not launcher._auth_thread.is_alive()