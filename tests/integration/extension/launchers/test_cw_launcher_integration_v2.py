# --- Standard Library Imports ---
import asyncio
import pytest

# --- Internal Framework Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems import CWAdapterLauncher
from app.extension.telemetry_systems.cleanwatts.cw_session import CWSession


@pytest.mark.asyncio
async def test_cw_launcher_real_integration():
    """Real end-to-end integration test for CWAdapterLauncher.

    Executes:
    1. Real CWSession authentication via AsyncHTTPOutputHandler (RPC).
    2. Scheduled token refresher initialization via AsyncPeriodicScheduler.
    3. Pipeline instantiation (CWEndpointBuilder -> AsyncHTTPInputHandler -> TelemetryPipelineService -> CWAdapter).
    4. Delivery of canonical telemetry into an in-memory AsyncLocalConnector.
    """
    logger = LoggingUtils("cw_integration_test", {"log_files": {"max_size": 10485760}})
    logger.info("=== Starting Cleanwatts Integration Test ===")

    # 1. Real runtime configurations matching the standardized schema with environment-keyed source_mapping
    runtime_configurations = {
        "timezone": "UTC",
        "frequency": {
            "value": 15,
            "unit": "s"
        },
        "grace_period": {
            "value": 2,
            "unit": "s"
        },
        "log_files": {
            "max_size": 10485760
        },
        "cleanwatts": {
            "auth": {
                "refresh_interval_seconds": 3300,
                "handler_type": "AsyncHTTP",
                "connection": {
                    "url": "https://innov.cleanwattsdigital.com/api/2.0",
                    "verify_ssl": True,
                    "headers": {
                        "Content-Type": "application/json"
                    },
                    "credentials": {
                        "username": "OPEVA-demo-09",
                        "password": "bx7J658W"
                    }
                },
                "topology": {
                    "endpoints": [
                        {
                            "path": "/sessions",
                            "method": "POST",
                            "timeout_ms": 10000,
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "rpc_properties": {
                                "enabled": True
                            }
                        },
                        {
                            "path": "/sessions/refresh?token={token}&refresh_token={refresh_token}",
                            "method": "PUT",
                            "timeout_ms": 10000,
                            "headers": {
                                "Content-Type": "application/json"
                            },
                            "rpc_properties": {
                                "enabled": True
                            }
                        }
                    ]
                }
            },
            "telemetry_systems": {
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
                                "timeout_ms": 10000,
                                "headers": {
                                    "Authorization": "CW {token}"
                                }
                            }
                        ]
                    }
                },
                "egress": {
                    "handler_type": "AsyncLocal",
                    "connection": {
                        "max_queue_size": 100
                    },
                    "topology": {
                        "rpc_properties": {
                            "enabled": False
                        }
                    }
                }
            },
            "source_mapping": {
                "R-H-01": {
                    "strategy": "parameter",
                    "rules": {
                        "energy_in": "35927",
                        "SoC": "33690"
                    }
                }
            }
        }
    }

    # 2. Real environment specifications (Living Lab Battery B01)
    environment_specs = {
        "group": "living lab",
        "entities": {
            "B01": {
                "label": "battery",
                "parameters": {
                    "energy_in": {
                        "label": "energy_in",
                        "type": "Battery charging energy",
                        "measurementUnit": "kWh"
                    },
                    "SoC": {
                        "label": "state_of_charge",
                        "type": "Battery State of charge",
                        "measurementUnit": "%"
                    }
                }
            }
        }
    }

    # 3. Setup Downstream Sink to capture canonical telemetry
    local_connector = AsyncLocalConnector(
        server_configurations={"max_queue_size": 100},
        logger=logger
    )
    received_canonical_messages = []

    async def manager_sink_callback(payload):
        logger.info(f"===> [SINK RECEIVED] Canonical Telemetry: {payload}")
        received_canonical_messages.append(payload)

    local_input_handler = AsyncLocalInputHandler(
        connector=local_connector,
        on_message_callback=manager_sink_callback,
        logger=logger
    )
    await local_input_handler.start()

    # Pass the real target callable into egress connection
    runtime_configurations["cleanwatts"]["telemetry_systems"]["egress"]["connection"]["target_callable"] = (
        local_input_handler.on_message
    )

    # 4. Instantiate Handlers Launchers & CWAdapterLauncher
    input_launcher = InputHandlersLauncher(logger=logger)
    output_launcher = OutputHandlersLauncher(logger=logger)

    launcher = CWAdapterLauncher(
        configurations=runtime_configurations,
        input_launcher=input_launcher,
        output_launcher=output_launcher
    )

    ingress_handler = None
    egress_handler = None
    rpc_listener = None

    try:
        # 5. Launch Environment
        ingress_handler, egress_handler, rpc_listener = await launcher.launch_environment(
            environment_name="R-H-01",
            environment_specs=environment_specs
        )

        # 6. Verify Authentication Succeeded
        assert launcher._session.is_token_valid() is True, "CWSession token validation failed after launch!"
        assert launcher._session.get_token() is not None, "Token is None after authentication!"

        # 7. Wait for at least one polling cycle to fetch and adapt real data
        wait_seconds = 20
        while wait_seconds > 0:
            if received_canonical_messages:
                break
            await asyncio.sleep(1)
            wait_seconds -= 1

        # 8. Assertions on real canonical output reflecting schema updates
        assert len(received_canonical_messages) > 0, "No canonical messages reached the sink during polling window."

        first_reading = received_canonical_messages[0][0]

        # Entity-level assertions
        assert "id" in first_reading and first_reading["id"] == "B01"
        assert "label" in first_reading and first_reading["label"] == "battery"
        assert "timestamp" in first_reading
        assert "parameters" in first_reading

        # Parameters-level assertions
        parameters = first_reading["parameters"]

        assert "energy_in" in parameters
        assert parameters["energy_in"]["label"] == "energy_in"
        assert "values" in parameters["energy_in"]
        # Allow list (if real data was present) or None (if receiver returned empty list and added metadata)
        if parameters["energy_in"]["values"] is None:
            assert "metadata" in parameters["energy_in"]
        else:
            assert isinstance(parameters["energy_in"]["values"], list)

        assert "SoC" in parameters
        assert parameters["SoC"]["label"] == "state_of_charge"
        assert "values" in parameters["SoC"]
        if parameters["SoC"]["values"] is None:
            assert "metadata" in parameters["SoC"]
        else:
            assert isinstance(parameters["SoC"]["values"], list)

    finally:
        if ingress_handler:
            await ingress_handler.stop()
        if egress_handler:
            await egress_handler.stop()
        if rpc_listener:
            await rpc_listener.stop()
        await local_input_handler.stop()
        await launcher.stop()