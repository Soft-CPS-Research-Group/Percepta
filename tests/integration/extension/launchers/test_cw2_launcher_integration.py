# --- Standard Library Imports ---
import asyncio
import pytest

# --- Internal Framework Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems import CW2AdapterLauncher


@pytest.mark.asyncio
async def test_cw2_launcher_real_integration():
    """Real end-to-end integration test for CW2AdapterLauncher.

    Executes:
    1. Pipeline bootstrapping (CW2EndpointBuilder -> AsyncRabbitMQInputHandler -> TelemetryPipelineService -> CW2Adapter).
    2. Connection to Cleanwatts Shelly RabbitMQ broker with dynamic consumer queue 'isep_consumer_{random_hash}'.
    3. Delivery and verification of canonical telemetry into an in-memory AsyncLocalConnector sink.
    """
    logger = LoggingUtils("cw2_integration_test", {"log_files": {"max_size": 10485760}})
    logger.info("=== Starting Cleanwatts v2 (Shelly) Integration Test for R-H-02 ===")

    # 1. Real runtime configurations matching the standardized schema with environment-keyed source_mapping
    runtime_configurations = {
        "timezone": "UTC",
        "log_files": {
            "max_size": 10485760
        },
        "cleanwatts_2": {
            "telemetry_systems": {
                "ingress": {
                    "handler_type": "AsyncRabbitMQ",
                    "connection": {
                        "host": "rabbit.cleanwatts.energy",
                        "port": 5671,
                        "vhost": "iEnergyInnov",
                        "heartbeat": 660,
                        "auth": {
                            "username": "isep_opeva",
                            "password": "tM070MJ7K5PY"
                        },
                        "ssl_conf": {
                            "enabled": True,
                            "check_hostname": False,
                            "verify_mode": None
                        }
                    },
                    "topology": {
                        "exchanges": [
                            {
                                "exchange_name": "{exchange_name}",
                                "exchange_properties": {
                                    "exchange_type": "fanout",
                                    "durable": False
                                },
                                "queues": [
                                    {
                                        "queue_name": "isep_consumer_{random_hash}",
                                        "routing_key": "",
                                        "consume_properties": {
                                            "no_ack": False,
                                            "requeue": False
                                        },
                                        "queue_properties": {
                                            "durable": True,
                                            "exclusive": True
                                        }
                                    }
                                ]
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
                "R-H-02": {
                    "strategy": "entity",
                    "rules": {
                        "GR02": "Shelly-shellyproem50-08f9e0e6a5a4"
                    }
                }
            }
        }
    }

    # 2. Real environment specifications for R-H-02
    environment_specs = {
        "group": "living lab",
        "entities": {
            "GR02": {
                "label": "grid_meter",
                "parameters": {
                    "energy_in_l1": {
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 1, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": "energy_in_l1"
                    },
                    "energy_in_total": {
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 1, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": "energy_in_total"
                    },
                    "energy_out_l1": {
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 1, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": "energy_out_l1"
                    },
                    "energy_out_total": {
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 1, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": "energy_out_total"
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

    # Pass target sink callable into egress connection
    runtime_configurations["cleanwatts_2"]["telemetry_systems"]["egress"]["connection"]["target_callable"] = (
        local_input_handler.on_message
    )

    # 4. Instantiate Launchers
    input_launcher = InputHandlersLauncher(logger=logger)
    output_launcher = OutputHandlersLauncher(logger=logger)

    launcher = CW2AdapterLauncher(
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
            environment_name="R-H-02",
            environment_specs=environment_specs
        )

        # 6. Wait for baseline (1st reading) + delta calculation (2nd reading, ~60s period)
        wait_seconds = 150
        while wait_seconds > 0:
            if received_canonical_messages:
                break
            await asyncio.sleep(1)
            wait_seconds -= 1

        # 7. Verify Canonical Messages reflecting schema updates
        assert len(received_canonical_messages) > 0, "No canonical telemetry messages received from RabbitMQ broker."

        first_reading = received_canonical_messages[0][0]

        # Entity-level assertions
        assert "id" in first_reading and first_reading["id"] == "GR02"
        assert "label" in first_reading and first_reading["label"] == "grid_meter"
        assert "timestamp" in first_reading
        assert "parameters" in first_reading

        # Parameters-level assertions
        parameters = first_reading["parameters"]
        expected_param_keys = ["energy_in_l1", "energy_in_total", "energy_out_l1", "energy_out_total"]

        for param_key in expected_param_keys:
            assert param_key in parameters, f"Parameter '{param_key}' missing from adapted output."
            param_data = parameters[param_key]
            assert "label" in param_data
            assert "values" in param_data

            # Validates values structure if present or metadata when empty/none
            if param_data["values"] is None:
                assert "metadata" in param_data
            else:
                assert isinstance(param_data["values"], list)
                if param_data["values"]:
                    reading_sample = param_data["values"][0]
                    assert "timestamp" in reading_sample
                    assert "value" in reading_sample

    finally:
        if ingress_handler:
            await ingress_handler.stop()
        if egress_handler:
            await egress_handler.stop()
        if rpc_listener:
            await rpc_listener.stop()
        await local_input_handler.stop()
        launcher.stop()