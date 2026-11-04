# --- Standard Library Imports ---
import json
import pytest
import asyncio
from unittest.mock import AsyncMock, patch

# --- Application Core Imports ---
from others.ic_translator import ICTranslator
from app.core.output_handlers.async_rabbitmq_output_handler import AsyncRabbitMQOutputHandler
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector
from app.core.utils.logger import LoggingUtils
from app.extension.utils.labels import Label, ParamLabel, ActLabel
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher

# ============================================================================
# FIXTURES BASED ON YOUR SPECIFICATIONS
# ============================================================================

@pytest.fixture
def mock_logger():
    """Provides a real, configured instance of LoggingUtils for unit testing."""
    config = {
        "log_files": {
            "max_size": 1024 * 1024  # 1MB
        }
    }
    return LoggingUtils(
        component_name="async_http_connector_unit_test",
        configurations=config,
        environment="test"
    )



@pytest.fixture
def environment_specs():
    """
    Environment specs based on 'i-charging headquarters 3Phase' configuration file,
    including parameters and actuations labels matching ParamLabel enum.
    """
    return {
        "group": "i-charging headquarters 3Phase",
        "entities": {
            "AC000001_1": {
                "parameters": {
                    "power": {
                        "label": ParamLabel.POWER.value,
                        "measurementUnit": "kW"
                    },
                    "electric_vehicle_id": {
                        "label": ParamLabel.ELECTRIC_VEHICLE_ID.value,
                        "measurementUnit": "text"
                    }
                },
                "actuators": {
                    "current_mod": {
                        "id": "AC000001_1",
                        "label": ActLabel.POWER.value,
                        "measurementUnit": "kW"
                    }
                },
                "label": Label.ELECTRIC_VEHICLE_CHARGER.value,
                "serialNumber": "AC000001",
                "plug": 1
            },
            "AC000002_1": {
                "parameters": {
                    "power": {
                        "label": ParamLabel.POWER.value,
                        "measurementUnit": "kW"
                    },
                    "electric_vehicle_id": {
                        "label": ParamLabel.ELECTRIC_VEHICLE_ID.value,
                        "measurementUnit": "text"
                    }
                },
                "label": Label.ELECTRIC_VEHICLE_CHARGER.value,
                "serialNumber": "AC000002",
                "plug": 1
            },
            "PV01": {
                "parameters": {
                    "energy": {
                        "label": ParamLabel.ENERGY.value,
                        "measurementUnit": "kWh"
                    }
                },
                "label": Label.PV_PANEL.value
            },
            "grid_meter_01": {
                "parameters": {
                    "energy_in_total": {
                        "label": ParamLabel.ENERGY_IN_TOTAL.value,
                        "measurementUnit": "kWh"
                    },
                    "energy_in_l1": {
                        "label": ParamLabel.ENERGY_IN_L1.value,
                        "measurementUnit": "kWh"
                    },
                    "energy_in_l2": {
                        "label": ParamLabel.ENERGY_IN_L2.value,
                        "measurementUnit": "kWh"
                    },
                    "energy_in_l3": {
                        "label": ParamLabel.ENERGY_IN_L3.value,
                        "measurementUnit": "kWh"
                    }
                },
                "label": Label.GRID_METER.value
            }
        }
    }


@pytest.fixture
def translator_configurations():
    """
    Complete configuration dictionary.
    """
    return {
        "frequency": {
            "value": 15,
            "unit": "seconds"
        },
        "grace_period": {
            "value": 2,
            "unit": "seconds"
        },
        "timezone": "UTC",
        "internal_amqp_server": {
            "identifier": "AsyncRabbitMQ",
            "connection": {
                "host": "localhost",
                "port": 5673,
                "vhost": "private_docker_vhost",
                "heartbeat": 60,
                "timeout": 10,
                "auth": {
                    "username": "dataprovider",
                    "password": "dataprovidermq"
                },
                "ssl_conf": {
                    "enabled": False,
                    "check_hostname": False,
                    "verify_mode": "none"
                }
            }
        },
        "log_files": {
            "max_size": 10485760
        },
        "sourceMapping": {
            "strategy": "Environment"
        }
    }

# TODO CONFIRMAR SE É ASSIM QUE FICAM AS CONFIGURAÇÕES (JUNÇÃO DE CONFIGURAÇÕES ESPECIFICAS A PROVIDER COM GERAIS)


@pytest.fixture
def output_topology_setup():
    """Provides Output Handler topology settings to simulate RabbitMQ routing."""
    return {
        "exchanges": [
            {
                "exchange_name": "env_1",
                "routing_key": "",
                "exchange_properties": {"exchange_type": "fanout", "durable": True}
            }
        ]
    }


# ============================================================================
# INTEGRATION TESTS: ICTranslator -> OutputHandler
# ============================================================================

@pytest.mark.asyncio
async def test_ic_translator_to_output_handler_full_flow_integration(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """
    Integration Test:
    Verifies the end-to-end data processing pipeline for i-charging headquarters 3Phase:
    1. Resolves and builds AsyncRabbitMQ Output Handler using OutputHandlersLauncher.
    2. Parses raw i-Charging JSON byte payloads (EV Chargers, PV Production, Meter values).
    3. Runs ICTranslator strategy decorators (@register_label_strategy).
    4. Validates parameters schema and temporal values.
    5. Dispatches canonical messages through OutputHandler.send().
    6. Verifies infrastructure teardown resilience via OutputHandlersLauncher.
    """

    with patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncRabbitMQConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        # 1. Instantiate Launcher Core
        launcher = OutputHandlersLauncher(logger=mock_logger)

        # 2. Build Output Handler through Launcher (returns tuple: output_handler, rpc_input_handler)
        output_handler, rpc_handler = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=output_topology_setup,
            server_configurations=translator_configurations.get("internal_amqp_server").get("connection")
        )

        assert mock_connect.call_count == 1
        assert len(launcher._teardown_hooks) == 1

        # 3. Instantiate ICTranslator with the OutputHandler built by the Launcher
        translator = ICTranslator(
            environment="env_1",
            environment_specs=environment_specs,
            configurations=translator_configurations,
            output_handler=output_handler,
            logger=mock_logger
        )

        # 4. Simulate raw byte payload arriving from i-charging broker/source
        raw_ic_payload = {
            "observation": {
                "charging.session": [
                    {
                        "serialnumber": "AC000001",
                        "plug": 1,
                        "power": 11.5,
                        "user.id": "user_opeva_01"
                    },
                    {
                        "serialnumber": "AC000002",
                        "plug": 1,
                        "power": 7.4,
                        "user.id": "user_opeva_02"
                    }
                ],
                "pv.production": 45.8,
                "meter.values": [
                    {
                        "id": "grid_meter_01",
                        "l123": 240.0,
                        "l1": 80.0,
                        "l2": 80.0,
                        "l3": 80.0
                    }
                ]
            }
        }

        raw_bytes = json.dumps(raw_ic_payload).encode("utf-8")

        # 5. Execute translation pipeline
        await translator.translate(raw_bytes)
        await asyncio.sleep(1)

        # 6. Verify publication through Launcher-managed Output Handler
        assert mock_publish.call_count == 1

        published_kwargs = mock_publish.call_args.kwargs
        published_message = published_kwargs["message"]

        # Ensure all observations were transformed into canonical envelopes
        assert isinstance(published_message, list)
        assert len(published_message) == 4  # 2 EV Chargers + 1 PV Panel + 1 Grid Meter

        # Validate EV Charger 1 structure
        ev1_msg = next(msg for msg in published_message if msg["id"] == "AC000001_1")
        assert ev1_msg["value"]["electric_vehicle"] == "user_opeva_01"
        assert ev1_msg["value"]["power"][0]["value"] == 11.5

        # Validate PV Panel structure
        pv_msg = next(msg for msg in published_message if msg["id"] == "PV01")
        assert pv_msg["value"]["energy"][0]["value"] == 45.8

        # Validate Grid Meter calculation (15s frequency -> hours_factor = 15/3600 = 0.0041666...)
        meter_msg = next(msg for msg in published_message if msg["id"] == "grid_meter_01")
        expected_energy_total = 240.0 * (15 / 3600)  # = 1.0 kWh
        assert pytest.approx(meter_msg["value"]["energy_in_total"][0]["value"], 0.0001) == expected_energy_total

        # 7. Clean up infrastructure using Launcher teardown
        await launcher.shutdown_infrastructure()
        assert mock_close.call_count == 1
        assert len(launcher._teardown_hooks) == 0

@pytest.mark.asyncio
async def test_ic_translator_ignores_unregistered_entities_integration(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """
    Integration Test:
    Ensures entities missing from environment specifications are safely discarded
    and not forwarded to OutputHandler.send().
    """
    with patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        connector = AsyncRabbitMQConnector(translator_configurations.get("internal_amqp_server").get("connection"), logger=mock_logger)
        output_handler = AsyncRabbitMQOutputHandler(output_topology_setup, connector, mock_logger)

        translator = ICTranslator(
            "i-charging headquarters 3Phase", environment_specs,
            translator_configurations, output_handler, mock_logger
        )

        # Payload containing unregistered serial/plug combination
        unknown_payload = {
            "observation": {
                "charging.session": [
                    {"serialnumber": "UNKNOWN_SERIAL", "plug": 9, "power": 11.0}
                ]
            }
        }

        await translator.translate(json.dumps(unknown_payload).encode("utf-8"))

        # Since message list is empty, output handler receives an empty list
        published_message = mock_publish.call_args.kwargs["message"]
        assert published_message == []