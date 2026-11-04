# --- Standard Library Imports ---
import asyncio
from unittest.mock import AsyncMock, patch

# --- Third-Party Imports ---
import pytest

# --- Internal Framework Imports ---
from app.core.connectors.async_rabbitmq_connector import AsyncRabbitMQConnector
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.extension.utils.labels import Label, ParamLabel, ActLabel
from app.core.utils.logger import LoggingUtils
from others.cw_translator import CWTranslator


# ============================================================================
# TEST FIXTURES
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
    """Environment specs based on 'living lab' configuration."""
    return {
        "group": "living lab",
        "entities": {
            "PV01": {
                "parameters": {
                    "energy": {
                        "id": "33711",
                        "type": "PV energy",
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 5, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": ParamLabel.ENERGY.value
                    }
                },
                "label": Label.PV_PANEL.value
            },
            "B01": {
                "parameters": {
                    "energy_in": {
                        "id": "35927",
                        "type": "Battery charging energy",
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 5, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": ParamLabel.ENERGY_IN.value
                    },
                    "energy_out": {
                        "id": "35928",
                        "type": "Battery discharging energy",
                        "measurementUnit": "kWh",
                        "temporal_behavior": {
                            "cumulative": True,
                            "periodicity": {"value": 5, "unit": "min"},
                            "aggregation_operation": "sum",
                            "fill_operation": "linear"
                        },
                        "label": ParamLabel.ENERGY_OUT.value
                    },
                    "SoC": {
                        "id": "33690",
                        "type": "Battery State of charge",
                        "measurementUnit": "%",
                        "temporal_behavior": {
                            "cumulative": False,
                            "periodicity": {"value": 1, "unit": "min"},
                            "aggregation_operation": "last",
                            "fill_operation": "linear"
                        },
                        "label": ParamLabel.STATE_OF_CHARGE.value
                    }
                },
                "actuators": {
                    "power_actuation_bat": {
                        "type": "Actuation",
                        "measurementUnit": "kW",
                        "label": ActLabel.POWER.value
                    }
                },
                "label": Label.BATTERY.value
            },
            "EVC01": {
                "parameters": {
                    "session_status": {
                        "id": "37507",
                        "type": "Session status",
                        "measurementUnit": "Status",
                        "label": ParamLabel.SESSION_STATUS.value
                    },
                    "power": {
                        "id": "41836",
                        "type": "Power",
                        "measurementUnit": "kW",
                        "temporal_behavior": {
                            "cumulative": False,
                            "periodicity": {"value": 5, "unit": "min"},
                            "aggregation_operation": "mean",
                            "fill_operation": "linear"
                        },
                        "label": ParamLabel.POWER.value
                    },
                    "electric_vehicle": {
                        "id": "EV01",
                        "type": "Name",
                        "label": ParamLabel.ELECTRIC_VEHICLE_ID.value
                    }
                },
                "actuators": {
                    "power_actuation_ev": {
                        "type": "Actuation",
                        "measurementUnit": "kW",
                        "label": ActLabel.POWER.value
                    }
                },
                "label": Label.ELECTRIC_VEHICLE_CHARGER.value
            }
        }
    }


@pytest.fixture
def translator_configurations():
    """Complete translator configuration dictionary."""
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
        "log_files": {"max_size": 10485760},
        "sourceMapping": {
            "strategy": "parameter",
            "rules": {
                "energy": "33711",
                "energy_in": "35927",
                "energy_out": "35928",
                "power": "41836",
                "SoC": "33690",
                "session_status": "37507",
                "power_actuation_bat": "35447",
                "power_actuation_ev": "39052"
            }
        }
    }

# TODO CONFIRMAR SE É ASSIM QUE FICAM AS CONFIGURAÇÕES (JUNÇÃO DE CONFIGURAÇÕES ESPECIFICAS A PROVIDER COM GERAIS)
# Não tenho a certeza se fica com alguma referencia ao ambiente na parte do sourceMapping

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
# INTEGRATION TESTS: CWTranslator -> OutputHandler
# ============================================================================

@pytest.mark.asyncio
async def test_cw_translator_generic_entity_translation_integration(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """
    Verifies end-to-end translation of a generic entity (PV01)
    and validates canonical envelope formatting dispatched via OutputHandler.
    """
    with patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock) as mock_connect, \
         patch.object(AsyncRabbitMQConnector, "close", new_callable=AsyncMock) as mock_close, \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        # 1. Launcher Setup & Output Handler Allocation
        launcher = OutputHandlersLauncher(logger=mock_logger)

        # 2. Build Output Handler through Launcher (returns tuple: output_handler, rpc_input_handler)
        output_handler, _ = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=output_topology_setup,
            server_configurations=translator_configurations.get("internal_amqp_server").get("connection")
        )

        assert mock_connect.call_count == 1
        assert len(launcher._teardown_hooks) == 1

        # 3. Instantiate ICTranslator with the OutputHandler built by the Launcher
        translator = CWTranslator(
            environment="env_1",
            environment_specs=environment_specs,
            configurations=translator_configurations,
            output_handler=output_handler,
            logger=mock_logger
        )

        # 4. Simulate raw byte payload arriving from Cleanwatts
        incoming_message = {
            "entity_id": "PV01",
            "label": "pv_panel",
            "parameters": {
                "energy": [
                    {"Date": "2026-08-17T10:00:00Z", "Read": 42.5},
                    {"Date": "2026-08-17 10:05:00", "Value": 43.1}
                ]
            }
        }

        # 5. Execute translation pipeline
        await translator.translate(incoming_message)
        await asyncio.sleep(1)

        # 6. Verify publication through Launcher-managed Output Handler
        assert mock_publish.call_count == 1

        published_kwargs = mock_publish.call_args.kwargs
        published_message = published_kwargs["message"]

        # Ensure all observations were transformed into canonical envelopes
        assert isinstance(published_message, list)
        assert len(published_message) == 1

        pv_record = published_message[0]
        assert pv_record["id"] == "PV01"
        assert "timestamp" in pv_record

        # Check readings
        energy_readings = pv_record["value"]["energy"]
        assert len(energy_readings) == 2
        assert energy_readings[0]["value"] == 42.5
        assert energy_readings[0]["timestamp"] == "2026-08-17 10:00:00 +0000"
        assert energy_readings[1]["value"] == 43.1
        assert energy_readings[1]["timestamp"] == "2026-08-17 10:05:00 +0000"

        # 7. Clean up infrastructure using Launcher teardown
        await launcher.shutdown_infrastructure()
        assert mock_close.call_count == 1
        assert len(launcher._teardown_hooks) == 0


@pytest.mark.asyncio
async def test_cw_translator_ev_charger_strategy_active_session_integration(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """
    Tests the '@register_label_strategy(\"ev_charger\")' pipeline:
    Validates that an active charging session (Read=1) dynamically associates the vehicle ID ('EV01').
    """
    with patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        launcher = OutputHandlersLauncher(logger=mock_logger)
        output_handler, _ = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=output_topology_setup,
            server_configurations=translator_configurations.get("internal_amqp_server").get("connection")
        )

        translator = CWTranslator(
            environment="env_1",
            environment_specs=environment_specs,
            configurations=translator_configurations,
            output_handler=output_handler,
            logger=mock_logger
        )

        # Incoming EV Charger message with active session (Read=1)
        evc_message = {
            "entity_id": "EVC01",
            "label": "ev_charger",
            "parameters": {
                "session_status": [
                    {"Read": 1, "DateUTC": "2026-08-17 10:00:00"}
                ],
                "power": [
                    {"Date": "2026-08-17 10:00:00", "Read": 22.0}
                ]
            }
        }

        await translator.translate(evc_message)

        published_message = mock_publish.call_args.kwargs["message"]
        evc_record = published_message[0]

        assert evc_record["id"] == "EVC01"
        assert evc_record["value"]["electric_vehicle"] == "EV01"
        assert evc_record["value"]["power"][0]["value"] == 22.0


@pytest.mark.asyncio
async def test_cw_translator_ev_charger_strategy_idle_session_integration(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """
    Validates that an idle charging session (Read=0) sets 'electric_vehicle' to empty string.
    """
    with patch.object(AsyncRabbitMQConnector, "publish", new_callable=AsyncMock) as mock_publish, \
         patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        launcher = OutputHandlersLauncher(logger=mock_logger)
        output_handler, _ = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=output_topology_setup,
            server_configurations=translator_configurations.get("internal_amqp_server").get("connection")
        )

        translator = CWTranslator(
            environment="living lab",
            environment_specs=environment_specs,
            configurations=translator_configurations,
            output_handler=output_handler,
            logger=mock_logger
        )

        # Incoming EV Charger message with idle session (Read=0)
        evc_idle_message = {
            "entity_id": "EVC01",
            "label": "ev_charger",
            "parameters": {
                "session_status": [
                    {"Read": 0, "DateUTC": "2026-08-17 10:00:00"}
                ],
                "power": [
                    {"Date": "2026-08-17 10:00:00", "Read": 0.0}
                ]
            }
        }

        await translator.translate(evc_idle_message)

        published_message = mock_publish.call_args.kwargs["message"]
        evc_record = published_message[0]

        assert evc_record["id"] == "EVC01"
        assert evc_record["value"]["electric_vehicle"] == ""
        assert evc_record["value"]["power"][0]["value"] == 0.0


@pytest.mark.asyncio
async def test_cw_translator_type_error_on_invalid_payload(
        mock_logger, environment_specs, translator_configurations, output_topology_setup
):
    """Ensures TypeError is raised when input message is not a dictionary."""
    with patch.object(AsyncRabbitMQConnector, "declare_exchange", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "connect", new_callable=AsyncMock), \
         patch.object(AsyncRabbitMQConnector, "is_connected", return_value=True):

        launcher = OutputHandlersLauncher(logger=mock_logger)
        output_handler, _ = await launcher.build_output_handler(
            handler_type="AsyncRabbitMQ",
            topology_setup=output_topology_setup,
            server_configurations=translator_configurations.get("internal_amqp_server").get("connection")
        )

        translator = CWTranslator(
            environment="living lab",
            environment_specs=environment_specs,
            configurations=translator_configurations,
            output_handler=output_handler,
            logger=mock_logger
        )

        with pytest.raises(TypeError, match="translate expected dict"):
            await translator.translate("invalid_string_payload")