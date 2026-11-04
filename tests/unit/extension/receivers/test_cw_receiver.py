# --- Standard Library Imports ---
import asyncio
import json
from unittest.mock import AsyncMock, MagicMock

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.utils.logger import LoggingUtils
from app.extension import CWReceiver
from others.cw_translator import CWTranslator


# ============================================================================
# UNIT TEST FIXTURES
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
        component_name="cw_receiver_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def mock_translator():
    """Provides a MagicMock mimicking the CWTranslator interface with an async translate method."""
    translator_mock = MagicMock(spec=CWTranslator)
    translator_mock.translate = AsyncMock()
    return translator_mock


@pytest.fixture
def valid_entities_specs():
    """Provides standard Cleanwatts entities schema specifications."""
    return {
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
def receiver(mock_translator, mock_logger, valid_entities_specs):
    """Fixture returning an initialized CWReceiver instance."""
    return CWReceiver(
        environment_name="R-H-01",
        translator=mock_translator,
        logger=mock_logger,
        environment_specs=valid_entities_specs
    )


# ============================================================================
# 1. INITIALIZATION & SCHEMA PARSING UNIT TESTS
# ============================================================================

def test_init_with_nested_entities_key_unit_test(mock_translator, mock_logger, valid_entities_specs):
    """Verifies that initialization correctly extracts the nested 'entities' dictionary."""
    inst = CWReceiver(
        environment_name="R-H-01",
        translator=mock_translator,
        logger=mock_logger,
        environment_specs=valid_entities_specs
    )
    assert inst._environment_name == "R-H-01"
    assert "PV01" in inst._entities
    assert "B01" in inst._entities
    assert inst._translator == mock_translator

def test_init_raises_key_error_when_entities_key_missing_unit_test(mock_translator, mock_logger):
    """Verifies that initialization raises KeyError when 'entities' key is absent."""
    invalid_specs = {"group": "living lab"}

    with pytest.raises(KeyError, match="Mandatory key 'entities' is missing"):
        CWReceiver(
            environment_name="R-H-01",
            translator=mock_translator,
            logger=mock_logger,
            environment_specs=invalid_specs
        )


def test_init_raises_value_error_when_entities_is_empty_unit_test(mock_translator, mock_logger):
    """Verifies that initialization raises ValueError when 'entities' dictionary is empty."""
    empty_entities_specs = {"entities": {}}

    with pytest.raises(ValueError, match="'entities' must be a non-empty dictionary"):
        CWReceiver(
            environment_name="R-H-01",
            translator=mock_translator,
            logger=mock_logger,
            environment_specs=empty_entities_specs
        )

# ============================================================================
# 2. URL RESOLUTION FALLBACK UNIT TESTS (_resolve_entity_from_url)
# ============================================================================

def test_resolve_entity_from_url_success_unit_test(receiver):
    """Verifies correct resolution of entity_id, param_name, and labels via URL tag query."""
    url = "https://api.cleanwatts.com/data/request/Instant?instantType=avg&tags=33690&from=...&to=..."
    ent_id, param_name, ent_label, param_label = receiver._resolve_entity_from_url(url)

    assert ent_id == "B01"
    assert param_name == "SoC"
    assert ent_label == "battery"
    assert param_label == "state_of_charge"


def test_resolve_entity_from_url_with_scalar_tag_value_unit_test(mock_translator, mock_logger):
    """Verifies URL tag resolution when parameters are structured as scalar strings/ints rather than dicts."""
    scalar_specs = {
        "entities": {
            "M01": {
                "label": "meter",
                "parameters": {
                    "id_1": {
                        "id": "99999",
                        "label": "grid_power"
                    }
                }
            }
        }
    }
    custom_receiver = CWReceiver(
        environment_name="R-H-01",
        translator=mock_translator,
        logger=mock_logger,
        environment_specs=scalar_specs
    )
    url = "https://api.cleanwatts.com/data/request/Instant?tags=99999"
    ent_id, param_name, ent_label, param_label = custom_receiver._resolve_entity_from_url(url)

    assert ent_id == "M01"
    assert param_name == "id_1"
    assert ent_label == "meter"
    assert param_label == "grid_power"


def test_resolve_entity_from_url_unmatched_tag_unit_test(receiver):
    """Verifies that unmatched URL query tags safely return None values."""
    url = "https://api.cleanwatts.com/data/request/Instant?tags=00000"
    ent_id, param_name, ent_label, param_label = receiver._resolve_entity_from_url(url)

    assert ent_id is None
    assert param_name is None
    assert ent_label is None
    assert param_label is None


# ============================================================================
# 3. MESSAGE CALLBACK & TRANSLATOR DISPATCH UNIT TESTS (_callback)
# ============================================================================

@pytest.mark.asyncio
async def test_callback_dispatches_consolidated_batch_with_metadata_unit_test(receiver, mock_translator):
    """Verifies that envelopes containing complete metadata are aggregated and dispatched to translator."""
    raw_payload_energy_in = [{"Date": "2026-08-18T10:00:00Z", "Read": 12.5}]
    raw_payload_soc = [{"Date": "2026-08-18T10:00:00Z", "Read": 85.0}]

    messages_batch = [
        {
            "source": "https://api.cleanwatts.com/data/request/Instant?tags=35927",
            "metadata": {
                "entity_id": "B01",
                "param_name": "energy_in",
                "entity_label": "battery",
                "param_label": "energy_in"
            },
            "payload": raw_payload_energy_in
        },
        {
            "source": "https://api.cleanwatts.com/data/request/Instant?tags=33690",
            "metadata": {
                "entity_id": "B01",
                "param_name": "SoC",
                "entity_label": "battery",
                "param_label": "state_of_charge"
            },
            "payload": raw_payload_soc
        }
    ]

    receiver._callback(messages_batch)
    await asyncio.sleep(0.01)

    mock_translator.translate.assert_called_once()
    dispatched_payload = mock_translator.translate.call_args[0][0]

    assert dispatched_payload["entity_id"] == "B01"
    assert dispatched_payload["label"] == "battery"
    assert "energy_in" in dispatched_payload["parameters"]
    assert "SoC" in dispatched_payload["parameters"]
    assert dispatched_payload["parameters"]["energy_in"]["label"] == "energy_in"
    assert dispatched_payload["parameters"]["energy_in"]["data"] == raw_payload_energy_in
    assert dispatched_payload["parameters"]["SoC"]["label"] == "state_of_charge"
    assert dispatched_payload["parameters"]["SoC"]["data"] == raw_payload_soc


@pytest.mark.asyncio
async def test_callback_deserializes_bytes_and_json_string_payloads_unit_test(receiver, mock_translator):
    """Verifies that bytes and serialized JSON string payloads are parsed into dicts/lists before dispatching."""
    data_dict_1 = [{"Date": "2026-08-18T10:00:00Z", "Read": 22.4}]
    data_dict_2 = [{"Date": "2026-08-18T10:00:00Z", "Read": 80.0}]

    bytes_payload = json.dumps(data_dict_1).encode("utf-8")
    string_payload = json.dumps(data_dict_2)

    messages_batch = [
        {
            "metadata": {
                "entity_id": "B01",
                "param_name": "energy_in",
                "entity_label": "battery",
                "param_label": "energy_in"
            },
            "payload": bytes_payload
        },
        {
            "metadata": {
                "entity_id": "B01",
                "param_name": "SoC",
                "entity_label": "battery",
                "param_label": "state_of_charge"
            },
            "payload": string_payload
        }
    ]

    receiver._callback(messages_batch)
    await asyncio.sleep(0.01)

    mock_translator.translate.assert_called_once()
    dispatched = mock_translator.translate.call_args[0][0]

    assert dispatched["parameters"]["energy_in"]["data"] == data_dict_1
    assert dispatched["parameters"]["SoC"]["data"] == data_dict_2

def test_callback_synchronous_translator_dispatch_unit_test(receiver):
    """Verifies that synchronous translator translate methods are executed directly."""
    sync_translator = MagicMock()
    # Ensure translate is a synchronous callable (not a coroutine function)
    sync_translator.translate = MagicMock()

    receiver._translator = sync_translator
    message = {
        "metadata": {
            "entity_id": "PV01",
            "param_name": "energy",
            "entity_label": "pv_panel",
            "param_label": "energy"
        },
        "payload": [{"Value": 10.0}]
    }

    receiver._callback(message)

    sync_translator.translate.assert_called_once()
    payload = sync_translator.translate.call_args[0][0]
    assert payload["entity_id"] == "PV01"


# ============================================================================
# 4. GUARD CLAUSE & EDGE CASE UNIT TESTS
# ============================================================================

def test_callback_ignores_empty_or_null_messages_unit_test(receiver, mock_translator):
    """Verifies that empty payloads or lists return immediately without side effects."""
    receiver._callback([])
    receiver._callback({})
    receiver._callback(None)

    mock_translator.translate.assert_not_called()


def test_callback_skips_unresolvable_envelopes_unit_test(receiver, mock_translator):
    """Verifies that envelopes without metadata and with unmatched URLs are ignored."""
    invalid_batch = [
        {"source": "https://api.cleanwatts.com/unknown?tags=99999", "payload": [{"Value": 1}]},
        "non_dict_message_item"
    ]

    receiver._callback(invalid_batch)

    mock_translator.translate.assert_not_called()


@pytest.mark.asyncio
async def test_callback_handles_raw_unparseable_string_payload_gracefully_unit_test(receiver, mock_translator):
    """Verifies that malformed string payloads that fail JSON decoding are retained as raw strings."""
    raw_text = "Internal Server Error 500"
    message = {
        "metadata": {
            "entity_id": "PV01",
            "param_name": "energy",
            "entity_label": "pv_panel",
            "param_label": "energy"
        },
        "payload": raw_text
    }

    receiver._callback(message)
    await asyncio.sleep(0.01)

    mock_translator.translate.assert_called_once()
    dispatched = mock_translator.translate.call_args[0][0]
    assert dispatched["parameters"]["energy"]["data"] == raw_text