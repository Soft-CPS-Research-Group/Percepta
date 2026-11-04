# --- Standard Library Imports ---
import datetime
import json

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems import CW2TelemetryAdapter, _LABEL_STRATEGIES


# ============================================================================
# CONSTANTS
# ============================================================================

EXCHANGE = "Shelly-shellyproem50-08f9e0e6a5a4"
SOURCE = "Shelly-shellyproem50-08f9e0e6a5a4/"
TOPIC = "shellyproem50-08f9e0e6a5a4/status/emdata:0"
T0 = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)


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
        component_name="cw2_adapter_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_environment_specs():
    """Provides a grid meter entity with two parameters on the same Shelly topic:
    one reported as a lifetime total (in kWh) and one reported per interval (in Wh)."""
    return {
        "entities": {
            "GR02": {
                "entity_type_id": "grid_meter",
                "parameters": {
                    "32df": {
                        "measurement_unit": "kWh",
                        "topic": "/status/emdata:0",
                        "shelly_accumulation": "lifetime",
                        "parameter_type_id": "energy_in_l1"
                    },
                    "67fgh": {
                        "measurement_unit": "Wh",
                        "topic": "/status/emdata:0",
                        "shelly_accumulation": "interval",
                        "parameter_type_id": "energy_in_total"
                    }
                }
            }
        }
    }


@pytest.fixture
def valid_source_mapping_rules():
    """Provides the entity-id-to-exchange mapping matching `valid_environment_specs`."""
    return {"GR02": EXCHANGE}


@pytest.fixture
def adapter(mock_logger, valid_environment_specs, valid_source_mapping_rules):
    """Fixture returning an initialized CW2TelemetryAdapter instance, using UTC."""
    return CW2TelemetryAdapter(
        environment="R-H-02",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        timezone="UTC"
    )


@pytest.fixture
def offset_adapter(mock_logger, valid_environment_specs, valid_source_mapping_rules):
    """Fixture returning an adapter with a fixed non-UTC offset (+03:00, no daylight saving),
    used to verify that timestamps follow the configured timezone."""
    return CW2TelemetryAdapter(
        environment="R-H-02",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        timezone="Etc/GMT-3"
    )


def param_spec(parameter_type_id, unit="kWh", accumulation="interval", topic="/status/emdata:0"):
    """Builds a parameter definition. A unit or topic of None leaves that key out."""
    spec = {
        "parameter_type_id": parameter_type_id,
        "shelly_accumulation": accumulation
    }
    if unit is not None:
        spec["measurement_unit"] = unit
    if topic is not None:
        spec["topic"] = topic
    return spec


def build_adapter(mock_logger, parameters, entity_type_id="grid_meter", timezone="UTC", exchange=EXCHANGE):
    """Builds an adapter with a single entity (GR02) and the given parameters.
    An entity_type_id of None leaves 'entity_type_id' out of the entity."""
    entity = {"parameters": parameters}
    if entity_type_id is not None:
        entity["entity_type_id"] = entity_type_id

    return CW2TelemetryAdapter(
        environment="R-H-02",
        environment_specs={"entities": {"GR02": entity}},
        source_mapping_rules={"GR02": exchange},
        logger=mock_logger,
        timezone=timezone
    )


def make_shelly_envelope(message_content=None, source=SOURCE, topic=TOPIC, payload=None):
    """Builds an AMQP envelope as produced by PayloadNormalizer.from_rabbitmq.

    By default, the payload carries the given Shelly message as the real devices send it:
    UTF-8 encoded JSON holding the topic and, in 'Message', the Shelly message as a JSON string.
    A ready-made payload can be given instead, to build envelopes with unusual payloads.
    """
    if payload is None:
        content = message_content if message_content is not None else {}
        payload = json.dumps({"Topic": topic, "Message": json.dumps(content)}).encode("utf-8")

    return {
        "protocol": "AMQP",
        "source": source,
        "status": None,
        "content_type": "application/octet-stream",
        "metadata": {},
        "payload": payload
    }


def format_timestamp(dt):
    """Formats a datetime the same way the adapter does."""
    return dt.strftime("%Y-%m-%d %H:%M:%S %z")


def call_grid_meter(adapter, raw_payload, now_dt, parameters_ids=None):
    """Helper invoking the grid meter strategy directly, with a controlled clock.
    By default, it processes all of the entity's parameters."""
    entity_parameters = adapter._entities["GR02"]["parameters"]
    if parameters_ids is None:
        parameters_ids = list(entity_parameters.keys())

    return adapter._grid_meter(
        entity_id="GR02",
        entity_parameters=entity_parameters,
        parameters_ids=parameters_ids,
        raw_payload=raw_payload,
        now_dt=now_dt,
        timestamp=format_timestamp(now_dt)
    )


# ============================================================================
# 1. INITIALIZATION & SCHEMA PARSING UNIT TESTS
# ============================================================================

def test_init_builds_source_lookup_from_the_mapping_unit_test(adapter):
    """Verifies that the source lookup is the mapping inverted: each exchange name, in
    lowercase, points to its entity."""
    assert adapter._source_to_entity == {"shelly-shellyproem50-08f9e0e6a5a4": "GR02"}


def test_init_strips_slashes_from_exchange_name_unit_test(mock_logger):
    """Verifies that leading and trailing slashes in the configured exchange name are ignored."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total")}, exchange="/Shelly-abc/")

    assert custom_adapter._source_to_entity == {"shelly-abc": "GR02"}


def test_init_starts_with_no_stored_lifetime_values_unit_test(adapter):
    """Verifies that no lifetime baseline exists before any message is processed."""
    assert adapter._last_parameter_values == {}


def test_init_raises_value_error_when_entities_key_missing_unit_test(mock_logger, valid_source_mapping_rules):
    """Verifies that initialization raises ValueError when 'entities' key is absent."""
    with pytest.raises(ValueError, match="entities"):
        CW2TelemetryAdapter(
            environment="R-H-02",
            environment_specs={"group": "living lab"},
            source_mapping_rules=valid_source_mapping_rules,
            logger=mock_logger
        )


def test_init_raises_value_error_when_entities_is_empty_unit_test(mock_logger, valid_source_mapping_rules):
    """Verifies that initialization raises ValueError when 'entities' dictionary is empty."""
    with pytest.raises(ValueError, match="cannot be empty"):
        CW2TelemetryAdapter(
            environment="R-H-02",
            environment_specs={"entities": {}},
            source_mapping_rules=valid_source_mapping_rules,
            logger=mock_logger
        )


def test_init_raises_type_error_when_source_mapping_rules_not_dict_unit_test(mock_logger, valid_environment_specs):
    """Verifies that initialization raises TypeError when 'source_mapping_rules' is not a dictionary."""
    with pytest.raises(TypeError, match="must be a dictionary"):
        CW2TelemetryAdapter(
            environment="R-H-02",
            environment_specs=valid_environment_specs,
            source_mapping_rules=["not", "a", "dict"],
            logger=mock_logger
        )


def test_grid_meter_strategy_is_registered_unit_test():
    """Verifies that the grid meter strategy is registered under its entity type id."""
    assert "grid_meter" in _LABEL_STRATEGIES


# ============================================================================
# 2. INGRESS NORMALIZATION UNIT TESTS (_normalize_ingress_payload)
# ============================================================================

def test_normalize_ingress_payload_resolves_entity_parameters_and_payload_unit_test(adapter):
    """Verifies that a valid envelope is resolved into its entity, its parameters for the topic,
    and the decoded Shelly message."""
    envelope = make_shelly_envelope({"id": 0, "total_act_energy": 45.2})

    resolved = adapter._normalize_ingress_payload(envelope)

    assert resolved == {
        "id": "GR02",
        "entity_type_id": "grid_meter",
        "parameters_ids": ["32df", "67fgh"],
        "raw_payload": {"id": 0, "total_act_energy": 45.2}
    }


def test_normalize_ingress_payload_ignores_source_case_unit_test(adapter):
    """Verifies that the source is matched regardless of letter case."""
    envelope = make_shelly_envelope({"total_act_energy": 1}, source="SHELLY-SHELLYPROEM50-08F9E0E6A5A4/")

    assert adapter._normalize_ingress_payload(envelope)["id"] == "GR02"


def test_normalize_ingress_payload_does_not_match_source_without_prefix_unit_test(adapter):
    """Verifies that a source that differs from the configured exchange name, such as one
    without the 'Shelly-' prefix, is not matched."""
    envelope = make_shelly_envelope({"total_act_energy": 1}, source="shellyproem50-08f9e0e6a5a4/")

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_accepts_str_payload_unit_test(adapter):
    """Verifies that a payload that is a JSON string, rather than bytes, is decoded."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Topic": TOPIC, "Message": json.dumps({"total_act_energy": 1})})
    )

    assert adapter._normalize_ingress_payload(envelope)["raw_payload"] == {"total_act_energy": 1}


def test_normalize_ingress_payload_accepts_already_decoded_payload_unit_test(adapter):
    """Verifies that a payload that is already a dictionary is used as it is."""
    envelope = make_shelly_envelope(
        payload={"Topic": TOPIC, "Message": json.dumps({"total_act_energy": 1})}
    )

    assert adapter._normalize_ingress_payload(envelope)["raw_payload"] == {"total_act_energy": 1}


def test_normalize_ingress_payload_accepts_already_decoded_shelly_message_unit_test(adapter):
    """Verifies that a Shelly 'Message' that is already a dictionary is used as it is."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Topic": TOPIC, "Message": {"total_act_energy": 1}}).encode("utf-8")
    )

    assert adapter._normalize_ingress_payload(envelope)["raw_payload"] == {"total_act_energy": 1}


def test_normalize_ingress_payload_returns_none_for_non_dict_message_unit_test(adapter):
    """Verifies that a message that is not a dictionary is rejected gracefully."""
    assert adapter._normalize_ingress_payload("not a dict") is None
    assert adapter._normalize_ingress_payload(None) is None


def test_normalize_ingress_payload_returns_none_for_unknown_source_unit_test(adapter):
    """Verifies that a source that maps to no entity is discarded."""
    envelope = make_shelly_envelope({"total_act_energy": 1}, source="Shelly-unknown-device/")

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_source_is_missing_unit_test(adapter):
    """Verifies that an envelope with no source is discarded."""
    envelope = make_shelly_envelope({"total_act_energy": 1})
    del envelope["source"]

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_for_invalid_json_payload_unit_test(adapter):
    """Verifies that a payload that is not valid JSON is discarded instead of raising."""
    envelope = make_shelly_envelope(payload=b"not json at all")

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_for_undecodable_bytes_unit_test(adapter):
    """Verifies that a payload that is not valid UTF-8 is discarded instead of raising."""
    envelope = make_shelly_envelope(payload=b"\xff\xfe")

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_payload_is_not_a_dict_unit_test(adapter):
    """Verifies that a payload that decodes to something other than a dictionary is discarded."""
    envelope = make_shelly_envelope(payload=json.dumps([1, 2, 3]).encode("utf-8"))

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_topic_matches_no_parameter_unit_test(adapter):
    """Verifies that a message whose topic matches none of the entity's parameters is discarded."""
    envelope = make_shelly_envelope({"total_act_energy": 1}, topic="shellyproem50-08f9e0e6a5a4/status/switch:0")

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_topic_is_missing_unit_test(adapter):
    """Verifies that a message with no topic matches no parameter."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Message": json.dumps({"total_act_energy": 1})}).encode("utf-8")
    )

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_matches_the_end_of_the_topic_unit_test(mock_logger):
    """Verifies that a parameter's topic must match the end of the message's topic, so a topic
    ending in ':10' does not match a parameter configured for ':1'."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", topic="/status/emdata:1")})
    envelope = make_shelly_envelope({"total_act_energy": 1}, topic="shellyproem50-08f9e0e6a5a4/status/emdata:10")

    assert custom_adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_matches_exact_topic_end_unit_test(mock_logger):
    """Verifies that a topic that ends exactly with the parameter's topic is matched."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", topic="/status/emdata:1")})
    envelope = make_shelly_envelope({"total_act_energy": 1}, topic="shellyproem50-08f9e0e6a5a4/status/emdata:1")

    assert custom_adapter._normalize_ingress_payload(envelope)["parameters_ids"] == ["p1"]


def test_normalize_ingress_payload_only_returns_parameters_of_the_topic_unit_test(mock_logger):
    """Verifies that only the parameters configured for the message's topic are returned."""
    custom_adapter = build_adapter(mock_logger, {
        "on_emdata": param_spec("energy_in_total", topic="/status/emdata:0"),
        "on_switch": param_spec("energy_out_total", topic="/status/switch:0")
    })

    resolved = custom_adapter._normalize_ingress_payload(make_shelly_envelope({"total_act_energy": 1}))

    assert resolved["parameters_ids"] == ["on_emdata"]


def test_normalize_ingress_payload_ignores_parameters_without_topic_unit_test(mock_logger):
    """Verifies that a parameter with no configured topic is never matched."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", topic=None)})

    assert custom_adapter._normalize_ingress_payload(make_shelly_envelope({"total_act_energy": 1})) is None


def test_normalize_ingress_payload_returns_none_for_invalid_shelly_message_unit_test(adapter):
    """Verifies that a Shelly 'Message' that is not valid JSON is discarded instead of raising."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Topic": TOPIC, "Message": "not json"}).encode("utf-8")
    )

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_shelly_message_is_missing_unit_test(adapter):
    """Verifies that a message with no 'Message' field is discarded."""
    envelope = make_shelly_envelope(payload=json.dumps({"Topic": TOPIC}).encode("utf-8"))

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_when_shelly_message_is_not_a_dict_unit_test(adapter):
    """Verifies that a Shelly 'Message' that decodes to something other than a dictionary is discarded."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Topic": TOPIC, "Message": json.dumps([1])}).encode("utf-8")
    )

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_does_not_default_the_entity_type_unit_test(mock_logger):
    """Verifies that an entity with no 'entity_type_id' is reported with no type, rather than
    being given a default one."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total")}, entity_type_id=None)

    resolved = custom_adapter._normalize_ingress_payload(make_shelly_envelope({"total_act_energy": 1}))

    assert resolved["id"] == "GR02"
    assert resolved["entity_type_id"] is None


# ============================================================================
# 3. GRID METER UNIT TESTS — KEY SELECTION AND KWH CONVERSION (_grid_meter)
# ============================================================================

@pytest.mark.parametrize("parameter_type_id, key", [
    ("energy_in_l1", "a_total_act_energy"),
    ("energy_out_l1", "a_total_act_ret_energy"),
    ("energy_in_l2", "b_total_act_energy"),
    ("energy_out_l2", "b_total_act_ret_energy"),
    ("energy_in_l3", "c_total_act_energy"),
    ("energy_out_l3", "c_total_act_ret_energy"),
    ("energy_in_total", "total_act"),
    ("energy_in_total", "total_act_energy"),
    ("energy_out_total", "total_act_ret"),
    ("energy_out_total", "total_act_ret_energy"),
])
def test_grid_meter_reads_the_expected_key_for_each_parameter_type_unit_test(mock_logger, parameter_type_id, key):
    """Verifies that each parameter type is read from the Shelly keys that report it."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec(parameter_type_id)})

    result = call_grid_meter(custom_adapter, {key: 2.5}, T0)

    assert result["p1"]["parameter_type_id"] == parameter_type_id
    assert result["p1"]["values"][0]["value"] == 2.5


def test_grid_meter_prefers_the_first_candidate_key_unit_test(adapter):
    """Verifies that, when several candidate keys are present, the first one in the list is used."""
    result = call_grid_meter(adapter, {"total_act": 2000, "total_act_energy": 5000}, T0, ["67fgh"])

    assert result["67fgh"]["values"][0]["value"] == pytest.approx(2.0)


def test_grid_meter_falls_back_to_a_later_candidate_key_unit_test(adapter):
    """Verifies that a later candidate key is used when the earlier ones are absent."""
    result = call_grid_meter(adapter, {"total_act_energy": 5000}, T0, ["67fgh"])

    assert result["67fgh"]["values"][0]["value"] == pytest.approx(5.0)


def test_grid_meter_skips_parameter_when_its_key_is_absent_unit_test(adapter):
    """Verifies that a parameter is skipped when the message carries none of its keys."""
    assert call_grid_meter(adapter, {"unrelated_key": 1}, T0, ["67fgh"]) == {}


def test_grid_meter_ignores_keys_of_other_parameter_types_unit_test(adapter):
    """Verifies that a key that belongs to another parameter type is not used."""
    assert call_grid_meter(adapter, {"b_total_act_energy": 1}, T0, ["67fgh"]) == {}


@pytest.mark.parametrize("unit, raw_value, expected_kwh", [
    ("Wh", 1500, 1.5),
    ("Wh", 1500, 1.5),
    ("Wh", 1500, 1.5),
    ("kWh", 1.5, 1.5),
    ("kWh", 1.5, 1.5),
    ("KWH", 1.5, 1.5),
    ("  kWh  ", 1.5, 1.5),
    ("MWh", 0.002, 2.0),
    ("MWh", 0.002, 2.0),
])
def test_grid_meter_converts_reading_to_kwh_unit_test(mock_logger, unit, raw_value, expected_kwh):
    """Verifies that a reading is converted to kWh from its parameter's unit, whatever the
    spelling or letter case of that unit."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", unit=unit)})

    result = call_grid_meter(custom_adapter, {"total_act_energy": raw_value}, T0)

    assert result["p1"]["values"][0]["value"] == pytest.approx(expected_kwh)


@pytest.mark.parametrize("unit", ["kW", "W", "", "unknown", None])
def test_grid_meter_skips_parameter_with_missing_or_unknown_unit_unit_test(mock_logger, unit):
    """Verifies that a parameter is skipped when its unit is missing or is not a known
    energy unit, since its reading cannot be converted safely."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", unit=unit)})

    assert call_grid_meter(custom_adapter, {"total_act_energy": 1500}, T0) == {}


def test_grid_meter_skips_only_the_parameter_with_unknown_unit_unit_test(mock_logger):
    """Verifies that a parameter with an unknown unit does not prevent the others from being reported."""
    custom_adapter = build_adapter(mock_logger, {
        "bad": param_spec("energy_in_total", unit="kW"),
        "good": param_spec("energy_out_total", unit="kWh")
    })

    result = call_grid_meter(custom_adapter, {"total_act_energy": 1, "total_act_ret_energy": 2}, T0)

    assert list(result.keys()) == ["good"]


def test_grid_meter_skips_unknown_unit_before_storing_a_baseline_unit_test(mock_logger):
    """Verifies that a parameter skipped for its unit does not store a lifetime baseline."""
    custom_adapter = build_adapter(
        mock_logger, {"p1": param_spec("energy_in_total", unit="kW", accumulation="lifetime")}
    )

    call_grid_meter(custom_adapter, {"total_act_energy": 100}, T0)

    assert "p1" not in custom_adapter._last_parameter_values


def test_grid_meter_rounds_value_to_six_decimal_places_unit_test(mock_logger):
    """Verifies that the converted value is rounded to six decimal places."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total", unit="Wh")})

    result = call_grid_meter(custom_adapter, {"total_act_energy": 1234.5678901}, T0)

    assert result["p1"]["values"][0]["value"] == 1.234568


def test_grid_meter_output_has_expected_structure_unit_test(adapter):
    """Verifies the structure of a parameter in the output, and that the given timestamp is used."""
    result = call_grid_meter(adapter, {"total_act_energy": 45.2}, T0, ["67fgh"])

    assert result == {
        "67fgh": {
            "parameter_type_id": "energy_in_total",
            "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": pytest.approx(0.0452)}]
        }
    }


def test_grid_meter_skips_parameter_without_parameter_type_id_unit_test(mock_logger):
    """Verifies that a parameter with no 'parameter_type_id' is skipped."""
    custom_adapter = build_adapter(mock_logger, {"p1": {"measurement_unit": "kWh", "topic": "/status/emdata:0"}})

    assert call_grid_meter(custom_adapter, {"total_act_energy": 1}, T0) == {}


def test_grid_meter_skips_unknown_parameter_id_unit_test(adapter):
    """Verifies that a parameter id that is not defined for the entity is skipped."""
    assert call_grid_meter(adapter, {"total_act_energy": 1}, T0, ["does_not_exist"]) == {}


def test_grid_meter_skips_parameter_type_without_known_keys_unit_test(mock_logger):
    """Verifies that a parameter type the Shelly has no keys for is skipped."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("power")})

    assert call_grid_meter(custom_adapter, {"total_act_energy": 1}, T0) == {}


# ============================================================================
# 4. GRID METER UNIT TESTS — INVALID READINGS (_grid_meter)
# ============================================================================

@pytest.mark.parametrize("bad_value", ["abc", "12.5", True, False, [1], {"x": 1}])
def test_grid_meter_skips_non_numeric_reading_unit_test(mock_logger, bad_value):
    """Verifies that a reading that is not a number is skipped. Booleans and numeric strings
    do not count as numbers."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total")})

    assert call_grid_meter(custom_adapter, {"total_act_energy": bad_value}, T0) == {}


def test_grid_meter_skips_null_reading_unit_test(adapter):
    """Verifies that a reading that is null is skipped."""
    assert call_grid_meter(adapter, {"total_act_energy": None}, T0, ["67fgh"]) == {}


def test_grid_meter_keeps_zero_reading_unit_test(adapter):
    """Verifies that a reading of zero is a valid reading and is kept."""
    result = call_grid_meter(adapter, {"total_act_energy": 0}, T0, ["67fgh"])

    assert result["67fgh"]["values"][0]["value"] == 0


def test_grid_meter_accepts_integer_readings_unit_test(adapter):
    """Verifies that an integer reading is accepted and converted."""
    result = call_grid_meter(adapter, {"total_act_energy": 2000}, T0, ["67fgh"])

    assert result["67fgh"]["values"][0]["value"] == pytest.approx(2.0)


def test_grid_meter_interval_reading_does_not_store_a_baseline_unit_test(adapter):
    """Verifies that a per-interval parameter never stores a lifetime baseline."""
    call_grid_meter(adapter, {"total_act_energy": 100}, T0, ["67fgh"])

    assert "67fgh" not in adapter._last_parameter_values


def test_grid_meter_treats_parameter_without_accumulation_as_interval_unit_test(mock_logger):
    """Verifies that a parameter with no 'shelly_accumulation' is read as a per-interval value."""
    spec = param_spec("energy_in_total")
    del spec["shelly_accumulation"]
    custom_adapter = build_adapter(mock_logger, {"p1": spec})

    result = call_grid_meter(custom_adapter, {"total_act_energy": 3.0}, T0)

    assert result["p1"]["values"][0]["value"] == 3.0


# ============================================================================
# 5. GRID METER UNIT TESTS — LIFETIME ACCUMULATION (_grid_meter)
# ============================================================================

def test_grid_meter_lifetime_first_reading_is_skipped_and_stored_unit_test(adapter):
    """Verifies that the first lifetime reading produces nothing, and is stored as the baseline."""
    result = call_grid_meter(adapter, {"a_total_act_energy": 4961.5}, T0, ["32df"])

    assert result == {}
    assert adapter._last_parameter_values["32df"] == {"value": 4961.5, "ts": T0}


def test_grid_meter_lifetime_reports_the_delta_between_readings_unit_test(adapter):
    """Verifies that the second lifetime reading reports the difference to the previous one."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])

    result = call_grid_meter(adapter, {"a_total_act_energy": 100.5}, T0 + datetime.timedelta(seconds=60), ["32df"])

    assert result["32df"]["parameter_type_id"] == "energy_in_l1"
    assert result["32df"]["values"][0]["value"] == pytest.approx(0.5)
    assert result["32df"]["values"][0]["timestamp"] == "2026-08-19 10:01:00 +0000"


def test_grid_meter_lifetime_updates_the_baseline_on_every_reading_unit_test(adapter):
    """Verifies that each lifetime reading becomes the baseline for the next one."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])
    t1 = T0 + datetime.timedelta(seconds=60)
    call_grid_meter(adapter, {"a_total_act_energy": 101.0}, t1, ["32df"])
    t2 = t1 + datetime.timedelta(seconds=60)

    result = call_grid_meter(adapter, {"a_total_act_energy": 101.25}, t2, ["32df"])

    assert adapter._last_parameter_values["32df"] == {"value": 101.25, "ts": t2}
    assert result["32df"]["values"][0]["value"] == pytest.approx(0.25)


def test_grid_meter_lifetime_keeps_a_zero_delta_unit_test(adapter):
    """Verifies that a lifetime counter that did not move produces a delta of zero, not a skipped reading."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])

    result = call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0 + datetime.timedelta(seconds=60), ["32df"])

    assert result["32df"]["values"][0]["value"] == 0


def test_grid_meter_lifetime_accepts_a_reading_exactly_at_the_age_limit_unit_test(adapter):
    """Verifies that a previous value exactly at the age limit is still used."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])

    result = call_grid_meter(adapter, {"a_total_act_energy": 100.5}, T0 + datetime.timedelta(seconds=120), ["32df"])

    assert result["32df"]["values"][0]["value"] == pytest.approx(0.5)


def test_grid_meter_lifetime_skips_when_previous_value_is_too_old_unit_test(adapter):
    """Verifies that a previous value older than the age limit is not used, and the current
    value becomes the new baseline instead."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])
    t1 = T0 + datetime.timedelta(seconds=121)

    result = call_grid_meter(adapter, {"a_total_act_energy": 150.0}, t1, ["32df"])

    assert result == {}
    assert adapter._last_parameter_values["32df"] == {"value": 150.0, "ts": t1}


def test_grid_meter_lifetime_recovers_after_a_stale_baseline_unit_test(adapter):
    """Verifies that readings are reported again once a fresh baseline exists after a long gap,
    and that the gap itself never produces an inflated delta."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])
    t1 = T0 + datetime.timedelta(minutes=5)
    stale = call_grid_meter(adapter, {"a_total_act_energy": 150.0}, t1, ["32df"])

    recovered = call_grid_meter(adapter, {"a_total_act_energy": 150.5}, t1 + datetime.timedelta(seconds=60), ["32df"])

    assert stale == {}
    assert recovered["32df"]["values"][0]["value"] == pytest.approx(0.5)


def test_grid_meter_lifetime_skips_negative_delta_unit_test(adapter):
    """Verifies that a counter that went backwards, e.g. after the device restarted, produces
    no reading, and the new value becomes the baseline."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])
    t1 = T0 + datetime.timedelta(seconds=60)

    result = call_grid_meter(adapter, {"a_total_act_energy": 50.0}, t1, ["32df"])

    assert result == {}
    assert adapter._last_parameter_values["32df"] == {"value": 50.0, "ts": t1}


def test_grid_meter_lifetime_recovers_after_a_counter_reset_unit_test(adapter):
    """Verifies that readings are reported again right after a counter reset, measured
    against the new baseline."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])
    t1 = T0 + datetime.timedelta(seconds=60)
    call_grid_meter(adapter, {"a_total_act_energy": 50.0}, t1, ["32df"])

    result = call_grid_meter(adapter, {"a_total_act_energy": 50.2}, t1 + datetime.timedelta(seconds=60), ["32df"])

    assert result["32df"]["values"][0]["value"] == pytest.approx(0.2)


def test_grid_meter_lifetime_non_numeric_reading_keeps_the_previous_baseline_unit_test(adapter):
    """Verifies that an invalid lifetime reading does not overwrite the stored baseline."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0}, T0, ["32df"])

    skipped = call_grid_meter(adapter, {"a_total_act_energy": "abc"}, T0 + datetime.timedelta(seconds=60), ["32df"])
    result = call_grid_meter(adapter, {"a_total_act_energy": 100.5}, T0 + datetime.timedelta(seconds=90), ["32df"])

    assert skipped == {}
    assert adapter._last_parameter_values["32df"]["ts"] == T0 + datetime.timedelta(seconds=90)
    assert result["32df"]["values"][0]["value"] == pytest.approx(0.5)


def test_grid_meter_lifetime_non_numeric_first_reading_stores_no_baseline_unit_test(adapter):
    """Verifies that an invalid first lifetime reading does not create a baseline."""
    call_grid_meter(adapter, {"a_total_act_energy": "abc"}, T0, ["32df"])

    assert "32df" not in adapter._last_parameter_values


def test_grid_meter_lifetime_converts_the_delta_not_the_baseline_unit_test(mock_logger):
    """Verifies that the delta is computed in the device's unit and then converted to kWh,
    and that the stored baseline stays in the device's unit."""
    custom_adapter = build_adapter(
        mock_logger, {"p1": param_spec("energy_in_total", unit="Wh", accumulation="lifetime")}
    )
    call_grid_meter(custom_adapter, {"total_act_energy": 1000}, T0)

    result = call_grid_meter(custom_adapter, {"total_act_energy": 1500}, T0 + datetime.timedelta(seconds=60))

    assert result["p1"]["values"][0]["value"] == pytest.approx(0.5)
    assert custom_adapter._last_parameter_values["p1"]["value"] == 1500


def test_grid_meter_lifetime_does_not_leave_floating_point_noise_unit_test(adapter):
    """Verifies that a delta such as 100.3 - 100.2, which is not exactly 0.1 in floating point,
    is rounded to a clean value."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.2}, T0, ["32df"])

    result = call_grid_meter(adapter, {"a_total_act_energy": 100.3}, T0 + datetime.timedelta(seconds=60), ["32df"])

    assert result["32df"]["values"][0]["value"] == 0.1


def test_grid_meter_lifetime_baselines_are_tracked_per_parameter_unit_test(mock_logger):
    """Verifies that each lifetime parameter keeps its own baseline."""
    custom_adapter = build_adapter(mock_logger, {
        "p_in": param_spec("energy_in_l1", accumulation="lifetime"),
        "p_out": param_spec("energy_out_l1", accumulation="lifetime")
    })
    call_grid_meter(custom_adapter, {"a_total_act_energy": 10.0, "a_total_act_ret_energy": 5.0}, T0)

    result = call_grid_meter(
        custom_adapter,
        {"a_total_act_energy": 11.0, "a_total_act_ret_energy": 5.5},
        T0 + datetime.timedelta(seconds=60)
    )

    assert result["p_in"]["values"][0]["value"] == pytest.approx(1.0)
    assert result["p_out"]["values"][0]["value"] == pytest.approx(0.5)


def test_grid_meter_reports_lifetime_and_interval_parameters_together_unit_test(adapter):
    """Verifies that, once a baseline exists, lifetime and interval parameters are reported in the same call."""
    call_grid_meter(adapter, {"a_total_act_energy": 100.0, "total_act_energy": 10.0}, T0)

    result = call_grid_meter(
        adapter,
        {"a_total_act_energy": 100.5, "total_act_energy": 20.0},
        T0 + datetime.timedelta(seconds=60)
    )

    assert result["32df"]["values"][0]["value"] == pytest.approx(0.5)
    assert result["67fgh"]["values"][0]["value"] == pytest.approx(0.02)


# ============================================================================
# 6. END-TO-END ADAPT() UNIT TESTS
# ============================================================================

def test_adapt_returns_empty_list_for_empty_message_unit_test(adapter):
    """Verifies that an empty or falsy message short-circuits and returns an empty list."""
    assert adapter.adapt({}) == []
    assert adapter.adapt(None) == []


def test_adapt_returns_empty_list_for_unknown_source_unit_test(adapter):
    """Verifies that a message from an unknown device produces no output."""
    assert adapter.adapt(make_shelly_envelope({"total_act_energy": 1}, source="Shelly-unknown/")) == []


def test_adapt_does_not_process_source_without_prefix_unit_test(adapter):
    """Verifies that a message whose source differs from the configured exchange name produces no output."""
    envelope = make_shelly_envelope({"total_act_energy": 45.2}, source="shellyproem50-08f9e0e6a5a4/")

    assert adapter.adapt(envelope) == []


def test_adapt_returns_empty_list_for_unmatched_topic_unit_test(adapter):
    """Verifies that a message whose topic matches no parameter produces no output."""
    envelope = make_shelly_envelope({"total_act_energy": 1}, topic="shellyproem50-08f9e0e6a5a4/status/switch:0")

    assert adapter.adapt(envelope) == []


def test_adapt_returns_empty_list_for_invalid_payload_unit_test(adapter):
    """Verifies that a payload that is not valid JSON produces no output instead of raising."""
    assert adapter.adapt(make_shelly_envelope(payload=b"not json at all")) == []


def test_adapt_returns_empty_list_when_entity_has_no_entity_type_id_unit_test(mock_logger):
    """Verifies that an entity with no 'entity_type_id' configured produces no output, instead
    of being treated as a grid meter."""
    custom_adapter = build_adapter(mock_logger, {"p1": param_spec("energy_in_total")}, entity_type_id=None)

    assert custom_adapter.adapt(make_shelly_envelope({"total_act_energy": 1})) == []


def test_adapt_returns_empty_list_when_no_strategy_is_registered_unit_test(mock_logger):
    """Verifies that an entity type with no registered strategy produces no output."""
    custom_adapter = build_adapter(
        mock_logger, {"p1": param_spec("energy_in_total")}, entity_type_id="unregistered_asset"
    )

    assert custom_adapter.adapt(make_shelly_envelope({"total_act_energy": 1})) == []


def test_adapt_returns_empty_list_when_every_parameter_is_skipped_unit_test(adapter):
    """Verifies that no message is emitted when all parameters are skipped, e.g. the first
    message carrying only a lifetime reading."""
    assert adapter.adapt(make_shelly_envelope({"a_total_act_energy": 100.0})) == []


def test_adapt_first_message_only_reports_the_interval_parameter_unit_test(adapter):
    """Verifies that the first message reports the per-interval parameter, while the lifetime
    parameter waits for a baseline."""
    result = adapter.adapt(make_shelly_envelope({"a_total_act_energy": 4961.5, "total_act_energy": 45.2}))

    parameters = result[0]["parameters"]
    assert list(parameters.keys()) == ["67fgh"]
    assert parameters["67fgh"]["parameter_type_id"] == "energy_in_total"
    assert parameters["67fgh"]["values"][0]["value"] == pytest.approx(0.0452)


def test_adapt_second_message_also_reports_the_lifetime_parameter_unit_test(adapter):
    """Verifies that the second message reports the lifetime parameter as a delta, and the
    per-interval parameter as a plain reading, both in kWh."""
    adapter.adapt(make_shelly_envelope({"a_total_act_energy": 4961.5, "total_act_energy": 45.2}))

    result = adapter.adapt(make_shelly_envelope({"a_total_act_energy": 4961.521, "total_act_energy": 30.0}))

    parameters = result[0]["parameters"]
    assert parameters["32df"]["parameter_type_id"] == "energy_in_l1"
    assert parameters["32df"]["values"][0]["value"] == pytest.approx(0.021)
    assert parameters["67fgh"]["values"][0]["value"] == pytest.approx(0.03)


def test_adapt_produces_canonical_message_unit_test(adapter):
    """Verifies the structure of the canonical entity message."""
    result = adapter.adapt(make_shelly_envelope({"total_act_energy": 45.2}))

    assert len(result) == 1
    message = result[0]
    assert message["id"] == "GR02"
    assert message["entity_type_id"] == "grid_meter"
    assert set(message.keys()) == {"id", "entity_type_id", "parameters", "timestamp"}


def test_adapt_uses_the_same_timestamp_for_message_and_readings_unit_test(adapter):
    """Verifies that the message and its readings share the same timestamp, formatted in UTC."""
    message = adapter.adapt(make_shelly_envelope({"total_act_energy": 45.2}))[0]

    assert message["timestamp"].endswith("+0000")
    assert message["parameters"]["67fgh"]["values"][0]["timestamp"] == message["timestamp"]


def test_adapt_uses_the_configured_timezone_unit_test(offset_adapter):
    """Verifies that timestamps follow the adapter's configured timezone."""
    message = offset_adapter.adapt(make_shelly_envelope({"total_act_energy": 45.2}))[0]

    assert message["timestamp"].endswith("+0300")


def test_adapt_accepts_str_payload_unit_test(adapter):
    """Verifies that a payload that is a string, rather than bytes, is processed."""
    envelope = make_shelly_envelope(
        payload=json.dumps({"Topic": TOPIC, "Message": json.dumps({"total_act_energy": 45.2})})
    )

    assert adapter.adapt(envelope)[0]["id"] == "GR02"


def test_adapt_ignores_unrelated_keys_in_the_shelly_message_unit_test(adapter):
    """Verifies that keys the adapter does not use, such as the Shelly channel id, do not
    appear in the output."""
    result = adapter.adapt(make_shelly_envelope({"id": 0, "total_act_energy": 45.2, "voltage": 230.1}))

    assert list(result[0]["parameters"].keys()) == ["67fgh"]