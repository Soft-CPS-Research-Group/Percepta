# --- Standard Library Imports ---
import datetime
import json

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems import CWTelemetryAdapter, _LABEL_STRATEGIES


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
        component_name="cw_adapter_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def valid_environment_specs():
    """Provides standard Cleanwatts entity schema specifications, covering a
    battery (no dedicated strategy) and an EV charger (dedicated strategy).
    Parameter ids are the Cleanwatts tag ids. The electric_vehicle parameter has
    its own id ("connected_vehicle") and holds the connected vehicle's id in 'vehicle_id'."""
    return {
        "entities": {
            "B01": {
                "entity_type_id": "battery",
                "parameters": {
                    "35927": {
                        "type": "Battery charging energy",
                        "measurement_unit": "kWh",
                        "parameter_type_id": "energy_in"
                    },
                    "33690": {
                        "type": "Battery State of charge",
                        "measurement_unit": "%",
                        "parameter_type_id": "state_of_charge"
                    }
                }
            },
            "EVC01": {
                "entity_type_id": "ev_charger",
                "parameters": {
                    "37507": {
                        "type": "Session status",
                        "measurement_unit": "Status",
                        "parameter_type_id": "session_status"
                    },
                    "41836": {
                        "type": "Power",
                        "measurement_unit": "kW",
                        "parameter_type_id": "power"
                    },
                    "connected_vehicle": {
                        "type": "Name",
                        "parameter_type_id": "electric_vehicle",
                        "vehicle_id": "EV01"
                    }
                }
            }
        }
    }


@pytest.fixture
def valid_source_mapping_rules():
    """Provides parameter-id-to-tag-id mapping rules matching `valid_environment_specs`.
    The electric_vehicle parameter has no tag, as it is never polled."""
    return {
        "35927": "35927",
        "33690": "33690",
        "37507": "37507",
        "41836": "41836"
    }


@pytest.fixture
def adapter(mock_logger, valid_environment_specs, valid_source_mapping_rules):
    """Fixture returning an initialized CWTelemetryAdapter instance, using UTC."""
    return CWTelemetryAdapter(
        environment="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        timezone="UTC"
    )


@pytest.fixture
def lisbon_adapter(mock_logger, valid_environment_specs, valid_source_mapping_rules):
    """Fixture returning an adapter configured with a non-UTC timezone,
    used to verify timezone conversion."""
    return CWTelemetryAdapter(
        environment="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        timezone="Europe/Lisbon"
    )


def make_http_envelope(source: str, payload) -> dict:
    """Builds a minimal canonical HTTP envelope, as produced by PayloadNormalizer.from_http."""
    return {
        "protocol": "HTTP",
        "source": source,
        "status": 200,
        "content_type": "application/json",
        "metadata": {},
        "payload": payload
    }


def tag_url(tag_id: str) -> str:
    """Builds a Cleanwatts request URL for the given tag id."""
    return f"https://api.cleanwattsdigital.com/data/request/Instant?instantType=avg&tags={tag_id}"


def call_session_status(adapter, raw_values, now_dt):
    """Helper invoking the EV charger strategy for a session_status reading."""
    return adapter._ev_charger(
        entity_id="EVC01",
        entity_parameters=adapter._entities["EVC01"]["parameters"],
        param_id="37507",
        parameter_type_id="session_status",
        raw_values=raw_values,
        now_dt=now_dt
    )


# ============================================================================
# 1. INITIALIZATION & SCHEMA PARSING UNIT TESTS
# ============================================================================

def test_init_with_valid_specs_unit_test(mock_logger, valid_environment_specs, valid_source_mapping_rules):
    """Verifies that initialization correctly stores entities and builds the tag lookup table."""
    inst = CWTelemetryAdapter(
        environment="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger
    )

    assert "B01" in inst._entities
    assert "EVC01" in inst._entities
    assert inst._tag_to_param["35927"] == ("B01", "35927")
    assert inst._tag_to_param["33690"] == ("B01", "33690")
    assert inst._tag_to_param["37507"] == ("EVC01", "37507")
    assert inst._tag_to_param["41836"] == ("EVC01", "41836")


def test_init_ignores_mapping_for_parameter_without_entity_unit_test(mock_logger, valid_environment_specs):
    """Verifies that a mapping rule for a parameter that belongs to no entity is not added to the tag lookup."""
    inst = CWTelemetryAdapter(
        environment="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules={"35927": "35927", "99999": "99999"},
        logger=mock_logger
    )

    assert "35927" in inst._tag_to_param
    assert "99999" not in inst._tag_to_param


def test_init_raises_value_error_when_entities_key_missing_unit_test(mock_logger, valid_source_mapping_rules):
    """Verifies that initialization raises ValueError when 'entities' key is absent."""
    with pytest.raises(ValueError, match="entities"):
        CWTelemetryAdapter(
            environment="R-H-01",
            environment_specs={"group": "living lab"},
            source_mapping_rules=valid_source_mapping_rules,
            logger=mock_logger
        )


def test_init_raises_value_error_when_entities_is_empty_unit_test(mock_logger, valid_source_mapping_rules):
    """Verifies that initialization raises ValueError when 'entities' dictionary is empty."""
    with pytest.raises(ValueError, match="cannot be empty"):
        CWTelemetryAdapter(
            environment="R-H-01",
            environment_specs={"entities": {}},
            source_mapping_rules=valid_source_mapping_rules,
            logger=mock_logger
        )


def test_init_raises_type_error_when_source_mapping_rules_not_dict_unit_test(mock_logger, valid_environment_specs):
    """Verifies that initialization raises TypeError when 'source_mapping_rules' is not a dictionary."""
    with pytest.raises(TypeError, match="must be a dictionary"):
        CWTelemetryAdapter(
            environment="R-H-01",
            environment_specs=valid_environment_specs,
            source_mapping_rules=["not", "a", "dict"],
            logger=mock_logger
        )


def test_ev_charger_strategy_is_registered_unit_test():
    """Verifies that the grid meter strategy is registered under its entity type id."""
    assert "ev_charger" in _LABEL_STRATEGIES


# ============================================================================
# 2. URL RESOLUTION UNIT TESTS (_resolve_param_from_url)
# ============================================================================

def test_resolve_param_from_url_success_unit_test(adapter):
    """Verifies correct resolution of (entity_id, param_id) from a URL with a known tag."""
    entity_id, param_id = adapter._resolve_param_from_url(tag_url("33690") + "&from=...&to=...")

    assert entity_id == "B01"
    assert param_id == "33690"


def test_resolve_param_from_url_unmatched_tag_unit_test(adapter):
    """Verifies that a URL with an unrecognized tag id returns (None, None)."""
    assert adapter._resolve_param_from_url(tag_url("00000")) == (None, None)


def test_resolve_param_from_url_no_tag_in_url_unit_test(adapter):
    """Verifies that a URL without a 'tags=' query parameter returns (None, None)."""
    url = "https://api.cleanwattsdigital.com/data/request/Instant?instantType=avg"

    assert adapter._resolve_param_from_url(url) == (None, None)


def test_resolve_param_from_url_empty_url_unit_test(adapter):
    """Verifies that an empty URL returns (None, None) without raising."""
    assert adapter._resolve_param_from_url("") == (None, None)


# ============================================================================
# 3. INGRESS NORMALIZATION UNIT TESTS (_normalize_ingress_payload)
# ============================================================================

def test_normalize_ingress_payload_resolves_entity_and_parameter_unit_test(adapter):
    """Verifies that a valid envelope is resolved into its entity, parameter, and raw values."""
    raw_values = [{"Date": "2026-08-19T10:00:00Z", "Read": 12.5}]
    envelope = make_http_envelope(tag_url("35927"), raw_values)

    resolved = adapter._normalize_ingress_payload(envelope)

    assert resolved["id"] == "B01"
    assert resolved["entity_type_id"] == "battery"
    assert resolved["param_id"] == "35927"
    assert resolved["parameter_type_id"] == "energy_in"
    assert resolved["values"] == raw_values


def test_normalize_ingress_payload_parses_json_string_payload_unit_test(adapter):
    """Verifies that a serialized JSON string payload is deserialized into a list."""
    raw_values = [{"Date": "2026-08-19T10:00:00Z", "Read": 85.0}]
    envelope = make_http_envelope(tag_url("33690"), json.dumps(raw_values))

    assert adapter._normalize_ingress_payload(envelope)["values"] == raw_values


def test_normalize_ingress_payload_parses_bytes_payload_unit_test(adapter):
    """Verifies that a bytes-encoded JSON payload is deserialized into a list."""
    raw_values = [{"Date": "2026-08-19T10:00:00Z", "Read": 85.0}]
    envelope = make_http_envelope(tag_url("33690"), json.dumps(raw_values).encode("utf-8"))

    assert adapter._normalize_ingress_payload(envelope)["values"] == raw_values


def test_normalize_ingress_payload_returns_none_for_unresolvable_tag_unit_test(adapter):
    """Verifies that an envelope whose URL tag cannot be resolved returns None."""
    envelope = make_http_envelope(tag_url("00000"), [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}])

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_returns_none_for_non_dict_message_unit_test(adapter):
    """Verifies that a non-dictionary message is rejected gracefully."""
    assert adapter._normalize_ingress_payload("not a dict") is None


def test_normalize_ingress_payload_returns_none_for_error_status_unit_test(adapter):
    """Verifies that an envelope with a non-2xx status is discarded."""
    envelope = make_http_envelope(tag_url("35927"), "Network Error: timeout")
    envelope["status"] = 503

    assert adapter._normalize_ingress_payload(envelope) is None


def test_normalize_ingress_payload_accepts_envelope_without_status_unit_test(adapter):
    """Verifies that an envelope with no status at all is not treated as an error."""
    envelope = make_http_envelope(tag_url("35927"), [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}])
    del envelope["status"]

    assert adapter._normalize_ingress_payload(envelope) is not None


def test_normalize_ingress_payload_returns_none_for_invalid_json_payload_unit_test(adapter):
    """Verifies that a payload that is not valid JSON is discarded instead of raising."""
    envelope = make_http_envelope(tag_url("35927"), "not json at all")

    assert adapter._normalize_ingress_payload(envelope) is None


# ============================================================================
# 4. DATE PARSING UNIT TESTS (_parse_date)
# ============================================================================

def test_parse_date_returns_utc_for_naive_date_unit_test(adapter):
    """Verifies that a date with no timezone is assumed to be UTC."""
    assert adapter._parse_date("2026-08-19 10:00:00") == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_parse_date_accepts_z_suffix_unit_test(adapter):
    """Verifies that a 'Z' suffix is read as UTC."""
    assert adapter._parse_date("2026-08-19T10:00:00Z") == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_parse_date_respects_explicit_offset_unit_test(adapter):
    """Verifies that a date carrying its own offset is not treated as UTC."""
    parsed = adapter._parse_date("2026-08-19T11:00:00+01:00")

    assert parsed == datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)


def test_parse_date_returns_none_for_invalid_date_unit_test(adapter):
    """Verifies that an invalid date returns None instead of raising."""
    assert adapter._parse_date("not-a-real-date") is None
    assert adapter._parse_date(12345) is None


# ============================================================================
# 5. VALUE NORMALIZATION UNIT TESTS (_build_values_list)
# ============================================================================

def test_build_values_list_extracts_read_and_formats_timestamp_unit_test(adapter):
    """Verifies that 'Read' values and ISO 'Z' timestamps are converted and formatted in UTC."""
    result = adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z", "Read": 22.4}])

    assert result == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 22.4}]


def test_build_values_list_extracts_value_key_when_read_absent_unit_test(adapter):
    """Verifies that readings using the 'Value' key are correctly extracted."""
    result = adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z", "Value": 85.0}])

    assert result[0]["value"] == 85.0


def test_build_values_list_falls_back_to_read_when_value_is_null_unit_test(adapter):
    """Verifies that a null 'Value' does not hide a valid 'Read' in the same reading."""
    result = adapter._build_values_list(
        [{"Date": "2026-08-19T10:00:00Z", "Value": None, "Read": 12.5}]
    )

    assert result[0]["value"] == 12.5


def test_build_values_list_keeps_zero_value_unit_test(adapter):
    """Verifies that a reading with a value of zero is a valid reading and is kept."""
    result = adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z", "Read": 0}])

    assert len(result) == 1
    assert result[0]["value"] == 0


def test_build_values_list_discards_reading_without_value_unit_test(adapter):
    """Verifies that a reading with neither 'Value' nor 'Read' is discarded, not turned into zero."""
    assert adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z"}]) == []


def test_build_values_list_discards_reading_with_null_value_unit_test(adapter):
    """Verifies that a reading with a null value is discarded."""
    assert adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z", "Read": None}]) == []


def test_build_values_list_treats_naive_date_as_utc_unit_test(adapter):
    """Verifies that a timezone-naive date string is assumed to be UTC."""
    result = adapter._build_values_list([{"Date": "2026-08-19 10:00:00", "Read": 1.0}])

    assert result[0]["timestamp"] == "2026-08-19 10:00:00 +0000"


def test_build_values_list_converts_date_with_offset_unit_test(adapter):
    """Verifies that a reading date carrying an offset is converted to the adapter's timezone."""
    result = adapter._build_values_list([{"Date": "2026-08-19T11:00:00+01:00", "Read": 1.0}])

    assert result[0]["timestamp"] == "2026-08-19 10:00:00 +0000"


def test_build_values_list_converts_to_configured_timezone_unit_test(lisbon_adapter):
    """Verifies that readings are converted to the adapter's configured timezone."""
    result = lisbon_adapter._build_values_list([{"Date": "2026-08-19T10:00:00Z", "Read": 22.4}])

    assert result[0]["timestamp"] == "2026-08-19 11:00:00 +0100"


def test_build_values_list_treats_naive_date_as_utc_before_converting_unit_test(lisbon_adapter):
    """Verifies that a naive date string is assumed to be UTC and then converted to the configured timezone."""
    result = lisbon_adapter._build_values_list([{"Date": "2026-08-19 10:00:00", "Read": 1.0}])

    assert result[0]["timestamp"] == "2026-08-19 11:00:00 +0100"


def test_build_values_list_reads_date_from_date_utc_key_unit_test(adapter):
    """Verifies that a reading's date is also read from 'DateUTC' when 'Date' is absent."""
    result = adapter._build_values_list([{"DateUTC": "2026-08-19 10:00:00", "Read": 1.0}])

    assert result == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 1.0}]


def test_build_values_list_falls_back_to_date_utc_when_date_is_null_unit_test(adapter):
    """Verifies that a null 'Date' does not hide a valid 'DateUTC' in the same reading."""
    result = adapter._build_values_list(
        [{"Date": None, "DateUTC": "2026-08-19 10:00:00", "Read": 1.0}]
    )

    assert len(result) == 1
    assert result[0]["timestamp"] == "2026-08-19 10:00:00 +0000"


def test_build_values_list_prefers_date_over_date_utc_unit_test(adapter):
    """Verifies that 'Date' takes precedence over 'DateUTC' when both are present."""
    result = adapter._build_values_list(
        [{"Date": "2026-08-19 10:00:00", "DateUTC": "2026-08-19 09:00:00", "Read": 1.0}]
    )

    assert result[0]["timestamp"] == "2026-08-19 10:00:00 +0000"


def test_build_values_list_skips_non_dict_items_unit_test(adapter):
    """Verifies that non-dictionary items in the input list are silently skipped."""
    raw = [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}, "not_a_dict", 42]

    assert len(adapter._build_values_list(raw)) == 1


def test_build_values_list_skips_items_without_date_unit_test(adapter):
    """Verifies that readings with neither 'Date' nor 'DateUTC' are excluded from the result."""
    assert adapter._build_values_list([{"Read": 1.0}]) == []


def test_build_values_list_discards_reading_with_empty_date_unit_test(adapter):
    """Verifies that a reading whose date is an empty string is discarded."""
    assert adapter._build_values_list([{"Date": "", "Read": 1.0}]) == []


def test_build_values_list_discards_reading_with_unparseable_date_unit_test(adapter):
    """Verifies that a reading with a malformed date is discarded rather than kept with an invalid timestamp."""
    assert adapter._build_values_list([{"Date": "not-a-real-date", "Read": 1.0}]) == []


def test_build_values_list_keeps_valid_readings_when_one_date_is_invalid_unit_test(adapter):
    """Verifies that an invalid date only discards its own reading, not the valid ones around it."""
    raw = [
        {"Date": "2026-08-19T10:00:00Z", "Read": 1.0},
        {"Date": "not-a-real-date", "Read": 2.0},
        {"Date": "2026-08-19T10:05:00Z", "Read": 3.0},
    ]

    result = adapter._build_values_list(raw)

    assert [item["value"] for item in result] == [1.0, 3.0]


def test_build_values_list_returns_empty_list_for_empty_input_unit_test(adapter):
    """Verifies that an empty or falsy input list returns an empty result list."""
    assert adapter._build_values_list([]) == []
    assert adapter._build_values_list(None) == []


# ============================================================================
# 6. PARAMETER BUILDING UNIT TESTS (_build_parameter)
# ============================================================================

def test_build_parameter_with_valid_readings_unit_test(adapter):
    """Verifies that valid readings are normalized into the parameter entry."""
    entry = adapter._build_parameter(
        "B01", "35927", "energy_in", [{"Date": "2026-08-19T10:00:00Z", "Read": 12.5}]
    )

    assert entry == {
        "parameter_type_id": "energy_in",
        "values": [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 12.5}]
    }


def test_build_parameter_with_no_readings_returns_empty_values_unit_test(adapter):
    """Verifies that a parameter with no readings gets an empty list, to be reported as empty by validation."""
    assert adapter._build_parameter("B01", "35927", "energy_in", []) == {
        "parameter_type_id": "energy_in",
        "values": []
    }


def test_build_parameter_with_null_readings_returns_empty_values_unit_test(adapter):
    """Verifies that a null payload is treated as having no readings."""
    assert adapter._build_parameter("B01", "35927", "energy_in", None)["values"] == []


def test_build_parameter_flags_malformed_when_readings_are_not_a_list_unit_test(adapter):
    """Verifies that readings that are not a list, e.g. an unexpected error body, are flagged
    as malformed rather than reported as empty."""
    entry = adapter._build_parameter("B01", "35927", "energy_in", {"error": "unexpected"})

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter readings malformed"


def test_build_parameter_flags_malformed_when_readings_are_an_empty_dict_unit_test(adapter):
    """Verifies that an empty dictionary is malformed, and not mistaken for an empty list of readings."""
    entry = adapter._build_parameter("B01", "35927", "energy_in", {})

    assert entry["metadata"] == "Parameter readings malformed"


def test_build_parameter_flags_malformed_when_readings_are_not_dicts_unit_test(adapter):
    """Verifies that a list whose items are not readings is flagged as malformed."""
    entry = adapter._build_parameter("B01", "35927", "energy_in", [1, 2, 3])

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter readings malformed"


def test_build_parameter_flags_malformed_when_all_readings_are_unusable_unit_test(adapter):
    """Verifies that readings which are all discarded are flagged as malformed, not as empty."""
    entry = adapter._build_parameter(
        "B01", "35927", "energy_in", [{"Date": "not-a-real-date", "Read": 12.5}]
    )

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter readings malformed"


# ============================================================================
# 7. END-TO-END ADAPT() UNIT TESTS — GENERIC (NO STRATEGY) PATH
# ============================================================================

def test_adapt_returns_empty_list_for_empty_message_unit_test(adapter):
    """Verifies that an empty or falsy message short-circuits and returns an empty list."""
    assert adapter.adapt({}) == []
    assert adapter.adapt(None) == []


def test_adapt_returns_empty_list_for_unresolvable_tag_unit_test(adapter):
    """Verifies that a message whose tag cannot be resolved produces no output."""
    envelope = make_http_envelope(tag_url("00000"), [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}])

    assert adapter.adapt(envelope) == []


def test_adapt_returns_empty_list_for_error_status_unit_test(adapter):
    """Verifies that an envelope reporting a request failure produces no output instead of raising."""
    envelope = make_http_envelope(tag_url("35927"), "Network Error: timeout")
    envelope["status"] = 503

    assert adapter.adapt(envelope) == []


def test_adapt_returns_empty_list_for_invalid_json_payload_unit_test(adapter):
    """Verifies that a payload that is not valid JSON produces no output instead of raising."""
    envelope = make_http_envelope(tag_url("35927"), "not json at all")

    assert adapter.adapt(envelope) == []


def test_adapt_returns_empty_list_when_entity_has_no_entity_type_id_unit_test(mock_logger):
    """Verifies that an entity with no 'entity_type_id' configured produces no output."""
    specs = {
        "entities": {
            "X02": {
                "parameters": {"66666": {"parameter_type_id": "power"}}
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-05",
        environment_specs=specs,
        source_mapping_rules={"66666": "66666"},
        logger=mock_logger
    )
    envelope = make_http_envelope(tag_url("66666"), [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}])

    assert custom_adapter.adapt(envelope) == []


def test_adapt_generic_parameter_produces_canonical_message_unit_test(adapter):
    """Verifies that a battery reading (no dedicated strategy) is normalized and wrapped
    into the canonical entity message format."""
    envelope = make_http_envelope(tag_url("35927"), [{"Date": "2026-08-19T10:00:00Z", "Read": 12.5}])

    result = adapter.adapt(envelope)

    assert len(result) == 1
    message = result[0]
    assert message["id"] == "B01"
    assert message["entity_type_id"] == "battery"
    assert message["parameters"]["35927"]["parameter_type_id"] == "energy_in"
    assert message["parameters"]["35927"]["values"] == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 12.5}]
    assert message["timestamp"].endswith("+0000")


def test_adapt_only_includes_the_received_parameter_unit_test(adapter):
    """Verifies that the output only contains the parameter that was received,
    without flagging the entity's other parameters as missing."""
    envelope = make_http_envelope(tag_url("35927"), [{"Date": "2026-08-19T10:00:00Z", "Read": 12.5}])

    parameters = adapter.adapt(envelope)[0]["parameters"]

    assert list(parameters.keys()) == ["35927"]


def test_adapt_flags_empty_payload_as_empty_unit_test(adapter):
    """Verifies that a parameter that arrives with no readings at all is reported as empty."""
    envelope = make_http_envelope(tag_url("35927"), [])

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter sent empty by receiver"


def test_adapt_flags_payload_that_is_not_a_list_as_malformed_unit_test(adapter):
    """Verifies that a payload with an unexpected structure is flagged as malformed, not as empty."""
    envelope = make_http_envelope(tag_url("35927"), json.dumps({"error": "unexpected"}))

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter readings malformed"


def test_adapt_flags_parameter_as_malformed_when_all_dates_are_invalid_unit_test(adapter):
    """Verifies that a parameter whose readings all have invalid dates is flagged as malformed,
    rather than reported as empty or as having an invalid value."""
    envelope = make_http_envelope(tag_url("35927"), [{"Date": "not-a-real-date", "Read": 12.5}])

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter readings malformed"


def test_adapt_keeps_valid_readings_when_only_some_dates_are_invalid_unit_test(adapter):
    """Verifies that readings with invalid dates are discarded individually, and the valid ones are kept."""
    envelope = make_http_envelope(tag_url("35927"), [
        {"Date": "2026-08-19T10:00:00Z", "Read": 1.0},
        {"Date": "not-a-real-date", "Read": 2.0},
    ])

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert [item["value"] for item in entry["values"]] == [1.0]


def test_adapt_accepts_readings_dated_with_date_utc_unit_test(adapter):
    """Verifies that a reading dated with 'DateUTC' instead of 'Date' goes through the whole flow."""
    envelope = make_http_envelope(tag_url("35927"), [{"DateUTC": "2026-08-19 10:00:00", "Read": 12.5}])

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert entry["values"] == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 12.5}]


def test_adapt_flags_value_of_wrong_type_as_invalid_unit_test(adapter):
    """Verifies that a reading whose value does not match the parameter type's expected
    type (a number) is flagged as invalid."""
    envelope = make_http_envelope(tag_url("35927"), [{"Date": "2026-08-19T10:00:00Z", "Read": "abc"}])

    entry = adapter.adapt(envelope)[0]["parameters"]["35927"]

    assert entry["values"] is None
    assert entry["metadata"] == "Parameter value invalid"


def test_adapt_keeps_zero_value_reading_unit_test(adapter):
    """Verifies that a zero reading, such as an idle power, goes through the whole flow as a valid value."""
    envelope = make_http_envelope(tag_url("41836"), [{"Date": "2026-08-19T10:00:00Z", "Read": 0}])

    entry = adapter.adapt(envelope)[0]["parameters"]["41836"]

    assert entry["values"] == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 0}]


def test_adapt_skips_type_validation_for_unregistered_parameter_type_unit_test(mock_logger):
    """Verifies that a parameter whose type id has no registered expected type is not
    flagged, and keeps its readings as they are."""
    specs = {
        "entities": {
            "X01": {
                "entity_type_id": "custom_asset",
                "parameters": {
                    "55555": {"parameter_type_id": "unknown_type"}
                }
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-03",
        environment_specs=specs,
        source_mapping_rules={"55555": "55555"},
        logger=mock_logger
    )
    envelope = make_http_envelope(tag_url("55555"), [{"Date": "2026-08-19T10:00:00Z", "Read": 1.0}])

    entry = custom_adapter.adapt(envelope)[0]["parameters"]["55555"]

    assert entry["values"] == [{"timestamp": "2026-08-19 10:00:00 +0000", "value": 1.0}]
    assert "metadata" not in entry


def test_adapt_does_not_check_value_type_for_unregistered_parameter_type_unit_test(mock_logger):
    """Verifies that, for an unregistered type, no type check is made, so even a
    non-numeric value is kept."""
    specs = {
        "entities": {
            "X01": {
                "entity_type_id": "custom_asset",
                "parameters": {
                    "55555": {"parameter_type_id": "unknown_type"}
                }
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-03",
        environment_specs=specs,
        source_mapping_rules={"55555": "55555"},
        logger=mock_logger
    )
    envelope = make_http_envelope(tag_url("55555"), [{"Date": "2026-08-19T10:00:00Z", "Read": "abc"}])

    entry = custom_adapter.adapt(envelope)[0]["parameters"]["55555"]

    assert entry["values"][0]["value"] == "abc"


# ============================================================================
# 8. END-TO-END ADAPT() UNIT TESTS — EV CHARGER STRATEGY DISPATCH
# ============================================================================

def test_adapt_ev_charger_power_reading_is_normalized_unit_test(adapter):
    """Verifies that a 'power' reading for an EV charger is normalized like a regular parameter."""
    envelope = make_http_envelope(tag_url("41836"), [{"Date": "2026-08-19T10:00:00Z", "Read": 3.7}])

    message = adapter.adapt(envelope)[0]

    assert message["id"] == "EVC01"
    assert message["entity_type_id"] == "ev_charger"
    assert message["parameters"]["41836"]["parameter_type_id"] == "power"
    assert message["parameters"]["41836"]["values"][0]["value"] == 3.7


def test_adapt_ev_charger_power_reading_only_includes_power_unit_test(adapter):
    """Verifies that a power reading produces only the power parameter, without
    session_status or electric_vehicle."""
    envelope = make_http_envelope(tag_url("41836"), [{"Date": "2026-08-19T10:00:00Z", "Read": 3.7}])

    parameters = adapter.adapt(envelope)[0]["parameters"]

    assert list(parameters.keys()) == ["41836"]


def test_adapt_ev_charger_active_session_reports_vehicle_id_unit_test(adapter):
    """Verifies that an active session_status reading (Read=1) reports the configured
    vehicle's id as the value of the connected vehicle parameter, and survives validation."""
    envelope = make_http_envelope(tag_url("37507"), [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}])

    entry = adapter.adapt(envelope)[0]["parameters"]["connected_vehicle"]

    assert entry["parameter_type_id"] == "electric_vehicle"
    assert entry["values"][0]["value"] == "EV01"


def test_adapt_ev_charger_inactive_session_reports_empty_vehicle_id_unit_test(adapter):
    """Verifies that an explicit inactive session_status reading (Read=0) reports an
    empty string for the electric_vehicle parameter, as a reading of its own: the
    values list is not empty, it holds one reading whose value is an empty string."""
    envelope = make_http_envelope(tag_url("37507"), [{"Read": 0, "DateUTC": "2026-08-19 10:00:00"}])

    entry = adapter.adapt(envelope)[0]["parameters"]["connected_vehicle"]

    assert len(entry["values"]) == 1
    assert entry["values"][0]["value"] == ""


def test_adapt_ev_charger_vehicle_value_depends_on_session_state_unit_test(adapter):
    """Verifies that the electric_vehicle value follows the session state: it carries the
    vehicle's id while active, and is empty when not."""
    active = make_http_envelope(tag_url("37507"), [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}])
    inactive = make_http_envelope(tag_url("37507"), [{"Read": 0, "DateUTC": "2026-08-19 10:05:00"}])

    active_value = adapter.adapt(active)[0]["parameters"]["connected_vehicle"]["values"][0]["value"]
    inactive_value = adapter.adapt(inactive)[0]["parameters"]["connected_vehicle"]["values"][0]["value"]

    assert (active_value, inactive_value) == ("EV01", "")


def test_adapt_ev_charger_session_status_is_not_part_of_output_unit_test(adapter):
    """Verifies that session_status is consumed as a trigger and never appears in the output."""
    envelope = make_http_envelope(tag_url("37507"), [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}])

    assert list(adapter.adapt(envelope)[0]["parameters"].keys()) == ["connected_vehicle"]


def test_adapt_ev_charger_empty_session_status_payload_reports_no_vehicle_without_session_unit_test(adapter):
    """Verifies that an empty session_status payload, with no previously known session,
    still produces a message, reporting that no vehicle is connected."""
    envelope = make_http_envelope(tag_url("37507"), [])

    entry = adapter.adapt(envelope)[0]["parameters"]["connected_vehicle"]

    assert entry["values"][0]["value"] == ""


def test_adapt_ev_charger_empty_session_status_payload_keeps_recent_session_active_unit_test(adapter):
    """Verifies that an empty session_status payload keeps a recently active session active."""
    adapter._last_session_status["EVC01"] = datetime.datetime.now(adapter._tz) - datetime.timedelta(minutes=2)
    envelope = make_http_envelope(tag_url("37507"), [])

    entry = adapter.adapt(envelope)[0]["parameters"]["connected_vehicle"]

    assert entry["values"][0]["value"] == "EV01"


def test_adapt_returns_empty_list_when_strategy_produces_no_parameters_unit_test(mock_logger):
    """Verifies that no message is emitted when the strategy produces no parameters, e.g. a
    session_status reading for a charger with no electric_vehicle parameter configured."""
    specs_without_ev = {
        "entities": {
            "EVC02": {
                "entity_type_id": "ev_charger",
                "parameters": {
                    "99001": {"parameter_type_id": "session_status"}
                }
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-02",
        environment_specs=specs_without_ev,
        source_mapping_rules={"99001": "99001"},
        logger=mock_logger
    )
    envelope = make_http_envelope(tag_url("99001"), [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}])

    assert custom_adapter.adapt(envelope) == []


# ============================================================================
# 9. EV CHARGER SESSION STATE UNIT TESTS (_ev_charger)
# ============================================================================

def test_ev_charger_active_session_stores_last_status_timestamp_unit_test(adapter):
    """Verifies that an active session reading records the parsed heartbeat date,
    as a timezone-aware datetime, as the entity's last known active session time."""
    now_dt = datetime.datetime.now(adapter._tz)

    call_session_status(adapter, [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}], now_dt)

    assert adapter._last_session_status["EVC01"] == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_ev_charger_active_session_reads_date_from_date_key_unit_test(adapter):
    """Verifies that the session heartbeat's date is also read from 'Date' when 'DateUTC' is absent."""
    now_dt = datetime.datetime.now(adapter._tz)

    call_session_status(adapter, [{"Read": 1, "Date": "2026-08-19T10:00:00Z"}], now_dt)

    assert adapter._last_session_status["EVC01"] == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_ev_charger_active_session_prefers_date_utc_over_date_unit_test(adapter):
    """Verifies that 'DateUTC' takes precedence when both date keys are present in a heartbeat."""
    now_dt = datetime.datetime.now(adapter._tz)

    call_session_status(
        adapter,
        [{"Read": 1, "DateUTC": "2026-08-19 10:00:00", "Date": "2026-08-19 09:00:00"}],
        now_dt
    )

    assert adapter._last_session_status["EVC01"] == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_ev_charger_active_session_respects_timezone_in_date_unit_test(adapter):
    """Verifies that a date that carries its own timezone is converted, not assumed to be UTC."""
    now_dt = datetime.datetime.now(adapter._tz)

    call_session_status(adapter, [{"Read": 1, "Date": "2026-08-19T11:00:00+01:00"}], now_dt)

    assert adapter._last_session_status["EVC01"] == datetime.datetime(
        2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc
    )


def test_ev_charger_reads_status_from_value_key_unit_test(adapter):
    """Verifies that the session status is also read from the 'Value' key when 'Read' is absent."""
    now_dt = datetime.datetime.now(adapter._tz)

    result = call_session_status(adapter, [{"Value": 1, "DateUTC": "2026-08-19 10:00:00"}], now_dt)

    assert result["connected_vehicle"]["values"][0]["value"] == "EV01"


def test_ev_charger_heartbeat_without_status_within_grace_period_stays_active_unit_test(adapter):
    """Verifies that a dated heartbeat carrying no status is treated as an active session
    if the last known active status is within the grace period."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(
        adapter, [{"DateUTC": "2026-08-19 10:10:00"}], base_time + datetime.timedelta(minutes=10)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == "EV01"


def test_ev_charger_heartbeat_without_status_beyond_grace_period_becomes_inactive_unit_test(adapter):
    """Verifies that a dated heartbeat carrying no status, beyond the grace period, is
    treated as inactive, and clears the stored last status."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(
        adapter, [{"DateUTC": "2026-08-19 10:20:00"}], base_time + datetime.timedelta(minutes=20)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == ""
    assert "EVC01" not in adapter._last_session_status


def test_ev_charger_heartbeat_without_status_and_without_previous_session_is_inactive_unit_test(adapter):
    """Verifies that a dated heartbeat carrying no status, with no previously known session,
    is treated as inactive."""
    result = call_session_status(
        adapter, [{"DateUTC": "2026-08-19 10:00:00"}], datetime.datetime.now(adapter._tz)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == ""


def test_ev_charger_empty_payload_within_grace_period_stays_active_unit_test(adapter):
    """Verifies that a session_status payload with no usable reading (empty, null, or not a list)
    keeps the session active while the last active heartbeat is within the grace period."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time
    later_dt = base_time + datetime.timedelta(minutes=10)

    for payload in ([], None, {"error": "unexpected"}):
        result = call_session_status(adapter, payload, later_dt)
        assert result["connected_vehicle"]["values"][0]["value"] == "EV01"


def test_ev_charger_empty_payload_beyond_grace_period_becomes_inactive_unit_test(adapter):
    """Verifies that a session_status payload with no usable reading, beyond the grace period,
    ends the session and clears the stored last status."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(adapter, [], base_time + datetime.timedelta(minutes=20))

    assert result["connected_vehicle"]["values"][0]["value"] == ""
    assert "EVC01" not in adapter._last_session_status


def test_ev_charger_empty_payload_without_previous_session_is_inactive_unit_test(adapter):
    """Verifies that a session_status payload with no usable reading and no known session is inactive."""
    result = call_session_status(adapter, [], datetime.datetime.now(adapter._tz))

    assert result["connected_vehicle"]["values"][0]["value"] == ""


def test_ev_charger_active_heartbeat_without_date_uses_grace_period_unit_test(adapter):
    """Verifies that an active heartbeat with no date is not recorded, and counts as no usable
    reading: a recently active session stays active, without its last date being overwritten."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(adapter, [{"Read": 1}], base_time + datetime.timedelta(minutes=5))

    assert result["connected_vehicle"]["values"][0]["value"] == "EV01"
    assert adapter._last_session_status["EVC01"] == base_time


def test_ev_charger_active_heartbeat_without_date_and_without_previous_session_is_inactive_unit_test(adapter):
    """Verifies that an active heartbeat with no date, with no known session, is inactive and not recorded."""
    result = call_session_status(adapter, [{"Read": 1}], datetime.datetime.now(adapter._tz))

    assert result["connected_vehicle"]["values"][0]["value"] == ""
    assert "EVC01" not in adapter._last_session_status


def test_ev_charger_active_heartbeat_with_invalid_date_beyond_grace_period_is_inactive_unit_test(adapter):
    """Verifies that an active heartbeat with an invalid date, beyond the grace period, ends the session."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(
        adapter, [{"Read": 1, "DateUTC": "not-a-date"}], base_time + datetime.timedelta(minutes=20)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == ""
    assert "EVC01" not in adapter._last_session_status


def test_ev_charger_inactive_reading_does_not_need_a_date_unit_test(adapter):
    """Verifies that an explicit inactive reading ends the session even with no date."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["EVC01"] = base_time

    result = call_session_status(adapter, [{"Read": 0}], base_time + datetime.timedelta(minutes=1))

    assert result["connected_vehicle"]["values"][0]["value"] == ""
    assert "EVC01" not in adapter._last_session_status


def test_ev_charger_explicit_inactive_reading_clears_session_unit_test(adapter):
    """Verifies that an explicit inactive reading ends the session, so a later heartbeat with
    no status, within the grace period, is not treated as still active."""
    base_time = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    call_session_status(adapter, [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}], base_time)
    assert "EVC01" in adapter._last_session_status

    call_session_status(
        adapter, [{"Read": 0, "DateUTC": "2026-08-19 10:01:00"}], base_time + datetime.timedelta(minutes=1)
    )
    assert "EVC01" not in adapter._last_session_status

    result = call_session_status(
        adapter, [{"DateUTC": "2026-08-19 10:02:00"}], base_time + datetime.timedelta(minutes=2)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == ""


def test_ev_charger_heartbeat_without_status_after_parsed_active_reading_does_not_raise_unit_test(adapter):
    """Regression test: an active reading stores a parsed date, and a later heartbeat with no
    status compares against it. Both must be timezone-aware, or the subtraction raises TypeError."""
    first_dt = datetime.datetime(2026, 8, 19, 10, 5, 0, tzinfo=datetime.timezone.utc)
    call_session_status(adapter, [{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}], first_dt)

    result = call_session_status(
        adapter, [{"DateUTC": "2026-08-19 10:10:00"}], first_dt + datetime.timedelta(minutes=5)
    )

    assert result["connected_vehicle"]["values"][0]["value"] == "EV01"


def test_ev_charger_state_is_tracked_per_entity_unit_test(adapter):
    """Verifies that session state recorded for one charger does not affect another."""
    now_dt = datetime.datetime(2026, 8, 19, 10, 0, 0, tzinfo=datetime.timezone.utc)
    adapter._last_session_status["OTHER_CHARGER"] = now_dt

    result = call_session_status(adapter, [{"DateUTC": "2026-08-19 10:00:00"}], now_dt)

    assert result["connected_vehicle"]["values"][0]["value"] == ""


def test_ev_charger_without_configured_vehicle_param_returns_empty_dict_unit_test(mock_logger):
    """Verifies that if the entity has no electric_vehicle parameter configured,
    the strategy logs a warning and returns an empty dictionary."""
    specs_without_ev = {
        "entities": {
            "EVC02": {
                "entity_type_id": "ev_charger",
                "parameters": {
                    "99001": {"parameter_type_id": "session_status"}
                }
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-02",
        environment_specs=specs_without_ev,
        source_mapping_rules={"99001": "99001"},
        logger=mock_logger
    )

    result = custom_adapter._ev_charger(
        entity_id="EVC02",
        entity_parameters=specs_without_ev["entities"]["EVC02"]["parameters"],
        param_id="99001",
        parameter_type_id="session_status",
        raw_values=[{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}],
        now_dt=datetime.datetime.now(custom_adapter._tz)
    )

    assert result == {}


def test_ev_charger_without_configured_vehicle_id_returns_empty_dict_unit_test(mock_logger):
    """Verifies that if the electric_vehicle parameter has no vehicle_id configured,
    the strategy logs a warning and returns an empty dictionary."""
    specs = {
        "entities": {
            "EVC03": {
                "entity_type_id": "ev_charger",
                "parameters": {
                    "88001": {"parameter_type_id": "session_status"},
                    "connected_vehicle": {"parameter_type_id": "electric_vehicle"}
                }
            }
        }
    }
    custom_adapter = CWTelemetryAdapter(
        environment="R-H-04",
        environment_specs=specs,
        source_mapping_rules={"88001": "88001"},
        logger=mock_logger
    )

    result = custom_adapter._ev_charger(
        entity_id="EVC03",
        entity_parameters=specs["entities"]["EVC03"]["parameters"],
        param_id="88001",
        parameter_type_id="session_status",
        raw_values=[{"Read": 1, "DateUTC": "2026-08-19 10:00:00"}],
        now_dt=datetime.datetime.now(custom_adapter._tz)
    )

    assert result == {}