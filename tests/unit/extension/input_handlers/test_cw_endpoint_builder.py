# --- Standard Library Imports ---
import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

# --- Third-Party Imports ---
import pytest

# --- Local Application Imports ---
from app.core.utils.logger import LoggingUtils
from app.extension.endpoints_builders import (
    CWEndpointBuilder,
    round_timestamp_to_nearest,
)


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
        component_name="cw_endpoint_builder_unit_test",
        configurations=config,
        environment="test"
    )


@pytest.fixture
def mock_cw_session():
    """Provides a MagicMock mimicking the static/class interface of CWSession."""
    session_mock = MagicMock()
    session_mock.get_token.return_value = "mocked_jwt_token_xyz"
    return session_mock


@pytest.fixture
def valid_base_templates():
    """Provides standard Cleanwatts base endpoint templates."""
    return [
        {
            "path": "/data/request/Instant?instantType=avg&tags={param_id}&from={from}&to={to}",
            "method": "GET",
            "timeout_ms": 5000,
            "headers": {"Authorization": "CW {token}"}
        }
    ]


@pytest.fixture
def valid_environment_specs():
    """Provides an environment specs dictionary with multiple entities and parameters."""
    return {
        "entities": {
            "PV01": {
                "label": "pv_panel",
                "parameters": {
                    "energy": {
                        "id": "33711",
                        "label": "energy",
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
                        "measurementUnit": "kWh"
                    },
                    "SoC": {
                        "id": "33690",
                        "label": "state_of_charge",
                        "measurementUnit": "%"
                    }
                }
            }
        }
    }


@pytest.fixture
def valid_source_mapping_rules():
    """Provides tag mapping rules for entity parameter resolution."""
    return {
        "energy": "33711",
        "energy_in": "35927",
        "SoC": "33690"
    }


@pytest.fixture
def builder(
    valid_base_templates,
    valid_environment_specs,
    valid_source_mapping_rules,
    mock_logger,
    mock_cw_session
):
    """Fixture returning an initialized CWEndpointBuilder instance."""
    return CWEndpointBuilder(
        base_endpoint_templates=valid_base_templates,
        environment_name="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        time_interval_seconds=15,
        timezone="Europe/Lisbon",
        cw_session=mock_cw_session
    )


# ============================================================================
# 1. HELPER FUNCTION UNIT TESTS (round_timestamp_to_nearest)
# ============================================================================

def test_round_timestamp_to_nearest_downward_unit_test():
    """Verifies that timestamp rounds down to the nearest 15-second boundary."""
    tz = ZoneInfo("UTC")
    # 10:00:04 rounds to 10:00:00
    sample_dt = datetime.datetime(2026, 8, 18, 10, 0, 4, tzinfo=tz)
    rounded = round_timestamp_to_nearest(sample_dt, interval_seconds=15)

    assert rounded.second == 0
    assert rounded.tzinfo == tz


def test_round_timestamp_to_nearest_upward_unit_test():
    """Verifies that timestamp rounds up to the nearest 15-second boundary."""
    tz = ZoneInfo("UTC")
    # 10:00:10 rounds to 10:00:15
    sample_dt = datetime.datetime(2026, 8, 18, 10, 0, 10, tzinfo=tz)
    rounded = round_timestamp_to_nearest(sample_dt, interval_seconds=15)

    assert rounded.second == 15
    assert rounded.tzinfo == tz


# ============================================================================
# 2. INITIALIZATION & PRE-CALCULATION UNIT TESTS (_precalculate_static_endpoints)
# ============================================================================

def test_init_success_and_boundary_alignment_unit_test(builder):
    """Verifies successful builder initialization and sliding window boundaries."""
    assert builder._interval == datetime.timedelta(seconds=15)
    assert builder._end - builder._start == datetime.timedelta(seconds=15)
    assert "PV01" in builder._static_grouped_endpoints
    assert "B01" in builder._static_grouped_endpoints


def test_precalculate_static_endpoints_grouping_and_metadata_unit_test(builder):
    """Verifies that static endpoints contain substituted param_ids and complete metadata."""
    grouped = builder._static_grouped_endpoints

    # 1. Check PV01 single parameter route
    pv_endpoints = grouped.get("PV01", [])
    assert len(pv_endpoints) == 1
    assert "tags=33711" in pv_endpoints[0]["path"]
    assert pv_endpoints[0]["metadata"] == {
        "environment": "R-H-01",
        "entity_id": "PV01",
        "entity_label": "pv_panel",
        "param_name": "energy",
        "param_label": "energy"
    }

    # 2. Check B01 multi-parameter routes
    b01_endpoints = grouped.get("B01", [])
    assert len(b01_endpoints) == 2
    param_names = [ep["metadata"]["param_name"] for ep in b01_endpoints]
    param_labels = [ep["metadata"]["param_label"] for ep in b01_endpoints]

    assert "energy_in" in param_names
    assert "SoC" in param_names
    assert "energy_in" in param_labels
    assert "state_of_charge" in param_labels


def test_precalculate_static_endpoints_skips_unmapped_parameters_unit_test(
    valid_base_templates, valid_environment_specs, mock_logger, mock_cw_session
):
    """Verifies that parameters without mapping rules are omitted with a warning."""
    # Only map 'energy', leave 'energy_in' and 'SoC' unmapped
    partial_rules = {"energy": "33711"}

    custom_builder = CWEndpointBuilder(
        base_endpoint_templates=valid_base_templates,
        environment_name="R-H-01",
        environment_specs=valid_environment_specs,
        source_mapping_rules=partial_rules,
        logger=mock_logger,
        cw_session=mock_cw_session
    )

    assert "PV01" in custom_builder._static_grouped_endpoints
    # B01 has no mapped parameters, so it must not exist in static grouped endpoints
    assert "B01" not in custom_builder._static_grouped_endpoints


def test_precalculate_static_endpoints_handles_empty_entities_unit_test(
    valid_base_templates, valid_source_mapping_rules, mock_logger, mock_cw_session
):
    """Verifies graceful handling when no entities are configured."""
    empty_specs = {"entities": {}}

    empty_builder = CWEndpointBuilder(
        base_endpoint_templates=valid_base_templates,
        environment_name="R-H-01",
        environment_specs=empty_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        cw_session=mock_cw_session
    )

    assert empty_builder._static_grouped_endpoints == {}

@pytest.mark.asyncio
async def test_build_endpoints_returns_empty_dict_when_no_static_endpoints_unit_test(
    valid_base_templates,
    valid_source_mapping_rules,
    mock_logger,
    mock_cw_session
):
    """Verifies that build_endpoints returns an empty dictionary and slides the
    time window correctly when no static endpoints are configured.
    """
    empty_specs = {"entities": {}}

    empty_builder = CWEndpointBuilder(
        base_endpoint_templates=valid_base_templates,
        environment_name="R-H-01",
        environment_specs=empty_specs,
        source_mapping_rules=valid_source_mapping_rules,
        logger=mock_logger,
        time_interval_seconds=15,
        timezone="UTC",
        cw_session=mock_cw_session
    )

    initial_end = empty_builder._end

    # Execute dynamic builder resolution
    resolved_groups = await empty_builder.build_endpoints()

    # 1. Must return an empty dictionary without raising exceptions
    assert resolved_groups == {}

    # 2. Sliding time window boundaries should still advance normally
    assert empty_builder._start == initial_end
    assert empty_builder._end == initial_end + empty_builder._interval

# ============================================================================
# 3. DYNAMIC RESOLUTION & SLIDING WINDOW UNIT TESTS (build_endpoints)
# ============================================================================

@pytest.mark.asyncio
async def test_build_endpoints_interpolates_time_window_and_auth_unit_test(builder, mock_cw_session):
    """Verifies that build_endpoints injects token and current from/to timestamps."""
    start_snapshot = builder._start
    end_snapshot = builder._end

    expected_from = start_snapshot.strftime("%Y-%m-%dT%H:%M:%S")
    expected_to = end_snapshot.strftime("%Y-%m-%dT%H:%M:%S")

    resolved_groups = await builder.build_endpoints()

    # 1. Check token was requested and header injected
    mock_cw_session.get_token.assert_called_once()
    pv_ep = resolved_groups["PV01"][0]
    assert pv_ep["headers"] == {"Authorization": "CW mocked_jwt_token_xyz"}

    # 2. Check dynamic timestamps replacement in path
    assert f"from={expected_from}" in pv_ep["path"]
    assert f"to={expected_to}" in pv_ep["path"]
    assert "{from}" not in pv_ep["path"]
    assert "{to}" not in pv_ep["path"]


@pytest.mark.asyncio
async def test_build_endpoints_slides_time_window_forward_unit_test(builder):
    """Verifies that consecutive build_endpoints calls slide temporal boundaries forward."""
    first_start = builder._start
    first_end = builder._end

    # First cycle
    await builder.build_endpoints()

    # Boundaries should advance by 1 interval
    assert builder._start == first_end
    assert builder._end == first_end + datetime.timedelta(seconds=15)

    # Second cycle
    second_end = builder._end
    await builder.build_endpoints()

    assert builder._start == second_end
    assert builder._end == second_end + datetime.timedelta(seconds=15)
    assert builder._start > first_start


@pytest.mark.asyncio
async def test_build_endpoints_handles_empty_token_fallback_unit_test(builder, mock_cw_session):
    """Verifies that an empty string token is formatted gracefully without crashing."""
    mock_cw_session.get_token.return_value = None

    resolved_groups = await builder.build_endpoints()

    pv_ep = resolved_groups["PV01"][0]
    assert pv_ep["headers"] == {"Authorization": "CW "}