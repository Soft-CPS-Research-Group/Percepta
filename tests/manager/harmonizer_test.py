import pytest
import pandas as pd
import datetime
import pytz
from app.manager.harmonizer import Harmonizer
from app.utils.logger import LoggingUtils


# Fixture: mocked logger for testing
@pytest.fixture
def mock_logger(mocker):
    logger = mocker.Mock(spec=LoggingUtils)
    return logger


# Fixture: Harmonizer instance with mocked logger
@pytest.fixture
def harmonizer(mock_logger):
    return Harmonizer(time_interval=60, logger=mock_logger)


# Test: _calculate_time_index generates a time index covering the full period with correct frequency and timezone
def test_calculate_time_index_1(harmonizer):
    tz = pytz.UTC

    # Input timestamps already aligned to 5-minute frequency
    timestamps = pd.date_range("2025-09-30 23:41", periods=3, freq="5min", tz=tz)

    # Period slightly extends beyond the input timestamps
    period_start = tz.localize(datetime.datetime(2025, 10, 1, 0, 0))
    period_end = tz.localize(datetime.datetime(2025, 10, 1, 0, 20))

    # Call the private method
    result = harmonizer._calculate_time_index(
        timestamps,
        freq=5,
        unit="min",
        period_start_time=period_start,
        period_end_time=period_end
    )

    # Ensure the index covers the full period
    assert result[0] <= period_start
    assert result[-1] >= period_end

    # Ensure timezone is preserved
    assert all(r.tzinfo == tz for r in result)

    # Expected index for verification
    expected_timestamps = pd.DatetimeIndex([
        datetime.datetime(2025, 9, 30, 23, 41, tzinfo=tz),
        datetime.datetime(2025, 9, 30, 23, 46, tzinfo=tz),
        datetime.datetime(2025, 9, 30, 23, 51, tzinfo=tz),
        datetime.datetime(2025, 9, 30, 23, 56, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 1, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 6, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 11, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 16, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 21, tzinfo=tz),
    ])
    pd.testing.assert_index_equal(result, expected_timestamps)

# Test: _calculate_time_index generates a time index covering the full period with correct frequency and timezone
def test_calculate_time_index_2(harmonizer):
    tz = pytz.UTC

    # Input timestamps already aligned to 5-minute frequency
    timestamps = pd.date_range("2025-10-01 00:01", periods=3, freq="5min", tz=tz)

    # Period slightly extends beyond the input timestamps
    period_start = tz.localize(datetime.datetime(2025, 10, 1, 0, 0))
    period_end = tz.localize(datetime.datetime(2025, 10, 1, 0, 20))

    # Call the private method
    result = harmonizer._calculate_time_index(
        timestamps,
        freq=5,
        unit="min",
        period_start_time=period_start,
        period_end_time=period_end
    )

    # Ensure the index covers the full period
    assert result[0] <= period_start
    assert result[-1] >= period_end

    # Ensure timezone is preserved
    assert all(r.tzinfo == tz for r in result)

    # Expected index for verification
    expected_timestamps = pd.DatetimeIndex([
        datetime.datetime(2025, 9, 30, 23, 56, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 1, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 6, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 11, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 16, tzinfo=tz),
        datetime.datetime(2025, 10, 1, 0, 21, tzinfo=tz),
    ])
    pd.testing.assert_index_equal(result, expected_timestamps)


# Test: _data_trimmer sets values to zero if no overlap and scales partial overlaps
def test_data_trimmer_partial_and_full_overlap(harmonizer):
    tz = pytz.UTC

    # Input data with three intervals: partial, partial, and full overlap
    data = pd.DataFrame({
        "start": [
            tz.localize(datetime.datetime(2025, 9, 30, 23, 50)),
            tz.localize(datetime.datetime(2025, 10, 1, 0, 0)),
            tz.localize(datetime.datetime(2025, 10, 1, 0, 10))
        ],
        "value": [50.0, 100.0, 200.0]
    }, index=[
        tz.localize(datetime.datetime(2025, 10, 1, 0, 0)),
        tz.localize(datetime.datetime(2025, 10, 1, 0, 10)),
        tz.localize(datetime.datetime(2025, 10, 1, 0, 20))
    ])

    start = tz.localize(datetime.datetime(2025, 10, 1, 0, 5))
    end = tz.localize(datetime.datetime(2025, 10, 1, 0, 20))

    harmonizer._data_trimmer(data, start, end)

    # First value is outside period → set to zero
    assert data.iloc[0]["value"] == 0.0

    # Second value partially overlaps → scaled
    assert data.iloc[1]["value"] == 50.0

    # Third value fully inside period → unchanged
    assert data.iloc[2]["value"] == 200.0


# Test: period_harmonizer with linear fill and cumulative=True
def test_period_harmonizer_linear_fill(harmonizer):
    tz = pytz.UTC

    # Input data
    data = [
        {"timestamp": "2025-10-01 00:01:00+00:00", "value": 10},
        {"timestamp": "2025-10-01 00:06:00+00:00", "value": 20},
        {"timestamp": "2025-10-01 00:11:00+00:00", "value": 30},
    ]

    temporal_behavior = {
        "cumulative": True,
        "periodicity": {"value": 5, "unit": "min"},
        "aggregation_operation": "sum",
        "fill_operation": "linear"
    }

    period_start = tz.localize(datetime.datetime(2025, 10, 1, 0, 0))
    period_end = tz.localize(datetime.datetime(2025, 10, 1, 0, 20))

    # Run harmonizer
    result = harmonizer.period_harmonizer(period_start, period_end, temporal_behavior, data)

    # Expected timestamps according to _calculate_time_index logic
    expected_timestamps = [
        "2025-09-30 23:56:00 +0000",
        "2025-10-01 00:01:00 +0000",
        "2025-10-01 00:06:00 +0000",
        "2025-10-01 00:11:00 +0000",
        "2025-10-01 00:16:00 +0000",
        "2025-10-01 00:21:00 +0000",
    ]

    # Expected values considering cumulative trimming and linear fill
    expected_values = [
        0,
        10 * (1 / 5),  # partial overlap fraction
        20,            # fully inside
        30,            # fully inside
        30,            # fully inside
        30 * (4 / 5),  # partial overlap at end
    ]

    # Extract actual timestamps and values
    result_timestamps = [row["timestamp"] for row in result]
    result_values = [row["value"] for row in result]

    # Assert timestamps match
    assert result_timestamps == expected_timestamps

    # Assert values approximately equal
    for rv, ev in zip(result_values, expected_values):
        assert pytest.approx(rv, rel=1e-2) == ev


# Test: period_harmonizer raises exception for invalid input data types
def test_period_harmonizer_type_check(harmonizer):
    tz = pytz.UTC

    # Data with incorrect types
    bad_data = [{"timestamp": 12345, "value": "not a float"}]

    temporal_behavior = {
        "cumulative": True,
        "periodicity": {"value": 5, "unit": "min"},
        "fill_operation": "linear"
    }

    start = tz.localize(datetime.datetime(2025, 10, 1, 0, 0))
    end = tz.localize(datetime.datetime(2025, 10, 1, 0, 5))

    # Expect an exception due to invalid types
    with pytest.raises(Exception):
        harmonizer.period_harmonizer(start, end, temporal_behavior, bad_data)
