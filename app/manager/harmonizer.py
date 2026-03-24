import pandas as pd
import datetime
import time
from app.utils.logger import LoggingUtils

class Harmonizer:
    _logger : LoggingUtils
    _time_interval : int

    def __init__(self, time_interval: int, logger: LoggingUtils):
        self._logger = logger
        self._time_interval = time_interval

    def _calculate_time_index(self, timestamps: pd.DatetimeIndex, freq: int, unit: str, period_start_time: datetime.datetime,
                              period_end_time: datetime.datetime) -> pd.DatetimeIndex:
        # Find the first timestamp for the time_index
        # It should be a multiple of the frequency and <= period_start
        first = timestamps.min()  # take the smallest real timestamp
        # Adjust so it starts <= period_start
        while first > period_start_time:
            first -= pd.to_timedelta(freq, unit=unit)

        # Find the last timestamp for the time_index
        last = timestamps.max()  # take the largest real timestamp
        # Adjust so it ends >= period_end

        while last < period_end_time:
            last += pd.to_timedelta(freq, unit=unit)

        # Generate the complete sequence
        return pd.date_range(start=first, end=last, freq=f"{freq}{unit}")

    def _data_trimmer(self, entity_param, data: pd.DataFrame, start: datetime.datetime, end: datetime.datetime) -> None:
        """
        Trims and normalizes data intervals to fit within the period [start, end].
        Each row represents an interval [row_start, row_end] with an associated value.
        The value is proportionally reduced according to the overlap of the interval with [start, end].

        Args:
            data (pd.DataFrame): Must contain a "start" column (datetime) and an index datetime representing the end of the interval.
            start (datetime.datetime): Start of the target period.
            time_interval (int): Duration (in minutes) of each interval.
            end (datetime.datetime): End of the target period.

        Returns:
            pd.DataFrame: DataFrame with the original 'value' column adjusted to the window.
        """

        print(f"Entity Param: {entity_param} DF4: \n\n{data}\n\n")


        for row in data.itertuples():
            row_start = getattr(row, "start")
            row_end = row.Index
            row_value = getattr(row, "value")

            # Compute the intersection of the row interval with the target window [start, end]
            overlap_start = max(row_start, start)
            overlap_end = min(row_end, end)

            if overlap_start >= overlap_end:
                # No overlap → set value to 0
                data.at[row.Index, "value"] = 0.0
            else:
                full_duration = (row_end - row_start).total_seconds()
                overlap_duration = (overlap_end - overlap_start).total_seconds()
                fraction = overlap_duration / full_duration
                # Scale the value by the fraction of overlap
                data.at[row.Index, "value"] = row_value * fraction

    def period_harmonizer(self,entity_param,period_start_time: datetime.datetime, period_end_time: datetime.datetime, temporal_behavior : dict, data : list) -> list:
        periodicity = temporal_behavior.get("periodicity", {"value": 1, "unit": "min"})
        cumulative = temporal_behavior.get("cumulative", False)
        fill_operation = temporal_behavior.get("fill_operation", "linear")

        # Build dataframe
        df = pd.DataFrame(data).rename(columns={"timestamp": "end"})

        df['end'] = pd.to_datetime(df['end'])
        df = df.set_index('end')

        # Calculate timedelta from periodicity definition
        value = periodicity.get("value")
        unit = periodicity.get("unit")  # e.g. "min", "s", "h"
        delta = pd.to_timedelta(value, unit=unit)
        self._logger.info(f"\nEntity Param: {entity_param} Period Start Time: {period_start_time} Period End Time: {period_end_time} Value: {value} Unit: {unit} Delta: {delta}\n")
        # Build expected time index for reindexing
        time_index = self._calculate_time_index(
            df.index, value, unit, period_start_time, period_end_time
        )

        df = df.reindex(time_index)

        # Apply filling method
        if fill_operation == "forward_fill":
            df = df.ffill()
        elif fill_operation == "zero_fill":
            df = df.fillna(0)
        elif fill_operation == "linear":
            df['value'] = df['value'].interpolate(method='linear', limit_direction='both')

        # Create start and end columns
        df['start'] = df.index - delta

        if cumulative:
            self._data_trimmer(entity_param, df, period_start_time, period_end_time)

        # Convert back to list of dicts
        updated_list = [
            {"timestamp": ts.strftime("%Y-%m-%d %H:%M:%S %z") if ts.tzinfo else ts.strftime(
                "%Y-%m-%d %H:%M:%S"),
             "value": val}
            for ts, val in df['value'].items()
        ]

        return updated_list