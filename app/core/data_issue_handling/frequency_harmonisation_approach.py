# --- Standard Library Imports ---
import math
from bisect import bisect_right
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

# --- Internal Framework Imports ---
from app.core.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.utils.logger import LoggingUtils
from app.core.utils.time_utils import duration_to_seconds, format_timestamp, parse_timestamp


class FrequencyHarmonisationApproach(DataIssueHandlingApproachBase):
    """Makes the readings of each parameter fit the period that is being processed.

    Words used in this file (they have different meanings, so please do not mix them):
    - Frequency: how often Percepta processes the data. It is ONE value for the whole Percepta instance, set in the
      Percepta Instance Configuration. Example: 10 minutes.
    - Period: the block of time that one round of processing is about. Its length is the frequency. It does not include
      its start, but it includes its end. Example: with a frequency of 10 minutes and a period that ends at 10:10, the
      period is the time after 10:00 up to 10:10.
    - Periodicity: how often ONE parameter reports a reading. Each parameter has its own, in the schema (in its
      'temporal_behavior'). Example: 3 minutes. The frequency is about Percepta, the periodicity is about a parameter.
    - Reading: one item {"timestamp": ..., "value": ...} of the list 'values' of a parameter.
    - Cumulative: a parameter is cumulative when each value is a total for a stretch of time (e.g., the energy used in 3
      minutes). It is not cumulative when each value is a measurement at one moment (e.g., the power now).
    - Slot: the stretch of time that one reading stands for. A slot is as long as the periodicity of the parameter, and
      it is named after its end. Example: with a periodicity of 3 minutes, the reading of 10:05 stands for the slot of
      10:05, which is the time after 10:02 up to 10:05.
    - Anchor: the most recent reading of the parameter. The slots are placed so that the anchor is the border between
      two slots: one slot ends exactly at it, and the next slot starts exactly at it. Example: with an anchor at 10:08
      and a periodicity of 3 minutes, one slot is 10:05-10:08 and the next one is 10:08-10:11.

    The problem: the readings of a parameter do not fit the period. The parameter reports at its own pace, which is
    usually different from the frequency, and at its own moments (e.g., 10:02, 10:05, 10:08, and not 10:00, 10:03,
    10:06). So the first slot may start before the period, the last slot may end after it, and some slots may have no
    reading. This approach solves it in 3 steps:
    1. Place the slots. First find the anchor. Then draw a row of slots, one right after the other, each one as long as
       the periodicity of the parameter, with the anchor as the border between two of them. The row goes backwards
       and forwards in time until the whole period is covered. Only the slots that touch the period will be in the
       result. The readings that belong to the other slots are not kept in the result, but they are still used in
       step 2, as neighbours, to work out the value of the readings that are missing.
       Example: periodicity of 3 minutes, anchor at 10:08, period 10:00-10:10. The row is:
       ... | 09:56-09:59 | 09:59-10:02 | 10:02-10:05 | 10:05-10:08 | 10:08-10:11 | ...
       The anchor (the reading of 10:08) belongs to the slot 10:05-10:08, the one that ends at it. The next slot,
       10:08-10:11, starts at the anchor and has no reading yet.
       The slot 09:56-09:59 will not be in the result, because it ends before the period starts (a reading in it can
       still be used in step 2). The other four will be in the result. The first one starts before the period (at
       09:59) and the last one ends after it (at 10:11), but they still touch it.
    2. Fill. Some slots have no reading (e.g., the parameter did not report, or the reading has not arrived yet). For
       each of them, the approach creates a new reading, so that every slot has one.
       - The timestamp of the new reading is the end of its slot. Example: for the slot 10:08-10:11, it is 10:11.
       - The value of the new reading is worked out from the other readings of the parameter, in the way chosen by
         the 'fill_operation' of the parameter in the schema. Example: the parameter has a reading of 12.0 at 10:05
         and a reading of 16.0 at 10:15, and the reading of 10:10 is missing:
         - "forward_fill": 12.0, the value of the closest reading before 10:10. If there is none before it, the value
           of the closest reading after it is used;
         - "zero_fill": 0.0;
         - "linear": 14.0, on a straight line between 12.0 and 16.0. 10:10 is half way between 10:05 and 10:15, so
           the value is half way between 12.0 and 16.0. If there is only one of the two readings, its value is used.
    3. Trim. A slot that is only partly inside the period must only count the part that is inside. This only matters
       for a cumulative parameter: its value is multiplied by (time of the slot inside the period) / (periodicity). A
       parameter that is not cumulative is not trimmed, because its values are measurements at one moment.

    Keeping the readings after the period (optional, 'keep_future_readings'):
    Some parameters are a series that goes forward in time (e.g., the prices of the day ahead, or a forecast), and the
    whole series is needed, not only what touches the period. A parameter with "keep_future_readings": true in its
    'temporal_behavior' is harmonised in a different way:
    - The readings that belong to slots AFTER the period are kept (the slots that touch the period are kept as before).
      They are kept as they arrived, with the same timestamp and value. No reading of the parameter is trimmed (step
      3), not even a cumulative one and not even in the slots that touch the period.
    - Before the readings are removed, the slots with no reading between the period and the last reading of the series
      are filled, with the 'fill_operation' of the parameter, as in step 2. So the series has no holes.
    - The readings that belong to slots BEFORE the period are still removed (they are only used as neighbours).
    - The slots after the last reading of the series are not created: there is no reading after them to work out
      their value.
    The parameter is false by default, and then the approach works as described in the rest of this docstring.
    Example: periodicity of 15 minutes, "linear", keep_future_readings true. The period is the time after 10:00:00 up
    to 10:00:15. The readings are 10:00 (0.10), 10:15 (0.12), 10:45 (0.18) and 11:00 (0.20), and the one of 10:30 is
    missing.
    Output: 10:00 is in the slot that ends at 10:00, which is before the period, so it is removed. The others are kept,
    and the missing one is created: 10:15 (0.12), 10:30 (0.15), 10:45 (0.18) and 11:00 (0.20).

    Example 1: the parameter reports more often than the frequency, and not at round times.
    Frequency 10 minutes, periodicity 3 minutes, cumulative, "forward_fill". The period is the time after 10:00 up to
    10:10. The parameter reported at 10:02, 10:05 and 10:08. The reading of 10:11 does not exist yet, because it will
    only arrive in the next period.
    - Step 1: the anchor is the reading of 10:08. The slots are 09:59-10:02, 10:02-10:05, 10:05-10:08 and 10:08-10:11.
    - Step 2: the slot 10:08-10:11 has no reading, so one is created at 10:11. Its value is the one of the reading
      before it (10:08).
    - Step 3: the first slot has 2 of its 3 minutes inside the period (10:00-10:02). The last slot also has 2 of its 3
      minutes inside it (10:08-10:10). So their values are multiplied by 2 / 3. The two slots in the middle are fully
      inside the period and are not changed.
    Input:  "values": [{"timestamp": "2026-01-01 10:02:00 +0000", "value": 6.0},
                       {"timestamp": "2026-01-01 10:05:00 +0000", "value": 9.0},
                       {"timestamp": "2026-01-01 10:08:00 +0000", "value": 3.0}]
    Output: "values": [{"timestamp": "2026-01-01 10:02:00 +0000", "value": 4.0},
                       {"timestamp": "2026-01-01 10:05:00 +0000", "value": 9.0},
                       {"timestamp": "2026-01-01 10:08:00 +0000", "value": 3.0},
                       {"timestamp": "2026-01-01 10:11:00 +0000", "value": 2.0}]
    In the next period (10:10-10:20), the real reading of 10:11 arrives. Its slot (10:08-10:11) is trimmed to the
    other part (10:10-10:11, which is 1 / 3 of the slot). So the slot is shared by the two periods: no time is
    counted twice, and no time is lost.

    Example 2: the parameter reports less often than the frequency.
    Frequency 15 seconds, periodicity 5 minutes, cumulative, "forward_fill". The period is the time after 10:05:30 up
    to 10:05:45. The only reading is the one of 10:05:00, which is the last one the parameter reported, so it is the
    anchor.
    - Step 1: the slots are 10:00-10:05 and 10:05-10:10. Only the second one touches the period, so only it will be
      in the result. The reading is in the first slot, so it is not in the result, but it is still used in step 2 to
      work out the value of the second slot.
    - Step 2: the slot 10:05-10:10 has no reading, so one is created at 10:10, with the value 400.0.
    - Step 3: the slot has 15 seconds inside the period, out of 300 seconds. So the value is 400 x 15 / 300 = 20.0.
    Input:  "values": [{"timestamp": "2026-01-01 10:05:00 +0000", "value": 400.0}]
    Output: "values": [{"timestamp": "2026-01-01 10:10:00 +0000", "value": 20.0}]
    In a period in which the parameter sends nothing, the reading above is not in the input. It is the Missing Data
    approach (which runs before this one) that gives the readings that it remembers to the entity. This approach
    does not remember anything between periods.

    Which readings are kept:
    - A reading does not need to arrive exactly at the end of a slot. It belongs to the slot whose time contains its
      timestamp. A slot does not include its start, but includes its end.
      Example: the slot 10:02-10:05 is the time after 10:02 up to 10:05. So:
      - a reading at 10:04 belongs to it, even if the parameter was expected to report at 10:05;
      - a reading at 10:05:00 also belongs to it, because the end is included;
      - a reading at 10:05:01 does not, it belongs to the next slot (10:05-10:08).
      A slot that has a reading keeps it exactly as it arrived. The reading at 10:04 stays at 10:04, it is not moved
      to 10:05.
    - If two readings belong to the same slot, only the one with the later timestamp is kept.
    - A reading that belongs to a slot that does not touch the period is removed. It is only used to work out the
      value of a missing reading. Example: a reading at 09:55 that arrived late belongs to an earlier period, which
      was already processed. (The exception is a parameter with "keep_future_readings": the readings of the slots after
      the period are kept.)
    - A reading with no timestamp, a timestamp in the wrong format, or a value that is not a number is removed,
      because it cannot be placed in time or used in a calculation.

    What the approach remembers:
    Nothing. It only works with the readings that it receives in the period. The memory of the readings of the past
    is in the Missing Data approach, which runs before this one.

    Assumption and known limit:
    The approach assumes that the timestamps of the readings follow the periodicity of the parameter (e.g., a
    parameter with a periodicity of 3 minutes reports at 10:02, 10:05, 10:08, and so on). This is expected in
    production. The approach always places the slots from the anchor, and it does not correct readings that do not
    follow the periodicity. This can be a problem when they do not:
    - A reading that is later than its slot, even by a few seconds, falls in the next slot. If that slot already has a
      reading, only the later one is kept, and the other one is lost.
      Example: periodicity of 5 minutes and readings at 10:04:40, 10:10:20 and 10:14:50. The anchor is 10:14:50, so the
      slots end at 10:14:50, 10:09:50 and 10:04:50. The reading of 10:10:20 is after the end of the slot 10:09:50, so it
      falls in the slot 10:14:50 and is lost, because the reading of 10:14:50 is later. The slot 10:09:50 gets a
      created reading instead.
    - If the parameter really reports more often than its periodicity says, readings are lost (several readings fall
      in the same slot). If it reports less often, readings are created that did not exist. For a cumulative
      parameter, both change the total.
    The approach does not warn about this. The periodicity in the schema must be correct.

    What the approach does NOT do:
    - It does not create a parameter that is not in the entity. It only works on the parameters that arrived.
    - If a parameter has no reading at all ("values": None, or an empty list), the slots cannot be placed and there is
      no value to fill with, so nothing is created (the rest of Percepta takes it as a null). A parameter with
      "values": None is left as it is. The only exception is an EMPTY list with "zero_fill": zero does not need a
      reading, so the slots end at the end of the period and are filled with zero.
    - A parameter without a periodicity in the schema is not changed.

    This approach should be the last one to be applied (see the README of this folder), because it needs the entities
    to be complete (Missing Data), without duplicated readings (Duplicated Data) and in order (Out-of-Order Data).
    """

    _LOG_PREFIX: str = "Frequency Harmonisation Approach |"

    # The ways of filling a missing reading that the 'fill_operation' of a parameter can have
    _FORWARD_FILL: str = "forward_fill"
    _ZERO_FILL: str = "zero_fill"
    _LINEAR: str = "linear"
    _FILL_OPERATIONS: Tuple[str, ...] = (_FORWARD_FILL, _ZERO_FILL, _LINEAR)

    def __init__(self, environment: Dict[str, Any], frequency_seconds: float, logger: LoggingUtils) -> None:
        """Reads from the schema how each parameter must be harmonised.

        The settings of each parameter are worked out once, here, and not again in every period. Only the parameters
        with a periodicity are kept, because the others cannot be harmonised.

        Args:
            environment (Dict[str, Any]): The environment, as returned by the Schema Storage. Only 'entities' is used.
            frequency_seconds (float): How often the data is processed, in seconds. It is the length of a period.
            logger (LoggingUtils): Logger of the environment.

        Raises:
            ValueError: If the environment has no entities, or the 'temporal_behavior' of a parameter is not valid
                (a periodicity that cannot be read, a 'cumulative' or 'keep_future_readings' that is not true or
                false, or a 'fill_operation' that is not one of the known ones).
        """
        super().__init__(environment, frequency_seconds, logger)

        # --- Guard Clause 1: the environment must have its entities ---
        entities = environment.get("entities")
        if not isinstance(entities, dict) or not entities:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: the environment has no 'entities'.")

        # The settings of each parameter that can be harmonised, by entity and by parameter.
        # Example: {"id_01": {"id_param_01": {
        #     "periodicity_seconds": 300, "cumulative": False, "fill_operation": "linear",
        #     "keep_future_readings": False}}}
        self._parameters: Dict[str, Dict[str, Dict[str, Any]]] = {}
        for entity_id, entity in entities.items():
            parameters = entity.get("parameters") if isinstance(entity, dict) else None
            if not parameters:
                continue

            for parameter_id, parameter in parameters.items():
                settings = self._read_parameter(entity_id, parameter_id, parameter)
                if settings is not None:
                    self._parameters.setdefault(entity_id, {})[parameter_id] = settings

    def handle(self, data: List[Dict[str, Any]], period_end: datetime) -> List[Dict[str, Any]]:
        """Harmonises the readings of each parameter of each entity.

        Expected Input Structure:
        -------------------------
        data = [
            {
                "id": "id_01",
                "entity_type_id": "entity_type_1",
                "parameters": {
                    "id_param_01": {
                        "parameter_type_id": "parameter_type_1",
                        "values": [
                            {"timestamp": "2026-01-01 10:05:00 +0000", "value": 12.0},
                            {"timestamp": "2026-01-01 10:15:00 +0000", "value": 16.0}
                        ]
                    }
                },
                "timestamp": "2026-01-01 10:15:00 +0000"
            }
        ]
        period_end = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)

        Expected Output Structure:
        --------------------------
        The same structure, with the lists 'values' harmonised (see the class docstring for examples).

        Args:
            data (List[Dict[str, Any]]): The data received in the period, already treated by the earlier approaches.
            period_end (datetime): The end of the period, with a timezone.

        Returns:
            List[Dict[str, Any]]: The data with the readings harmonised. It is the same list that was received, changed.
        """
        for entity in data:
            # The settings of the parameters of this entity. An entity that is not in the schema, or that has no
            # parameter that can be harmonised, is left as it is.
            entity_settings = self._parameters.get(entity.get("id"))
            if not entity_settings:
                continue

            for parameter_id, parameter in entity.get("parameters", {}).items():
                settings = entity_settings.get(parameter_id)

                # A parameter that cannot be harmonised (it has no periodicity in the schema), or that arrived with no
                # list 'values' at all, is left as it is.
                # Note that a parameter with an EMPTY list 'values' is not skipped: it stays empty (no reading to work
                # from), unless its 'fill_operation' is "zero_fill", which creates the readings with zero.
                if settings is None or parameter.get("values") is None:
                    continue

                self._harmonise(entity.get("id"), parameter_id, parameter, settings, period_end)

        return data

    def _read_parameter(self, entity_id: str, parameter_id: str, parameter: Any) -> Optional[Dict[str, Any]]:
        """Works out, from the schema, how a parameter must be harmonised.

        Expected Input Structure:
        -------------------------
        parameter = {
            "parameter_type_id": "parameter_type_1",
            "temporal_behavior": {
                "cumulative": True,
                "periodicity": {"value": 5, "unit": "min"},
                "aggregation_operation": "sum",
                "fill_operation": "linear",
                "keep_future_readings": False         # Optional (default False). True keeps the readings after the period
            }
        }

        Expected Output Structure:
        --------------------------
        {"periodicity_seconds": 300, "cumulative": True, "fill_operation": "linear", "keep_future_readings": False}

        Args:
            entity_id (str): Id of the entity. It is only used in the error messages.
            parameter_id (str): Id of the parameter. It is only used in the error messages.
            parameter (Any): The parameter, as it is in the schema.

        Returns:
            Optional[Dict[str, Any]]: The settings of the parameter, or None if it has no periodicity (and so it
                cannot be harmonised).

        Raises:
            ValueError: If the periodicity cannot be read, 'cumulative' or 'keep_future_readings' is not true or
                false, or 'fill_operation' is not a known one.
        """
        temporal_behavior = parameter.get("temporal_behavior") if isinstance(parameter, dict) else None
        periodicity = temporal_behavior.get("periodicity") if isinstance(temporal_behavior, dict) else None

        # --- Guard Clause 2: without a periodicity, the expected readings cannot be known ---
        if periodicity is None:
            return None

        where = (
            f"parameter '{parameter_id}' of entity '{entity_id}' (parameter type: "
            f"{parameter.get('parameter_type_id') if isinstance(parameter, dict) else None})"
        )
        try:
            periodicity_seconds = duration_to_seconds(periodicity, "periodicity")
        except ValueError as err:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: {where}: {err}") from err

        # --- Guard Clause 3: 'cumulative' is optional (the default is False), but it must be true or false ---
        cumulative = temporal_behavior.get("cumulative", False)
        if not isinstance(cumulative, bool):
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: {where}: 'cumulative' must be true or false.")

        # --- Guard Clause 4: 'fill_operation' is optional, but it must be a known one if given ---
        # Without it, the readings that are missing are not created.
        fill_operation = temporal_behavior.get("fill_operation")
        if fill_operation is not None and fill_operation not in self._FILL_OPERATIONS:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: {where}: 'fill_operation' is '{fill_operation}', which "
                f"is not a known one. Known: {list(self._FILL_OPERATIONS)}."
            )

        # --- Guard Clause 5: 'keep_future_readings' is optional (the default is False), but it must be true or false ---
        keep_future_readings = temporal_behavior.get("keep_future_readings", False)
        if not isinstance(keep_future_readings, bool):
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: {where}: 'keep_future_readings' must be true or false."
            )

        return {
            "periodicity_seconds": periodicity_seconds,
            "cumulative": cumulative,
            "fill_operation": fill_operation,
            "keep_future_readings": keep_future_readings
        }

    def _harmonise(
            self, entity_id: str, parameter_id: str, parameter: Dict[str, Any], settings: Dict[str, Any],
            period_end: datetime
    ) -> None:
        """Makes the readings of one parameter fit the period: places the slots, fills them and trims them.

        It changes 'parameter["values"]'. The class docstring explains the words used here (period, periodicity, slot,
        anchor, ...) and shows examples. The method works in six steps, each one explained in the code:
        1. Collects the readings that can be used (the ones that arrived and can be placed in time and used in a calculation);
        2. Finds the anchor (the most recent reading);
        3. Works out which slots touch the period (and, if the parameter keeps the readings after the period, the slots
           after it, up to the last reading);
        4. Puts each reading in the slot where its timestamp falls;
        5. Goes through the slots: keeps the reading, or creates one, and trims it if the parameter is cumulative;
        6. Replaces the list 'values' by the new list.

        Args:
            entity_id (str): Id of the entity.
            parameter_id (str): Id of the parameter.
            parameter (Dict[str, Any]): The parameter as it arrived, with its list 'values'.
            settings (Dict[str, Any]): The settings of the parameter, as given by '_read_parameter'.
            period_end (datetime): The end of the period, with a timezone.
        """
        periodicity = settings["periodicity_seconds"]
        keep_future = settings["keep_future_readings"]
        values = parameter["values"]

        # All the times below are numbers of seconds since a fixed date (the "epoch"), so they are easy to compare and
        # to add. The example used in the comments below is: frequency of 10 minutes, period that ends at 10:10 (so it
        # starts at 10:00), and a parameter with a periodicity of 3 minutes.
        period_end_epoch = period_end.timestamp()
        period_start_epoch = period_end_epoch - self._frequency_seconds

        # STEP 1: collects the readings that can be used. Each item of 'usable' is:
        # (time of the reading, its value, the reading itself).
        usable: List[Tuple[float, Any, Dict[str, Any]]] = []
        for reading in values:
            # A reading with no timestamp, a timestamp in the wrong format, or a value that is not a number cannot be
            # placed in time or used in a calculation. It is skipped, so it will not be in the new list.
            # 'isinstance(value, bool)' is checked because, for Python, True and False are numbers (1 and 0).
            try:
                moment = parse_timestamp(reading["timestamp"]).timestamp()
                value = reading.get("value")
            except (KeyError, ValueError, TypeError, AttributeError):
                continue
            if isinstance(value, bool) or not isinstance(value, (int, float)):
                continue
            usable.append((moment, value, reading))

        # The readings are sorted by time. This lets '_fill_value' find the readings around a time quickly.
        usable.sort(key=lambda item: item[0])

        # STEP 2: the anchor is the most recent reading. One slot ends exactly at it, and the others follow from it.
        if usable:
            anchor = usable[-1][0]
        elif settings["fill_operation"] == self._ZERO_FILL:
            # There is no reading at all, but zero does not need one. The slots are placed to end at the end of the
            # period.
            anchor = period_end_epoch
        else:
            # There is no reading to place the slots, and no value to fill with. So nothing can be created. The
            # readings that arrived (they were all unusable) are removed.
            self._logger.debug(
                f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity_id}' (parameter type: "
                f"{parameter.get('parameter_type_id')}): no reading to use, nothing created."
            )
            parameter["values"] = []
            return

        # STEP 3: works out which slots touch the period. The slots are numbered from the anchor:
        # - slot 0 is the one that ends at the anchor;
        # - slot 1 is the next one (it ends one periodicity after the anchor), slot 2 the one after it, and so on;
        # - slot -1 is the one before slot 0, slot -2 the one before it, and so on.
        # So slot 'n' is the time after (anchor + (n - 1) x periodicity) up to (anchor + n x periodicity).
        # - 'first_slot' is the first slot that ends AFTER the start of the period (the start is not in the period).
        # - 'last_slot' is the first slot that ends AT or AFTER the end of the period (so it contains the end).
        # 'math.floor' rounds down and 'math.ceil' rounds up. 'round(..., 9)' removes tiny errors in the last decimals
        # of a division (e.g., 2.0000000001 would be rounded up to 3 instead of 2).
        # Example: anchor 10:08, so slot 0 is 10:05-10:08. The start of the period (10:00) is 8 minutes before the
        # anchor: -8 / 3 = -2.67, rounded down is -3, plus 1 is -2. So 'first_slot' is -2, which is 09:59-10:02. The
        # end of the period (10:10) is 2 minutes after the anchor: 2 / 3 = 0.67, rounded up is 1. So 'last_slot' is 1,
        # which is 10:08-10:11. The slots that touch the period are -2, -1, 0 and 1.
        first_slot = math.floor(round((period_start_epoch - anchor) / periodicity, 9)) + 1
        last_slot = math.ceil(round((period_end_epoch - anchor) / periodicity, 9))

        # A parameter that keeps the readings after the period goes on until the slot of its last reading, which is the
        # anchor (slot 0). If the anchor is not after the period, the last slot is the same as before.
        final_slot = max(last_slot, 0) if keep_future else last_slot

        # STEP 4: puts each reading in a slot. A reading at the end of a slot belongs to that slot, so the distance
        # from the anchor is divided by the periodicity and rounded UP ('math.ceil').
        # Example: anchor 10:08, periodicity 3 minutes.
        #   reading at 10:08 -> 0 minutes from the anchor -> 0 / 3 = 0.0 -> slot 0 (10:05-10:08)
        #   reading at 10:07 -> -1 minute -> -1 / 3 = -0.33 -> rounded up is 0 -> slot 0 (10:05-10:08)
        #   reading at 10:05 -> -3 minutes -> -3 / 3 = -1.0 -> slot -1 (10:02-10:05)
        # Only the slots that touch the period are kept. If two readings belong to the same slot, the later one wins.
        in_slot: Dict[int, Tuple[float, Any, Dict[str, Any]]] = {}
        for item in usable:
            slot = math.ceil(round((item[0] - anchor) / periodicity, 9))
            if first_slot <= slot <= final_slot and (slot not in in_slot or item[0] > in_slot[slot][0]):
                in_slot[slot] = item

        # All the usable readings, as (time, value), in order of time. Also the ones that are not in a slot that
        # touches the period: they are good neighbours to work out the value of a missing reading.
        neighbours = [(item[0], item[1]) for item in usable]
        neighbour_times = [item[0] for item in usable]

        # STEP 5: goes through the slots that touch the period, from the first to the last.
        harmonised: List[Dict[str, Any]] = []
        created = 0
        for slot in range(first_slot, final_slot + 1):
            slot_end = anchor + slot * periodicity
            slot_start = slot_end - periodicity

            if slot in in_slot:
                # The slot has a reading: it is kept as it arrived (same timestamp and same value).
                _, value, reading = in_slot[slot]
            else:
                # The slot has no reading: one is created at the end of the slot, with the value given by the
                # 'fill_operation'. If no value can be worked out, the slot stays without a reading.
                value = self._fill_value(settings["fill_operation"], slot_end, neighbour_times, neighbours)
                if value is None:
                    continue

                # The timestamp of the new reading is the end of the slot. It is worked out from the end of the period,
                # moved by the difference between the end of the slot and the end of the period. The difference is
                # negative if the slot ends before the period ends, and positive if it ends after it.
                difference = round(slot_end - period_end_epoch, 6)
                reading = {"timestamp": format_timestamp(period_end + timedelta(seconds=difference)), "value": value}
                created += 1

            # Trim: only a cumulative parameter is trimmed, and only a slot that is partly outside the period. The
            # value is multiplied by the part of the slot that is inside the period, divided by the periodicity.
            # Example: slot 09:59-10:02 and period 10:00-10:10. The part inside is from 10:00 up to 10:02 (the smaller
            # of the two ends minus the larger of the two starts) = 2 minutes. With a periodicity of 3 minutes, the
            # value is multiplied by 2 / 3. The multiplication is done before the division, so that the result is exact
            # when possible (6 x 120 / 180 = 4.0).
            # This is done after the filling, so that the values used to fill were not trimmed.
            # The reading is copied before the change, so that the reading that arrived is not changed.
            # A parameter that keeps the readings after the period is never trimmed, not even in the slots that touch
            # the period: the series is kept as it arrived. (A slot after the period has no part inside it, so trimming
            # it would multiply the value by a negative number.)
            if settings["cumulative"] and not keep_future:
                inside = min(slot_end, period_end_epoch) - max(slot_start, period_start_epoch)
                if inside < periodicity - 1e-9:
                    reading = dict(reading)
                    reading["value"] = value * inside / periodicity

            harmonised.append(reading)

        # STEP 6: the list 'values' is replaced by the new one. The readings that arrived and are not in the new list
        # are the ones in slots that do not touch the period, the ones that cannot be read, and the ones that lost
        # their slot to a later reading.
        removed = len(values) - len(in_slot)
        if created or removed:
            self._logger.debug(
                f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity_id}' (parameter type: "
                f"{parameter.get('parameter_type_id')}): created {created} reading(s) and removed {removed}."
            )
        parameter["values"] = harmonised

    def _fill_value(
            self, fill_operation: Optional[str], moment: float, usable_times: List[float],
            usable: List[Tuple[float, Any]]
    ) -> Optional[float]:
        """Works out the value of a reading that is missing, using the readings around it.

        'usable_times' and 'usable' have the readings of the parameter in order of time, as the times only, and as
        (time, value) pairs. They have the same readings in the same positions. They can have readings from outside
        the period, which are good neighbours.

        Example: the usable readings are (10:05, 12.0) and (10:15, 16.0), and the reading at 10:10 is missing.
        - "forward_fill": 12.0, the value of the closest reading before 10:10;
        - "zero_fill": 0.0;
        - "linear": 14.0, half way between 12.0 and 16.0, because 10:10 is half way between 10:05 and 10:15.

        Args:
            fill_operation (Optional[str]): "forward_fill", "zero_fill" or "linear". None means do not fill.
            moment (float): The time of the missing reading, in seconds since the epoch.
            usable_times (List[float]): The times of the usable readings, sorted.
            usable (List[Tuple[float, Any]]): The (time, value) of the usable readings, sorted by time.

        Returns:
            Optional[float]: The value, or None if it cannot be worked out (no operation, or no reading to use).
        """
        if fill_operation is None:
            return None
        if fill_operation == self._ZERO_FILL:
            return 0.0

        # From here, the value depends on the other readings. Without any, there is nothing to use.
        if not usable:
            return None

        # 'bisect_right' gives the position, in the sorted times, where 'moment' would be inserted, after any reading
        # at the same time. So the reading at the position before it (if any) is the closest one at or before the
        # moment, and the reading at that position (if any) is the closest one after it.
        # Example: times [10:05, 10:15] and moment 10:10 -> position 1 -> before = 10:05 and after = 10:15.
        position = bisect_right(usable_times, moment)
        before = usable[position - 1] if position > 0 else None
        after = usable[position] if position < len(usable) else None

        # Linear: if the readings exist on both sides, the value is on the straight line between them. The fraction
        # says how far the moment is from the first reading to the second one (0 = on the first, 1 = on the second).
        if fill_operation == self._LINEAR and before is not None and after is not None:
            fraction = (moment - before[0]) / (after[0] - before[0])
            return before[1] + (after[1] - before[1]) * fraction

        # Forward fill, or linear with only one side: the value of the closest reading before the moment. If there is
        # none before (the gap is at the start), the closest one after it. At least one of them exists, because
        # 'usable' is not empty.
        return before[1] if before is not None else after[1]
