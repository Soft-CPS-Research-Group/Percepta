# --- Standard Library Imports ---
from datetime import datetime
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.utils.time_utils import parse_timestamp


class DuplicatedDataApproach(DataIssueHandlingApproachBase):
    """Removes the duplicated readings of each parameter.

    Two readings of the same parameter are duplicates when they have the same timestamp. They are duplicates even if
    their values are different. The timestamp used is the one given by the Telemetry System, which is assumed to be
    the time at which the value was produced.

    The approach is based on this idea: a Telemetry System cannot produce two different values for the same instant. So
    two readings with the same timestamp are the same reading that arrived twice.

    Only the readings received in the current period are compared. Readings of previous periods are not kept, so they
    are not compared. This is on purpose: a data stream never ends, and keeping everything to compare with new readings
    would need more and more memory.

    The lists 'values' have the readings in the order in which they arrived, with the most recent arrival at the end.
    When there are duplicates, the FIRST reading is kept and the ones that arrived after it are removed. Keeping the
    first was a choice, because either one could be right.

    The approach does not use the schema or the frequency, and it does not keep anything between periods.

    Example:
    --------
    Input:  [{"id": "id_01", "parameters": {"id_param_01": {"parameter_type_id": "parameter_type_1", "values": [
                {"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4},
                {"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.6}]}}, ...}]
    Output: [{"id": "id_01", "parameters": {"id_param_01": {"parameter_type_id": "parameter_type_1", "values": [
                {"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4}]}}, ...}]
    """

    _LOG_PREFIX: str = "Duplicated Data Approach |"

    def handle(self, data: List[Dict[str, Any]], period_end: datetime) -> List[Dict[str, Any]]:
        """Removes, from each parameter of each entity, the readings that repeat a timestamp.

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
                            {"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4},
                            {"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.6}
                        ]
                    }
                },
                "timestamp": "2026-01-01 10:15:00 +0000"
            }
        ]
        period_end = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)      # Not used by this approach

        Expected Output Structure:
        --------------------------
        The same structure. The second reading above is removed, because it has the same timestamp as the first:
        "values": [{"timestamp": "2026-01-01 10:15:00 +0000", "value": 3.4}]

        Args:
            data (List[Dict[str, Any]]): The data received in the period.
            period_end (datetime): The end of the period. This approach does not use it.

        Returns:
            List[Dict[str, Any]]: The data without duplicated readings. It is the same list that was received, changed.
        """
        for entity in data:
            for parameter_id, parameter in entity.get("parameters", {}).items():
                values = parameter.get("values")

                # A parameter with no readings has nothing to remove
                if not values:
                    continue

                unique_values = self._remove_duplicates(values)

                # Only says something if something was removed, so the log stays empty in the normal case
                removed = len(values) - len(unique_values)
                if removed:
                    self._logger.debug(
                        f"{self._LOG_PREFIX} Parameter '{parameter_id}' of entity '{entity.get('id')}' "
                        f"(parameter type: {parameter.get('parameter_type_id')}): removed {removed} duplicated reading(s)."
                    )

                parameter["values"] = unique_values

        return data

    @staticmethod
    def _remove_duplicates(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Returns the readings without the ones that repeat a timestamp, keeping the first of each timestamp.

        The timestamps are compared as moments in time, and not as text. So "2026-01-01 10:00:00 +0100" and
        "2026-01-01 09:00:00 +0000" are the same instant, and are duplicates, even if they are written differently.
        A timestamp that cannot be read is compared as text. This way, a badly written timestamp does not stop the
        other readings from being checked.

        Example:
        --------
        [{"timestamp": "T1", "value": 3.4}, {"timestamp": "T1", "value": 3.6}, {"timestamp": "T2", "value": 5.0}]
        -> [{"timestamp": "T1", "value": 3.4}, {"timestamp": "T2", "value": 5.0}]

        Args:
            values (List[Dict[str, Any]]): The readings of a parameter, in the order in which they arrived.

        Returns:
            List[Dict[str, Any]]: A new list with the readings that were kept, in the same order.
        """
        # The timestamps already seen. A set is used because it is fast to check if something is in it.
        seen_timestamps = set()
        unique_values = []

        for reading in values:
            timestamp = reading.get("timestamp")

            # The key used to compare this reading with the others. It is the moment in time if the timestamp can be
            # read. If not, it is the text itself (a text and a moment never match, so they are never mixed up).
            try:
                key = parse_timestamp(timestamp)
            except ValueError:
                key = timestamp

            # A reading whose timestamp was already seen is a duplicate, and is left out. Because the readings are
            # visited in order, the one that is kept is always the first of each timestamp.
            if key in seen_timestamps:
                continue

            seen_timestamps.add(key)
            unique_values.append(reading)

        return unique_values
