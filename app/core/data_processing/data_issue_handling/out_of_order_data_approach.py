# --- Standard Library Imports ---
from datetime import datetime
from typing import Any, Dict, List

# --- Internal Framework Imports ---
from app.core.data_processing.data_issue_handling.data_issue_handling_approach_base import DataIssueHandlingApproachBase
from app.core.utils.time_utils import parse_timestamp


class OutOfOrderDataApproach(DataIssueHandlingApproachBase):
    """Sorts the readings of each parameter by timestamp, from the oldest to the most recent.

    The readings arrive in the lists 'values' in the order in which they were received, which is not always the order
    in which they were produced. This approach puts them in chronological order, so that the approaches that come after
    it can rely on the order.

    This approach only sorts. It does not remove readings, even the ones that arrived late and are older than the
    current period. Those readings may still be useful to the approaches that come after, such as Frequency
    Harmonisation. Whether to keep them or not is decided by those approaches.

    The approach does not use the schema or the frequency, and it does not keep anything between periods.

    Example:
    --------
    Input:  {"id_01": {"parameters": {"id_param_01": {"parameter_type_id": "parameter_type_1", "values": [
                {"timestamp": "2026-01-01 10:10:00 +0000", "value": 5.0},
                {"timestamp": "2026-01-01 10:05:00 +0000", "value": 3.0},
                {"timestamp": "2026-01-01 10:15:00 +0000", "value": 7.5}]}}, ...}}
    Output: {"id_01": {"parameters": {"id_param_01": {"parameter_type_id": "parameter_type_1", "values": [
                {"timestamp": "2026-01-01 10:05:00 +0000", "value": 3.0},
                {"timestamp": "2026-01-01 10:10:00 +0000", "value": 5.0},
                {"timestamp": "2026-01-01 10:15:00 +0000", "value": 7.5}]}}, ...}}
    """

    _LOG_PREFIX: str = "Out-of-Order Data Approach |"

    def handle(self, data: Dict[str, Dict[str, Any]], period_end: datetime) -> Dict[str, Dict[str, Any]]:
        """Sorts the readings of each parameter of each entity by timestamp.

        Expected Input Structure:
        -------------------------
        data = {
            "id_01": {
                "entity_type_id": "entity_type_1",
                "parameters": {
                    "id_param_01": {
                        "parameter_type_id": "parameter_type_1",
                        "values": [
                            {"timestamp": "2026-01-01 10:10:00 +0000", "value": 5.0},
                            {"timestamp": "2026-01-01 10:05:00 +0000", "value": 3.0}
                        ]
                    }
                },
                "timestamp": "2026-01-01 10:10:00 +0000"
            }
        }
        period_end = datetime(2026, 1, 1, 10, 15, 0, tzinfo=timezone.utc)      # Not used by this approach

        Expected Output Structure:
        --------------------------
        The same structure, with each list 'values' sorted from the oldest to the most recent timestamp:
        "values": [{"timestamp": "2026-01-01 10:05:00 +0000", "value": 3.0},
                   {"timestamp": "2026-01-01 10:10:00 +0000", "value": 5.0}]

        Args:
            data (Dict[str, Dict[str, Any]]): The data received in the period.
            period_end (datetime): The end of the period. This approach does not use it.

        Returns:
            Dict[str, Dict[str, Any]]: The data with the readings sorted. It is the same dictionary that was received, changed.
        """
        for entity in data.values():
            for parameter in entity.get("parameters", {}).values():
                values = parameter.get("values")

                # A parameter with no readings has nothing to sort
                if not values:
                    continue

                parameter["values"] = self._sort_by_timestamp(values)

        return data

    @staticmethod
    def _sort_by_timestamp(values: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Returns the readings sorted from the oldest to the most recent timestamp.

        The timestamps are compared as moments in time, and not as text. A text would give a wrong order when the
        timestamps have different offsets (e.g., "10:00:00 +0100" is earlier than "09:30:00 +0000", but a text
        comparison would say the opposite).

        A reading whose timestamp cannot be read (it is missing or badly written) cannot be placed in time. These
        readings are put at the end, in the order in which they arrived, so that none is lost and one bad timestamp
        does not stop the others from being sorted.

        Readings with the same timestamp keep the order in which they arrived.

        Example:
        --------
        [{"timestamp": "T2", ...}, {"timestamp": "bad", ...}, {"timestamp": "T1", ...}]  (T1 is earlier than T2)
        -> [{"timestamp": "T1", ...}, {"timestamp": "T2", ...}, {"timestamp": "bad", ...}}

        Args:
            values (List[Dict[str, Any]]): The readings of a parameter, in the order in which they arrived.

        Returns:
            List[Dict[str, Any]]: A new list with the same readings, sorted.
        """
        # Readings that can be placed in time, each one with its moment, and the ones that cannot
        readable = []
        unreadable = []

        for reading in values:
            try:
                readable.append((parse_timestamp(reading["timestamp"]), reading))
            except (KeyError, ValueError):
                unreadable.append(reading)

        # Sorts only by the moment ('key'), so that two readings with the same moment are never compared with each
        # other (dictionaries cannot be compared)
        readable.sort(key=lambda pair: pair[0])

        return [reading for _, reading in readable] + unreadable
