from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
from typing import Optional
from app.core.utils.logger import LoggingUtils


class TimeZoneUtils:
    """
    Utility class for resolving and instantiating ZoneInfo timezone objects.
    """

    @staticmethod
    def resolve_timezone(
        tz_name: Optional[str] = "UTC",
        logger: Optional[LoggingUtils] = None,
        log_prefix: str = "Timezone Resolver |" # TODO SECALHAAR NÃO PASSO ASSIM VER O QUE FAZER
    ) -> ZoneInfo:
        """
        Safely resolves a string timezone identifier into a ZoneInfo instance.
        Falls back gracefully to 'UTC' if the timezone name is invalid or missing.

        Args:
            tz_name (Optional[str]): String identifier of the target timezone (e.g., 'Europe/Lisbon').
                                     Defaults to 'UTC' if None or omitted.
            logger (Optional[LoggingUtils]): Optional logger instance to log warnings on fallback.
            log_prefix (str): Prefix string attached to warning log entries.

        Returns:
            ZoneInfo: Validated ZoneInfo instance (target timezone or UTC fallback).
        """
        target_tz = tz_name if tz_name else "UTC"

        try:
            return ZoneInfo(target_tz)
        except (ZoneInfoNotFoundError, Exception) as tz_err:
            if logger:
                logger.warning(
                    f"{log_prefix} Invalid timezone '{target_tz}' provided ({tz_err}). Falling back to 'UTC'."
                )
            return ZoneInfo("UTC")