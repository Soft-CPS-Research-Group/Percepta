import datetime
from zoneinfo import ZoneInfo
from typing import Any, Callable, Optional
from apscheduler.schedulers.blocking import BlockingScheduler
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils


class SyncPeriodicScheduler:
    """
    Infrastructure boundary adapter responsible for periodic task execution.
    Uses a BlockingScheduler to keep the main execution thread alive.
    It triggers an injected processing function at a configured frequency and
    routes the resulting processed data to an external output pipeline.
    """

    def __init__(
            self,
            timezone: ZoneInfo,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the PeriodicScheduler context.

        Args:
            timezone (ZoneInfo): Target timezone name (e.g., 'UTC', 'Europe/London').
            logger (LoggingUtils): Logger instance for operational tracking.
        """
        self._logger = logger

        # Set up the execution timezone
        self._tz = timezone

        # Initialize the underlying BlockingScheduler engine
        self._scheduler = BlockingScheduler()

    def create_job(
            self,
            time_interval: int,
            trigger_callback: Callable[[], Any],
            job_id: str = "periodic_window_job",
            kwargs: Optional[Any] = None,
    ) -> None:
        """
        Configures and registers a periodic execution window job into the scheduler.

        Args:
            time_interval (int): Execution frequency in seconds.
            trigger_callback (Callable): Injected function to execute periodically. Receives a datetime and returns data.
            job_id (str): Unique identifier for the scheduled job.
        """
        cron_kwargs = DataSet.get_cron_expressions(time_interval)

        # Schedule the job in the blocking engine
        self._scheduler.add_job(
            trigger_callback,
            'cron',
            coalesce=True,
            id=job_id,
            **cron_kwargs
        )
        self._scheduler.configure(wakeup_interval=0.1)
        self._logger.debug(f"Scheduler | Job [{job_id}] created successfully with an interval of {time_interval}s.")

    def get_next_run_time(self, job_id: str) -> Optional[datetime.datetime]:
        """
        Retrieves the next scheduled execution timestamp for a given job ID.

        Args:
            job_id (str): The unique identifier of the scheduled job.

        Returns:
            Optional[datetime.datetime]: The localized next run time, or None if the job does not exist.
        """
        job = self._scheduler.get_job(job_id=job_id)

        if job and job.next_run_time:
            # Localize the next run time to the scheduler's configured timezone
            localized_next_run = job.next_run_time.astimezone(self._tz)
            self._logger.debug(f"Scheduler | Next scheduled run for job [{job_id}] is: {localized_next_run}")
            return localized_next_run

        self._logger.warning(f"Scheduler | Job [{job_id}] not found or has no upcoming execution scheduled.")
        return None

    def start(self) -> None:
        """
        Starts the scheduler engine.
        WARNING: This call is BLOCKING and will halt the main thread.
        """
        self._logger.debug("Scheduler | Entering blocking loop. Main thread is now managed by the scheduler.")
        self._scheduler.start()

    def stop(self) -> None:
        """Gracefully shuts down the scheduler engine."""
        self._scheduler.shutdown()
        self._logger.debug("Scheduler | Blocked loop released. Shut down gracefully.")