# --- Standard Library Imports ---
import datetime
from zoneinfo import ZoneInfo
from typing import Any, Callable, Optional

# --- Third-Party Imports ---
from apscheduler.schedulers.asyncio import AsyncIOScheduler

# --- Internal Framework Imports ---
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils


class AsyncPeriodicScheduler:
    """
    Infrastructure boundary adapter responsible for non-blocking periodic task execution.
    Uses an AsyncIOScheduler integrated into the active asyncio event loop.
    """

    def __init__(
            self,
            timezone: ZoneInfo,
            logger: LoggingUtils
    ) -> None:
        """
        Initializes the PeriodicScheduler context.
        """
        self._logger = logger
        self._tz = timezone

        # Instantiate the underlying Asynchronous Event Loop Scheduler engine
        self._scheduler = AsyncIOScheduler(timezone=str(timezone))

        # Own flag, because the engine only changes its state later (inside the event loop) after a shutdown,
        # so a second `stop` right after the first would still see it as running.
        self._is_running = False

    def create_job(
            self,
            time_interval: int,
            trigger_callback: Callable[[], Any],
            job_id: str = "periodic_window_job",
            kwargs: Optional[Any] = None,
    ) -> None:
        """
        Configures and registers a periodic execution window job into the scheduler.
        The trigger_callback can now be a native 'async def' coroutine function.
        Jobs can be created before or after `start`.

        Raises:
            ValueError: If a job with the same 'job_id' already exists.
        """
        # --- Guard Clause 1: the job id must be unique ---
        # Example: two environments creating "omie_R-H-01" -> the second one would replace the first
        if self._scheduler.get_job(job_id=job_id) is not None:
            error_msg = f"Scheduler | Job [{job_id}] already exists. Each job needs its own 'job_id'."
            self._logger.error(error_msg)
            raise ValueError(error_msg)

        cron_kwargs = DataSet.get_cron_expressions(time_interval)

        # Schedule the job in the async engine
        self._scheduler.add_job(
            trigger_callback,
            'cron',
            coalesce=True,
            id=job_id,
            **cron_kwargs
        )
        self._logger.info(f"Scheduler | Async Job [{job_id}] created successfully with an interval of {time_interval}s.")

    def get_next_run_time(self, job_id: str) -> Optional[datetime.datetime]:
        """
        Retrieves the next scheduled execution timestamp for a given job ID.
        """
        job = self._scheduler.get_job(job_id=job_id)

        if job and job.next_run_time:
            localized_next_run = job.next_run_time.astimezone(self._tz)
            self._logger.info(f"Scheduler | Next scheduled run for job [{job_id}] is: {localized_next_run}")
            return localized_next_run

        self._logger.warning(f"Scheduler | Job [{job_id}] not found or has no upcoming execution scheduled.")
        return None

    def start(self) -> None:
        """
        Starts the scheduler engine inside the active asyncio event loop.
        NOTE: This call is now NON-BLOCKING and returns immediately.
        """
        self._logger.info("Scheduler | Hooking tasks into the active asyncio Event Loop. Operation is non-blocking.")
        self._scheduler.start()
        self._is_running = True

    def stop(self) -> None:
        """Gracefully shuts down the scheduler engine from the active loop. Safe to call more than once."""
        # --- Guard Clause 1: only a running scheduler can be shut down ---
        # Example: stop() called twice, or called when `start` was never called -> nothing to do
        if not self._is_running:
            self._logger.debug("Scheduler | Not running. Nothing to shut down.")
            return

        self._is_running = False
        self._scheduler.shutdown()
        self._logger.info("Scheduler | Async loop tasks detached. Shut down gracefully.")