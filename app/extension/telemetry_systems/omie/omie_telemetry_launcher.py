# --- Standard Library Imports ---
import asyncio
from typing import Callable, Optional
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.connectors.async_local_connector import AsyncLocalConnector
from app.core.input_handlers.async_local_input_handler import AsyncLocalInputHandler
from app.core.data_ingestion.telemetry_system_launcher_base import (
    TelemetrySystemComponents,
    TelemetrySystemLauncherBase
)
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.data import DataSet
from app.core.utils.logger import LoggingUtils
from app.extension.telemetry_systems.omie.omie_spot_price_service import OmieSpotPriceService
from app.extension.telemetry_systems.omie.omie_telemetry_adapter import OMIETelemetryAdapter


class OMIETelemetryLauncher(TelemetrySystemLauncherBase):
    """Launcher of the OMIE Telemetry System (day-ahead electricity spot price).

    The prices are the same for every environment, so the launcher keeps ONE `OmieSpotPriceService` and ONE
    scheduler, shared by all of them. For each environment, it returns the OMIE Adapter and a local Input
    Handler, and adds a job to the scheduler. At every time interval, the job reads the prices from the service
    and gives them to the Input Handler of that environment.

    How it differs from the other Telemetry Systems:
    - In the others, the Input Handler brings the data from outside the application: HTTP requests to an API
      (which need an Endpoint Builder to create the requests) or messages from RabbitMQ.
    - In OMIE, the data is already inside the application, in the service, which requests the prices from
      OMIE once for the whole system. So there is nothing to request or to subscribe to, and no Endpoint
      Builder is needed. The Input Handler is a local one: it only receives the prices that the job reads
      from the service.
    """

    _LOG_PREFIX: str = "OMIE Telemetry Launcher |"
    _SOURCE: str = "omie_spot_price"           # 'source' of the envelopes created by the Input Handlers
    _DEFAULT_MAX_QUEUE_SIZE: int = 10000       # Same default as AsyncLocalConnector

    def __init__(self, input_launcher, output_launcher, logger) -> None:
        """Initializes the OMIE launcher. The service and the scheduler are created on the first `launch`.

        Args:
            input_launcher (InputHandlersLauncher): Factory of Input Handlers (not used by this launcher).
            output_launcher (OutputHandlersLauncher): Factory of Output Handlers (not used by this launcher).
            logger (LoggingUtils): Logging interface, used for the service and the scheduler, which belong to
                the whole Telemetry System and not to one environment.
        """
        super().__init__(input_launcher, output_launcher, logger)
        self._service: Optional[OmieSpotPriceService] = None
        self._scheduler: Optional[AsyncPeriodicScheduler] = None

        # Environments may be launched at the same time, and the service must be created only once
        self._lock = asyncio.Lock()

    async def launch(
        self,
        telemetry_system_id: str,
        connection_configuration: dict,
        environment_name: str,
        environment_specs: dict,
        source_mapping: dict,
        settings: dict,
        callback: Callable,
        logger: LoggingUtils
    ) -> TelemetrySystemComponents:
        """Prepares the OMIE Adapter and Input Handler for one environment.

        Expected `connection_configuration` Data Structure:
        ---------------------------------------------------
        {
            "protocol_id": "AsyncLocal",                # The prices are read from memory, not from the network
            "connection_info": {                        # (dict, optional) Every key is optional
                "market_timezone": "Europe/Madrid",     # Timezone of the OMIE market days
                "publication_time": "13:00",            # "HH:MM" (market timezone) after which tomorrow's prices are fetched
                "refresh_interval_seconds": 600,        # How often the service checks for missing prices
                "retention_seconds": 7200,              # How long past prices are kept
                "max_queue_size": 10000                 # Messages buffered for each environment
            }
        }

        These values only matter on the first launch, because the service is created then and shared.

        Expected `source_mapping` Data Structure:
        -----------------------------------------
        {
            # Parameter id -> series of the service that holds its prices ('pt_spot_price' or 'es_spot_price').
            # There is no 'fetch_key': nothing is requested for each parameter.
            "em01_price": {"match_key": "pt_spot_price"}
        }

        Args:
            telemetry_system_id (str): Id of the Telemetry System.
            connection_configuration (dict): How the service works, as detailed above.
            environment_name (str): Name of the environment (e.g., "R-H-03").
            environment_specs (dict): Schema of the environment, with only the parameters handled by OMIE.
            source_mapping (dict): Source mapping of OMIE for this environment.
            settings (dict): Instance settings ("timezone" and "frequency"). The frequency is how often
                the prices are sent to the pipeline.
            callback (Callable): Function the Input Handler calls with each message it receives.
            logger (LoggingUtils): Logger of the environment, given to the Adapter and the Input Handler,
                so their logs show the environment.

        Returns:
            TelemetrySystemComponents: The Adapter and the Input Handler (not started).

        Raises:
            ValueError: If the connection configuration or the source mapping is invalid.
        """
        connection_info = connection_configuration.get("connection_info", {})

        # --- Guard Clause 1: connection_info, when present, must be a dictionary ---
        if not isinstance(connection_info, dict):
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: 'connection_info' must be a dictionary for "
                f"telemetry system '{telemetry_system_id}'."
            )

        # --- Guard Clause 2: the source mapping needs at least one rule ---
        rules = source_mapping
        if not isinstance(rules, dict) or not rules:
            raise ValueError(
                f"{self._LOG_PREFIX} Launch error: missing or empty source mapping "
                f"for environment '{environment_name}'."
            )

        timezone = settings.get("timezone", "UTC")
        time_interval = DataSet.calculate_interval(settings.get("frequency"))

        # The Adapter is created first: if its configuration is wrong, it fails before anything is started
        adapter = OMIETelemetryAdapter(
            environment=environment_name,
            environment_specs=environment_specs,
            source_mapping_rules=rules,
            logger=logger,
            timezone=timezone
        )

        # The service and the scheduler are shared by all the environments
        async with self._lock:
            await self._ensure_started(connection_info, timezone)

        # Messages are not delivered directly: they are pushed into a queue, and the Input Handler takes them from
        # it and gives them to the callback when it is started. So nothing is lost if a job runs before that.
        connector = AsyncLocalConnector(
            {"max_queue_size": connection_info.get("max_queue_size", self._DEFAULT_MAX_QUEUE_SIZE)},
            logger
        )
        input_handler = AsyncLocalInputHandler(
            connector=connector,
            on_message_callback=callback,
            source=self._SOURCE,
            logger=logger
        )

        async def send_prices() -> None:
            """Reads the prices from the service and pushes them to the Input Handler of this environment."""
            # The Input Handler was stopped (for example, the launch of this environment failed afterwards), so
            # nobody reads this queue any more. Nothing is pushed, instead of filling it and logging at every run.
            if not connector.is_connected():
                return

            try:
                # Example: {"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0, ...}, "es_spot_price": {...}}
                payload = self._service.get_future_prices_payload()

                # No prices known yet (for example, OMIE is not reachable): there is nothing to send
                if not payload:
                    logger.debug(f"{self._LOG_PREFIX} No prices to send to environment '{environment_name}'.")
                    return

                input_handler.on_message(payload)
            except Exception as err:
                # A failure here must not stop the job: it will run again at the next time interval
                logger.error(f"{self._LOG_PREFIX} Failed to send the prices to '{environment_name}': {err}")

        # The job runs at fixed times of the clock (for example, every 15 minutes: :00, :15, :30 and :45), so the
        # first run could be up to one interval after the start. The prices are sent once now, so the pipeline
        # has them from the start.
        await send_prices()

        self._scheduler.create_job(
            time_interval=time_interval,
            trigger_callback=send_prices,
            job_id=f"omie_prices_{environment_name}"
        )

        return TelemetrySystemComponents(adapter=adapter, input_handler=input_handler)

    async def _ensure_started(self, connection_info: dict, timezone: str) -> None:
        """Creates and starts the service and the scheduler, if this is the first launch.

        Either both are started or none is: if anything fails, what was already started is stopped and the
        error is raised, so the next environment tries again from the beginning.

        Args:
            connection_info (dict): Options of the service, as detailed in `launch`.
            timezone (str): Timezone of the scheduler (the instance timezone).

        Raises:
            ValueError: If the timezone or an option of the service is invalid.
        """
        # --- Guard Clause 1: already started by a previous environment ---
        if self._service is not None and self._scheduler is not None:
            return

        # Both are only created here (nothing is started or requested yet), so a wrong timezone or option
        # fails before any request is made to OMIE.
        # Example: "Europe/Lisbonn" (typo) or "publication_time": "13h" -> ValueError, nothing is left running
        try:
            scheduler = AsyncPeriodicScheduler(timezone=ZoneInfo(timezone), logger=self._logger)
        except Exception as err:
            raise ValueError(f"{self._LOG_PREFIX} Launch error: invalid timezone '{timezone}': {err}") from err

        options = {
            key: connection_info[key]
            for key in ("market_timezone", "publication_time", "refresh_interval_seconds", "retention_seconds")
            if key in connection_info
        }
        service = OmieSpotPriceService(logger=self._logger, **options)

        try:
            # Fetches the prices available now and starts the service's own refresh job.
            # If OMIE does not answer, this does NOT fail: the service logs a warning and tries again in the
            # next run, so the environment starts without prices and receives them as soon as they exist.
            await service.start()
            scheduler.start()
        except Exception:
            # Stop what was started (both are safe to stop even if they did not start) and let the error go up
            scheduler.stop()
            await service.stop()
            raise

        self._service = service
        self._scheduler = scheduler
        self._logger.debug(f"{self._LOG_PREFIX} OMIE service and scheduler started.")

    async def stop(self) -> None:
        """Stops the scheduler first (so nothing reads from the service any more), then the service."""
        if self._scheduler:
            self._scheduler.stop()
            self._scheduler = None

        if self._service:
            await self._service.stop()
            self._service = None