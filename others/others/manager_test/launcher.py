# launchers/manager_launcher.py
import datetime
from threading import Thread
from zoneinfo import ZoneInfo
from typing import List

from app.core.utils.logger import LoggingUtils
from app.core.repositories.irepositories.time_series_repository import TimeSeriesRepository
from app.start_app.launcher_base import LauncherBase

from app.manager_test.manager import Manager
from others.save.sync_rabbitMQ_input_handler import RabbitMQInputHandler
from app.core.utils.sync_periodic_scheduler import SyncPeriodicScheduler
from app.manager_test.data_quality_launcher import DataQualityLauncher


class ManagerLauncher(LauncherBase):
    """
    Composition Root for the Manager microservice module context.
    Coordinates component lifecycles, maps configurations, and orchestrates boundary execution flows.
    """

    def __init__(self, environment, environment_specs, time_series_repository: TimeSeriesRepository,
                 logger: LoggingUtils, configurations: dict):
        super().__init__(logger)

        self._environment = environment
        self._environment_specs = environment_specs

        self._time_interval = configurations.get('frequency', 60)
        timezone_str = configurations.get('timezone', 'UTC')

        # Resolve Timezone configuration safely
        self._tz = self._resolve_timezone(timezone_str)

        # Set up a specialized contextual logger for the inner execution engine
        manager_logger = LoggingUtils("manager", configurations, environment)

        # Build Inbound Pipeline asynchronously without polluting the launcher instance state
        data_quality_pipeline = self._build_data_quality_pipeline(
            time_series_repository, configurations, manager_logger
        )

        # Initialize Pure Domain Core Core Component
        self._manager = Manager(data_quality_pipeline, configurations, manager_logger)

        # Initialize Reactive Boundary Input Adapter
        self._rabbitmq_receiver = self._build_rabbitmq_receiver(configurations, manager_logger)

        # Initialize Proactive Boundary Input Adapter (Generic Time Clock Engine)
        self._periodic_scheduler = SyncPeriodicScheduler(self._tz, manager_logger)

        # TODO: Load output pipeline dynamically via OutputLauncher here later

    def _resolve_timezone(self, timezone_str: str) -> ZoneInfo:
        try:
            tz = ZoneInfo(timezone_str)
        except Exception:
            self._logger.warning(f"Launcher | Invalid timezone '{timezone_str}' in config. Falling back to UTC.")
            tz = ZoneInfo("UTC")

        return tz

    def _build_rabbitmq_receiver(self, configurations: dict, logger: LoggingUtils):
        server = configurations.get('internal_amqp_server').get('server')

        return RabbitMQInputHandler(self._environment, self._manager.new_message, server, logger)

    def _build_data_quality_pipeline(self, repository: TimeSeriesRepository, configs: dict,
                                     logger: LoggingUtils) -> list:
        """
        Private factory helper. Computes and returns the pipeline sequence
        without introducing mutating side-effects to the launcher instance.
        """
        # Extract path from configurations dictionary map safely
        pipeline_config_path = configs.get('pipeline_config_path', './configs/pipeline_config.yaml')

        # Instantiate the specialized data quality sub-factory
        dq_launcher = DataQualityLauncher(
            time_series_repository=repository,
            environment_specs=self._environment_specs,
            configurations=configs,
            logger=logger
        )

        # Load and return the processed validation handlers chain array
        return dq_launcher.load_handler_pipeline(pipeline_config_path)

    def launch(self) -> List[Thread]:
        """Wires up threading runtime boundaries and triggers execution intervals."""
        threads: List[Thread] = []

        # Worker Thread A: Inbound Streaming Messages Subscriber Loop
        rabbitmq_receiver = Thread(
            target=self._rabbitmq_receiver.start_messaging_service,
            daemon=True,
            name=f"mq_receiver_{self._environment}"
        )
        threads.append(rabbitmq_receiver)

        # Register the local job wrapper method as the clock execution target callback.
        # This architecture preserves scheduler reusability and keeps orchestrations here.
        job_id = 'process_window'
        self._periodic_scheduler.create_job(
            time_interval=self._time_interval,
            trigger_callback=self._job,
            job_id=job_id
        )

        # Worker Thread B: Proactive Background Clock Loop Trigger
        periodic_scheduler = Thread(
            target=self._periodic_scheduler.start,
            daemon=True,
            name=f"scheduler_clock_{self._environment}"
        )
        threads.append(periodic_scheduler)

        # Warm up system logs with upcoming state telemetry updates
        self._periodic_scheduler.get_next_run_time(job_id)

        # Todo estas threads serão registadas no Fine Grained WatchDog
        return threads

    def stop(self) -> None:
        """
        Orchestrates a graceful shutdown sequence.
        Safely stops the clock engine and tears down active network communication sockets.
        """
        self._logger.info(f"Launcher | Initiating graceful shutdown for environment context: {self._environment}")

        # 1. Terminate the periodic scheduler loop first to prevent triggering new intervals
        try:
            self._periodic_scheduler.stop()
            self._logger.info("Launcher | Periodic scheduler engine stopped successfully.")
        except Exception as e:
            self._logger.error(f"Launcher | Failed to stop periodic scheduler cleanly: {e}")

        # 2. Halt message consumption and close down RabbitMQ TCP channels securely
        try:
            self._rabbitmq_receiver.stop()
            self._logger.info("Launcher | RabbitMQ receiver infrastructure dismantled cleanly.")
        except Exception as e:
            self._logger.error(f"Launcher | Failed to stop RabbitMQ receiver cleanly: {e}")

    def _job(self) -> None:
        """
        Orchestration workflow callback triggered periodically by the generic Scheduler.
        Aligns target processing window timestamps, executes domain processing,
        and routes resulting messages downstream to the egress pipeline.
        """
        next_run_time = self._periodic_scheduler.get_next_run_time("process_window")

        if next_run_time is not None:
            next_run_tz = next_run_time.astimezone(self._tz)
            run_time = next_run_tz - datetime.timedelta(seconds=self._time_interval)
        else:
            # Fallback to current clock time if job metadata is temporarily inaccessible
            run_time = datetime.datetime.now(self._tz)

        # 1. Pull processed clean batch snapshot payload from Domain Core
        processed_data = self._manager.process_window(run_time) # Todo oq fazer com a resposta
















# --- app/core/application_core.py ---
import logging
import sys
from typing import List, Any


class ApplicationCore:
    """
    The Main Microservice Orchestrator. It acts as the root execution lifecycle manager,
    holding references to running logical handlers, threads, and infrastructure launchers.
    """

    def __init__(self, launcher: Any, thread_manager: Any, logger: logging.Logger) -> None:
        self._launcher = launcher  # O teu InfrastructureLauncher
        self._thread_manager = thread_manager  # O teu outro sistema de gestão de threads
        self._logger = logger

        # Listas para guardar o que foi criado no arranque
        self._active_output_handlers: List[Any] = []
        self._active_input_handlers: List[Any] = []

    def bootstrap(self) -> None:
        """Starts up the microservice nodes."""
        self._logger.info("Core | Bootstrapping application components...")

        # 1. Ativa as fábricas do Launcher
        output_handler, rpc_input_handler = self._launcher._build_rabbitmq_output_handler(...)

        self._active_output_handlers.append(output_handler)
        if rpc_input_handler:
            self._active_input_handlers.append(rpc_input_handler)

            # 2. Passa o input handler para o gestor de threads criar e arrancar a Thread!
            self._thread_manager.register_and_start_consumer(rpc_input_handler)

    # ◄── O MANTENEDOR DO CICLO DE VIDA: Quem aciona a destruição
    def shutdown(self) -> None:
        """
        Graceful shutdown orchestrator sequence triggered upon termination signals.
        It commands component layers to close down sequentially from top to bottom.
        """
        self._logger.info("Core | Initiating graceful shutdown chain...")

        # =========================================================================
        # PASSO 1: Chamar o stop() LÓGICO de todos os Handlers ativos
        # =========================================================================
        self._logger.info("Core | Phase 1: Flushing logical handler boundaries...")

        # Aqui é chamado o stop() do teu RabbitMQOutputHandler!
        for out_handler in self._active_output_handlers:
            out_handler.stop()

            # Aqui é chamado o stop() do teu RabbitMQInputHandler!
        for in_handler in self._active_input_handlers:
            in_handler.stop()

        # =========================================================================
        # PASSO 2: Mandar o Gestor de Threads parar a concorrência do SO
        # =========================================================================
        self._logger.info("Core | Phase 2: Terminating background thread execution contexts...")
        # O teu sistema de gestão de threads faz o join() de forma centralizada
        self._thread_manager.stop_all_monitored_threads()

        # =========================================================================
        # PASSO 3: Mandar o Launcher fechar as ligações físicas na tomada
        # =========================================================================
        self._logger.info("Core | Phase 3: Severing physical infrastructure connectors...")
        # O Launcher fecha os connectors que guardou no self._infrastructure_connectors
        self._launcher.shutdown_infrastructure()

        self._logger.info("Core | Graceful shutdown sequence finalized cleanly. Exiting process.")
        sys.exit(0)







        # --- app/main.py ---
        import logging
        import signal
        from app.launchers.infrastructure_launcher import InfrastructureLauncher
        from app.core.application_core import ApplicationCore

        if __name__ == "__main__":
            logger = logging.getLogger("PerceptaLogger")

            # 1. Instancia os componentes de suporte
            launcher = InfrastructureLauncher(logger)
            thread_manager = MyCustomThreadManager(logger)  # O teu sistema de threads

            # 2. Instancia o Orquestrador Raiz
            app_core = ApplicationCore(launcher, thread_manager, logger)

            # 3. CAPTURA DE SINAIS DO SISTEMA OPERATIVO:
            # Se o Linux mandar um SIGINT (Ctrl+C) ou SIGTERM (Kubernetes Kill), chama o app_core.shutdown
            signal.signal(signal.SIGINT, lambda signum, frame: app_core.shutdown())
            signal.signal(signal.SIGTERM, lambda signum, frame: app_core.shutdown())

            # 4. Arranca o barco
            app_core.bootstrap()

            # Mantém a app viva
            signal.pause()