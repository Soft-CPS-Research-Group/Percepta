# --- Standard Library Imports ---
import asyncio
import signal
import sys
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

# --- Internal Framework Imports ---
from app.core.data_issue_handling.data_issue_handling_approaches_launcher import DataIssueHandlingApproachesLauncher
from app.core.input_handlers.input_handlers_launcher import InputHandlersLauncher
from app.core.launchers.containers_launchers.data_ingestion_launcher import DataIngestionLauncher
from app.core.launchers.containers_launchers.data_processing_launcher import DataProcessingLauncher
from app.core.launchers.containers_launchers.decision_dispatching_launcher import DecisionDispatchingLauncher
from app.core.launchers.containers_launchers.inference_launcher import InferenceLauncher
from app.core.output_handlers.output_handlers_launcher import OutputHandlersLauncher
from app.core.storages.runtime_data_storage.runtime_data_storage_launcher import RuntimeDataStorageLauncher
from app.core.storages.schema_storage.schema_storage_launcher import SchemaStorageLauncher
from app.core.utils.event_loop_thread import EventLoopThread
from app.core.utils.instance_configuration import InstanceConfiguration
from app.core.utils.logger import LoggingUtils


@dataclass
class _Container:
    """A container of one environment: its launcher and the thread where it runs."""
    launcher: Any
    thread: EventLoopThread


class SingleProcessApplicationLauncher:
    """Launcher of the whole Percepta application.

    This Application Launcher launches all the containers in the SAME process, but each container of each environment
    runs in its OWN thread, with its own asyncio event loop (see `EventLoopThread`). For example, with the environments
    R-H-01 and R-H-02, there are eight threads: Data Ingestion, Data Processing, Inference and Decision Dispatching of
    R-H-01, and the same four of R-H-02. The main thread only launches and stops them, and waits for the signal to stop.

    Because an event loop and everything created in it belongs to its thread, nothing is shared between the threads:
    each container of each environment has its own launcher, and so its own Input and Output Handlers, connections,
    schedulers, Runtime Data Storage and (for Cleanwatts) its own login session. The containers talk to each other
    only through the network, so every link of 'inter_container_communication' must use a network protocol (e.g.,
    "AsyncRabbitMQ"). The "AsyncLocal" protocol, which calls the next container in memory, only works inside one event
    loop, so it is refused.

    It launches the Data Ingestion, Data Processing, Inference and Decision Dispatching containers. It does the following:
    1. Reads the Percepta Instance Configuration.
    2. Mounts the Schema Storage configured in it and gets the environments from the storage.
    3. Creates, for each environment, the launchers of the four containers and the thread of each one.
    4. Launches the environments one at a time. In each, the containers are launched from the last to the first
       (Decision Dispatching, Inference, Data Processing and Data Ingestion), so that the consumers of a link exist
       before the container that sends to them starts. Each container is launched inside its own thread.
    5. Keeps the application running until it is stopped.

    When another container is added, it is launched before the one that comes before it in the pipeline.
    """

    _LOG_PREFIX: str = "Application Launcher |"
    _DEFAULT_LOG_MAX_SIZE: int = 10 * 1024 * 1024     # Bytes. Used if 'logging.log_files.max_size' is not configured

    # From the last container of the pipeline to the first. It is the launch order, and the stop order is the reverse.
    _LAUNCH_ORDER: Tuple[str, ...] = ("decision_dispatching", "inference", "data_processing", "data_ingestion")

    _LINKS: Tuple[str, ...] = (
        "data_ingestion_to_data_processing", "data_processing_to_inference", "inference_to_decision_dispatching"
    )

    def __init__(self, instance_configuration_path: str) -> None:
        """Reads the configuration, mounts the Schema Storage and prepares the launchers of every environment.

        The format of the configuration is validated by InstanceConfiguration (see its docstring for the whole
        structure). The parts used here are:
        -----------------------------------------------------------------------------------------------------
        {
            "logging": {"log_files": {"max_size": 1048576}},    # (dict, optional) Settings of the logger
            "schema_storage": {
                "connection_configuration": {
                    "schema_storage_type_id": "json",           # Id of a Schema Storage type known to Percepta
                    "connection_info": {"file_path": "environment_files/environments.json"}
                }
            },
            "inter_container_communication": {                  # Every link must use a network protocol
                "data_ingestion_to_data_processing": {"connection_configuration": {"protocol_id": "AsyncRabbitMQ", ...}},
                "data_processing_to_inference": {"connection_configuration": {"protocol_id": "AsyncRabbitMQ", ...}},
                "inference_to_decision_dispatching": {"connection_configuration": {"protocol_id": "AsyncRabbitMQ", ...}}
            }
        }

        Args:
            instance_configuration_path (str): Path to the Percepta Instance Configuration (JSON).

        Raises:
            FileNotFoundError: If the configuration file (or the file of a JSON Schema Storage) does not exist.
            ValueError: If the configuration is not valid JSON or is not well formed (see InstanceConfiguration),
                a link between containers uses "AsyncLocal", or the Schema Storage cannot be built or has no
                environments.
        """
        # Reads the configuration and validates its general format. The specific format of each 'connection_info'
        # is not validated here: it is validated by who uses it (Input Handlers, Output Handlers and Storages).
        self._instance_configuration = InstanceConfiguration(instance_configuration_path)

        self._logging_configuration = self._resolve_logging_configuration()
        self._logger = LoggingUtils("application_launcher", self._logging_configuration)

        # Guard Clause 1: Checks that no link between containers is in memory. The containers run in different threads
        # (each with its own event loop), and a call from one loop to an object of another is not safe.
        for link in self._LINKS:
            link_configuration = getattr(self._instance_configuration, link)
            if link_configuration is not None and link_configuration.get("protocol_id") == "AsyncLocal":
                raise ValueError(
                    f"{self._LOG_PREFIX} Initialization error: 'inter_container_communication.{link}' uses "
                    f"'AsyncLocal', which only works inside one event loop. Each container runs in its own thread, "
                    f"so use a network protocol (e.g., 'AsyncRabbitMQ')."
                )

        # The storage is mounted here, and the environments are read from it
        schema_storage_configuration = self._instance_configuration.schema_storage
        schema_storage = SchemaStorageLauncher(self._logger).build_schema_storage(
            schema_storage_type_id=schema_storage_configuration["schema_storage_type_id"],
            connection_info=schema_storage_configuration["connection_info"],
            logger=self._logger
        )
        self._environments = schema_storage.get_environments()

        # Guard Clause 2: Checks that there is something to launch.
        if not self._environments:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: the Schema Storage has no environments.")

        # Each container of each environment has its own launcher and its own thread (not started yet). The launchers
        # are created here, and not at launch, so that a wrong configuration is found when the application starts.
        self._containers: Dict[str, Dict[str, _Container]] = {
            name: self._create_containers(name) for name in self._environments
        }

        self._launched_environments: List[str] = []

    def _create_containers(self, environment_name: str) -> Dict[str, _Container]:
        """Creates the launchers of the four containers of an environment and the thread of each one.

        Args:
            environment_name (str): Name of the environment (used in the names of the threads).

        Returns:
            Dict[str, _Container]: The containers of the environment by name ("data_ingestion", "data_processing",
                "inference" and "decision_dispatching").
        """
        launchers = {
            "data_ingestion": DataIngestionLauncher(
                instance_configuration=self._instance_configuration,
                input_launcher=InputHandlersLauncher(self._logger),
                output_launcher=OutputHandlersLauncher(self._logger),
                logger=self._logger
            ),
            "data_processing": DataProcessingLauncher(
                instance_configuration=self._instance_configuration,
                input_launcher=InputHandlersLauncher(self._logger),
                output_launcher=OutputHandlersLauncher(self._logger),
                approaches_launcher=DataIssueHandlingApproachesLauncher(self._logger),
                logger=self._logger
            ),
            "decision_dispatching": DecisionDispatchingLauncher(
                instance_configuration=self._instance_configuration,
                input_launcher=InputHandlersLauncher(self._logger),
                output_launcher=OutputHandlersLauncher(self._logger),
                logger=self._logger
            ),
            "inference": InferenceLauncher(
                instance_configuration=self._instance_configuration,
                input_launcher=InputHandlersLauncher(self._logger),
                output_launcher=OutputHandlersLauncher(self._logger),
                runtime_data_storage_launcher=RuntimeDataStorageLauncher(self._logger),
                logger=self._logger
            )
        }
        return {
            container_name: _Container(launcher, EventLoopThread(f"{container_name}_{environment_name}"))
            for container_name, launcher in launchers.items()
        }

    async def launch(self) -> None:
        """Launches all the environments of the Schema Storage, one at a time.

        Each container is launched inside its own thread, so that everything it creates (connections, tasks,
        schedulers) belongs to the event loop of that thread. If an environment fails, everything already started is
        stopped and the error is raised again.

        Raises:
            ValueError: If an environment has no Telemetry Systems.
        """
        names = list(self._environments)

        # Environments that were not launched yet. The first one is the one being launched, so it is the one
        # that failed if there is an error
        pending = list(names)

        try:
            for name in names:
                # The containers are launched from the last to the first. The links are over the network, so no
                # container needs a method of the next one: it is enough that the consumers of a link exist (and have
                # their queue) before the container that sends to them starts.
                for container_name in self._LAUNCH_ORDER:
                    container = self._containers[name][container_name]

                    # Each container has its own logger, so every log line shows which environment it belongs to
                    container_logger = LoggingUtils(
                        component_name=container_name,
                        configurations=self._logging_configuration,
                        environment=name
                    )

                    container.thread.start()

                    # The 'await' runs 'launch_environment' in the thread of the container and waits until it returns.
                    # What keeps running after it returns is what it started inside: the Input Handlers register their
                    # own asyncio tasks or consumers (e.g., the consumer of a RabbitMQ queue, the periodic requests),
                    # and those stay alive in the event loop of that thread.
                    await container.thread.run(container.launcher.launch_environment(
                        environment_name=name,
                        environment=self._environments[name],
                        logger=container_logger
                    ))

                # Remembers what was launched (it is cleared when the application stops)
                self._launched_environments.append(name)
                pending.remove(name)
                self._logger.info(f"{self._LOG_PREFIX} Environment '{name}' launched.")
        except Exception as err:
            # If one environment fails, none is left running: everything already started (even the environments that
            # were launched before this one) is stopped, and then the error is raised again so that the application
            # does not continue as if everything was fine.
            # Says what happened to each environment. It must be done before 'stop()', which clears the list.
            # The first pending environment is the one that failed, and the others were never tried.
            # Example: names = [A, B, C, D], B fails -> launched: [A], pending: [B, C, D] -> failed: [B], not tried: [C, D]
            failed, not_tried = pending[:1], pending[1:]
            self._logger.error(
                f"{self._LOG_PREFIX} Failed to launch the environments: {err}. "
                f"Launched: {self._launched_environments}. Failed: {failed}. Not tried: {not_tried}. "
                f"Stopping everything that was started."
            )
            await self.stop()
            raise

    async def stop(self) -> None:
        """Stops everything that was started, from the first container to the last (so that nothing is sent to a
        container that has already stopped): Data Ingestion, Data Processing, Inference and then Decision
        Dispatching. Each container is stopped inside its own thread, and the thread is closed at the end."""
        for container_name in reversed(self._LAUNCH_ORDER):
            for name, containers in self._containers.items():
                container = containers[container_name]

                # Guard Clause: Only the containers that were started have something to stop.
                if not container.thread.is_running:
                    continue

                try:
                    await container.thread.run(container.launcher.stop())
                except Exception as err:
                    # A failure in one container must not keep the others from stopping
                    self._logger.error(
                        f"{self._LOG_PREFIX} Failed to stop '{container_name}' of environment '{name}': "
                        f"{type(err).__name__}: {err}"
                    )

        for containers in self._containers.values():
            for container in containers.values():
                await container.thread.close()

        self._launched_environments.clear()
        self._logger.info(f"{self._LOG_PREFIX} Application stopped.")

    async def run(self) -> None:
        """Launches all the environments and keeps the application running until SIGINT or SIGTERM."""
        # The 'stop_event' is a flag that starts "off". The application waits for it at the end of this method,
        # and it is switched on ('set') when a request to stop arrives.
        stop_event = asyncio.Event()
        loop = asyncio.get_running_loop()

        # Two signals ask the application to stop:
        # - SIGINT is sent when the user presses Ctrl+C in the terminal;
        # - SIGTERM is sent by other programs (e.g., 'kill', Docker, Kubernetes) when they want it to stop.
        # When one of them arrives, 'stop_event.set' is called, which switches the flag on. This way, the
        # application is not cut abruptly: it stops in an orderly way (see the 'finally' below).
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, stop_event.set)
            except NotImplementedError:
                # 'add_signal_handler' is not available on Windows. The asyncio event loop is not the same on every
                # system: on Linux and macOS it is the 'SelectorEventLoop', which supports it; on Windows it is the
                # 'ProactorEventLoop', which does not. (The rest of asyncio works the same on all of them.)
                # Here, this means the 'stop_event' is never switched on. Python's default behaviour for Ctrl+C is
                # used instead: Python raises a KeyboardInterrupt, and 'asyncio.run' handles it by cancelling this
                # task, which raises a 'CancelledError' at the 'await stop_event.wait()' below. That interrupts the
                # wait and runs the 'finally', so 'stop()' is still called. Only after that does 'asyncio.run' raise
                # the KeyboardInterrupt. So the application stops in an orderly way on both systems; only the path
                # is different.
                pass

        # Note: signals and KeyboardInterrupt are not the same thing.
        # - A signal (SIGINT, SIGTERM, SIGKILL) is a message that the operating system sends to a process.
        #   SIGINT is sent by Ctrl+C. SIGTERM is the polite request to stop (e.g., 'kill', Docker, Kubernetes), and the
        #   program can catch it and close everything calmly. SIGKILL ('kill -9') kills the process at once: it cannot
        #   be caught or ignored, so neither the 'finally' nor 'stop()' below run.
        # - KeyboardInterrupt is an exception of Python (like ValueError), not something sent by the operating system.
        #   Python has a default handling for SIGINT: it turns it into a KeyboardInterrupt, raised in the code that is
        #   running. So "Ctrl+C -> SIGINT (system) -> KeyboardInterrupt (Python)".
        # Catching SIGINT here with 'add_signal_handler' happens before Python turns it into the exception, so on Linux
        # the KeyboardInterrupt does not even appear.

        await self.launch()
        self._logger.info(f"{self._LOG_PREFIX} Running. Press Ctrl+C to stop.")

        try:
            # After launching, nothing else is done here: the work is done in the background by the Input Handlers
            # (e.g., consumers of RabbitMQ queues, periodic requests). This line only keeps the application alive,
            # without using CPU, until the flag is switched on (or the wait is cancelled, on Windows). Without it,
            # 'run' would end right after 'launch', and so would the program, because 'asyncio.run' ends when the
            # function it runs ends.
            await stop_event.wait()
        finally:
            # Runs always, whether the stop was requested by a signal, by a Ctrl+C on Windows or caused by an error.
            # It stops the Input Handlers, the Output Handlers and the Telemetry Systems, so that no connection is
            # left open.
            await self.stop()

    def _resolve_logging_configuration(self) -> dict:
        """Returns the logger settings of the instance configuration, with the defaults the logger requires.

        Expected Output Structure:
        --------------------------
        {"log_files": {"max_size": 10485760}}

        Returns:
            dict: The 'logging' settings, with 'log_files.max_size' always present.
        """
        logging_configuration = dict(self._instance_configuration.logging)

        # The logger needs a maximum file size, so a default is used when none is set
        log_files = logging_configuration.get("log_files", {})
        logging_configuration["log_files"] = {"max_size": self._DEFAULT_LOG_MAX_SIZE, **log_files}
        return logging_configuration

if __name__ == "__main__":
    # Two different things are called "run" here:
    # - 'asyncio.run(...)' is a function of the asyncio library. It starts the event loop: it creates it, runs the
    #   coroutine it receives inside it until that coroutine ends, and then closes the loop. The loop only exists
    #   while 'asyncio.run' is working. Everything that is 'async' (the 'await's, the tasks, the RabbitMQ consumers)
    #   runs inside it. This block is outside the loop, in normal (not 'async') code.
    # - 'ApplicationLauncher.run()' is the method of this class. It is the coroutine given to 'asyncio.run'.
    #
    # Why the KeyboardInterrupt is caught here, and not in 'ApplicationLauncher.run()' or 'stop()':
    # On Windows, Ctrl+C does not switch on the 'stop_event' (see 'ApplicationLauncher.run()'). It happens in this order:
    # 1. Inside the loop, 'asyncio.run' cancels the task that is running 'ApplicationLauncher.run()'. A
    #    'CancelledError' is raised at its 'await stop_event.wait()', the 'finally' runs, 'stop()' closes everything
    #    and 'ApplicationLauncher.run()' ends.
    # 2. Only then, with 'ApplicationLauncher.run()' and 'stop()' already finished and the loop closed, does
    #    'asyncio.run' raise the KeyboardInterrupt.
    # So the KeyboardInterrupt is born after 'ApplicationLauncher.run()' and 'stop()' have ended, and the only code
    # still active to catch it is the one that called 'asyncio.run', which is this block. A 'try/except' inside
    # 'ApplicationLauncher.run()' would never see it.
    # On Linux and macOS this does not happen: 'add_signal_handler' catches Ctrl+C first and
    # 'ApplicationLauncher.run()' ends normally.
    #
    # The 'CancelledError' of step 1 does not need to be caught: 'asyncio.run' handles it by itself and turns it into
    # the KeyboardInterrupt. It must not be swallowed in 'ApplicationLauncher.run()' either, because a cancellation
    # has to end the task.
    #
    # Which errors are caught, and why:
    # The constructor of the launcher reads and validates the Percepta Instance Configuration, creates the logger and
    # builds the Schema Storage. These are the startup errors, and they have a clear message with a prefix, so only the
    # message is shown (see the last 'except'), without a traceback, which would only show where in the code the
    # check was made and would not help whoever has to fix the configuration. They are:
    # - 'ValueError': a file with the wrong format, e.g., "Instance Configuration | 'external_systems.telemetry_systems'
    #   is missing."
    # - 'FileNotFoundError': the configuration file, or the file of the Schema Storage, does not exist.
    # - 'PermissionError': there is no right to read a file, or to create the folder of the log files.
    # - 'IsADirectoryError': a path that should be a file is a folder.
    # The last three are kinds of 'OSError' (errors of the operating system with files and folders). They are listed
    # one by one, and not as 'OSError', so that only the ones that are known to be configuration problems are caught.
    # These errors are not written to the log files: the logger is created after the configuration was read, and the
    # errors are raised, not logged. Any other type of error is not caught on purpose: it is a bug (e.g., a launcher
    # module with a syntax error), not a configuration problem, so it keeps its traceback.
    try:
        application_launcher = SingleProcessApplicationLauncher(
            instance_configuration_path="configs/percepta_instance_configuration.json"
        )
        asyncio.run(application_launcher.run())
    except KeyboardInterrupt:
        # Nothing to do: everything was already stopped in 'ApplicationLauncher.run()'. Catching it only avoids
        # showing a traceback, as if the application had failed, when it was stopped on purpose.
        pass
    except (FileNotFoundError, PermissionError, IsADirectoryError, ValueError) as err:
        # 'err' is the exception that was caught (the object created by the 'raise' that failed). 'as err' only gives
        # it a name. In the f-string, '{err}' turns it into text, and that text is the message given to the 'raise'.
        # Example: 'raise ValueError("Instance Configuration | 'frequency' is missing or is not a dictionary.")'
        # makes the line below show:
        # "Percepta could not start: Instance Configuration | 'frequency' is missing or is not a dictionary."
        # Without a traceback, this message is all the user sees, so it must say what is wrong and where.
        #
        # 'file=sys.stderr' says where 'print' writes. By default, 'print' writes to 'stdout', the normal output of a
        # program. 'sys.stderr' is the other output, 'stderr', which is meant for errors. In the terminal both look
        # the same, but they can be separated: e.g., 'python app.py > output.txt' saves only 'stdout' in the file and
        # the error still appears on the screen, and Docker and monitoring tools treat 'stderr' as "something failed".
        # 'sys' is the module imported at the top of this file.
        print(f"Percepta could not start: {err}", file=sys.stderr)

        # Ends the process with exit code 1. Code 0 means "worked" and any other code means "failed", which is what
        # scripts, Docker and schedulers check to know that the application did not start.
        sys.exit(1)

# ----------------------------------------------------------------------------------------------------------------
# Notes about asyncio and the event loop (to understand the code above)
# ----------------------------------------------------------------------------------------------------------------
# What is asyncio?
# A standard library of Python (so it works on Linux, macOS and Windows) to do many things "at the same time"
# without threads, using 'async' functions (coroutines) and 'await'. It is good when the tasks spend most of their
# time waiting for I/O (network, files, timers), which is the case of the Data Ingestion: waiting for messages of
# RabbitMQ, for responses of HTTP requests, for the next period of a scheduler...
#
# What is an event loop?
# A cycle that runs in a single thread and decides which task advances next. It works like a waiter who serves
# several tables alone: he does not stand still waiting for a table to choose its dish. He takes an order, goes to
# the next table and comes back to the first one when the kitchen says its dish is ready. In order:
# 1. The loop has a list of tasks that can advance, and runs one until its first 'await' that has to wait for
#    something (e.g., a response, a message, a timer).
# 2. At that 'await', the task is suspended and gives the control back to the loop, which moves on to another task.
# 3. When the operating system says that what the task was waiting for has arrived, the loop puts the task back in
#    the list and resumes it.
# 4. This repeats until there is nothing left to do or the loop is stopped.
# Consequences:
# - Everything runs in one thread: two tasks never run at the same instant, they take turns at each 'await'.
# - An 'await' is where the other tasks can advance. A long calculation or a blocking call without 'await' (e.g.,
#   'time.sleep') stops the whole loop, and every other task has to wait.
# - 'await coroutine' runs the coroutine now and waits for it to return; 'asyncio.create_task(coroutine)' schedules
#   it in the loop and continues without waiting.
# - 'asyncio.run' is what creates the loop, runs the coroutine given to it and closes the loop.
#
# Is the event loop the same on every system?
# The code written with 'async'/'await' is the same on all of them, but the loop underneath is not, because each
# operating system has its own way of saying "this is ready":
# - Linux and macOS (Unix-like): 'SelectorEventLoop'. It uses 'epoll' or 'kqueue', which answer "this socket has data
#   to read". When the loop is told, it reads the data (the "reactor" model).
# - Windows: 'ProactorEventLoop' (the default since Python 3.8). It uses the IOCP of Windows: the loop asks the system
#   to do the read, and is told when it is finished (the "proactor" model).
# In practice, this only shows in low-level details. The one that matters here: 'loop.add_signal_handler' (used in
# 'ApplicationLauncher.run' to catch Ctrl+C and SIGTERM) exists on Unix-like systems, but is not implemented on the
# Windows loop and raises 'NotImplementedError'. That is why 'run' catches that error, and why the Ctrl+C takes a
# different path on Windows (see the comments in 'run' and in the 'if __name__ == "__main__"' block).
#
# Why this is especially useful in Python (the GIL)
# What is the GIL? The Global Interpreter Lock is a lock inside the usual Python interpreter (CPython): only ONE
# thread can run Python code at each instant, even on a computer with many cores. So several threads do not run Python
# code in parallel, they take turns. A thread that waits for I/O releases the lock, so threads still help with
# waiting, but they cost memory, the switching between them has a cost, and shared data needs locks to avoid errors.
# For a program like this one, that spends most of its time waiting for I/O, the event loop gets the same benefit
# (doing other things while one waits) with a single thread: no threads to manage, and the switch between tasks only
# happens at an 'await', so it is easy to know where it can happen. Because of that, the data is safe between two
# 'await's (e.g., 'counter += 1' or 'list.append(x)' are never left half done), and locks are rarely needed. They are
# still needed when a sequence that must stay consistent has an 'await' in the middle, because another task can run
# there and change the same data (e.g., "if key not in cache: cache[key] = await fetch()"). In that case, use an
# 'asyncio.Lock'.
# Being single-threaded, the event loop does not use more than one core, which threads could not do for Python code
# either, because of the GIL. For heavy calculations, the usual solution is different: separate processes (e.g.,
# 'multiprocessing').
# State at the time of writing: up to Python 3.12 (the version of this project) the GIL is always present. The newer
# versions have an optional "free-threaded" build without the GIL (experimental in 3.13, and supported but still
# optional in later versions), but the normal build keeps the GIL, and it is the one used here.
