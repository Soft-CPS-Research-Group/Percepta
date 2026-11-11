"""Entry point of Percepta: runs the Application Launcher chosen in the Percepta Instance Configuration.

Each instance (e.g., each Docker container) can use a different Application Launcher, set in
'application_launcher.path' of its configuration. Without it, the SingleProcessApplicationLauncher is used, which
launches all the containers.

Run with: python -m app.core.launchers.run_application
"""
# --- Standard Library Imports ---
import asyncio
import sys

# --- Internal Framework Imports ---
from app.core.utils.class_loader import load_class
from app.core.launchers.application_launchers.single_process_application_launcher import ApplicationLaunchError
from app.core.utils.instance_configuration import InstanceConfiguration

_INSTANCE_CONFIGURATION_PATH: str = "configs/percepta_instance_configuration.json"


def _load_application_launcher_class(instance_configuration_path: str) -> type:
    """Loads the class of the Application Launcher chosen in the Percepta Instance Configuration.

    Args:
        instance_configuration_path (str): Path to the Percepta Instance Configuration (JSON).

    Returns:
        type: The class. It is built with 'instance_configuration_path' and has the async method 'run()'.

    Raises:
        FileNotFoundError: If the configuration file does not exist.
        ValueError: If the configuration is not valid, or the class cannot be loaded.
    """
    path = InstanceConfiguration(instance_configuration_path).application_launcher_path
    return load_class(path, "application_launcher.path", "Application Launcher |")


def main() -> None:
    """Starts the application with the Application Launcher of the configuration and ends with the right exit code."""
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
        # The Application Launcher is chosen in the configuration ('application_launcher.path'). Without it, the
        # SingleProcessApplicationLauncher is used (all the containers).
        launcher_class = _load_application_launcher_class(_INSTANCE_CONFIGURATION_PATH)
        application_launcher = launcher_class(instance_configuration_path=_INSTANCE_CONFIGURATION_PATH)
        asyncio.run(application_launcher.run())
    except ApplicationLaunchError:
        # The environments could not be launched. The error was already written in the log by 'launch()', and
        # everything that had started was stopped. A traceback would only show where in the code the failure was
        # detected, so the program just ends with exit code 1 (any code other than 0 means "failed").
        sys.exit(1)
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


if __name__ == "__main__":
    main()
