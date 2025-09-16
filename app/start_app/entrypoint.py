import time
import multiprocessing
from datetime import datetime, timedelta
from app.utils.data import DataSet
from app.utils.logger import LoggingUtils
from app.repositories.repository_factory import RepositoryFactory
from .accumulator_entrypoint import launch_accumulator_service as accumulator_entrypoint
from .receiver_entrypoint import launch_receivers as receivers_entrypoint

# Load configuration from file once at startup
configurations = DataSet.get_schema('./configs/runtime_configurations.json')
# Initialize logger once for main process
logger = LoggingUtils(component_name="entrypoint", configurations=configurations)

def wait_for_next_interval_start(time_interval):
    """
    Wait until the next aligned time interval from the start of the hour,
    plus 1 second. For example, if interval=120s, waits until hh:00:01, hh:02:01, etc.
    """
    now = datetime.now()
    start_of_hour = now.replace(minute=0, second=0, microsecond=0)
    seconds_since_hour = (now - start_of_hour).total_seconds()
    next_multiple = ((seconds_since_hour // time_interval) + 1) * time_interval
    wait_seconds = next_multiple - seconds_since_hour + 1
    target_time = now + timedelta(seconds=wait_seconds)
    logger.info(f"Waiting {wait_seconds:.2f}s until {target_time.strftime('%H:%M:%S')} to start data requesters...")
    time.sleep(wait_seconds)

def start_data_requesters(specs_repo):
    """
    Starts multiple data receiver/requester processes in parallel,
    passing shared environment specs, configuration and logger to each.
    """
    receivers_process = multiprocessing.Process(target=receivers_entrypoint, args=(specs_repo, configurations, logger))

    receivers_process.start()


def launch_app():
    """
    Main orchestration function that:
    - Calculates the time interval from configurations
    - Creates shared environment specs repository
    - Starts the accumulator process
    - Waits for the next aligned interval start
    - Starts the data requester processes
    """

    frequency = configurations.get('frequency')

    time_interval = DataSet.calculate_interval(frequency)

    repository_factory = RepositoryFactory(configurations, logger)

    # Instantiate shared environment specs repository
    environment_repository = repository_factory.build_environment_repository()

    accumulator_process = multiprocessing.Process(
        target=accumulator_entrypoint,
        args=(environment_repository, repository_factory, configurations, logger)
    )
    accumulator_process.start()

    # Wait until next aligned time interval before starting requesters
    wait_for_next_interval_start(time_interval)


    # Start data requesters after the wait
    start_data_requesters(environment_repository)

    # Optionally wait for the accumulator process to finish
    accumulator_process.join()


if __name__ == "__main__":
    # This block runs only if the script is executed directly (not imported as a module)

    try:
        # Set the multiprocessing start method to 'fork'
        # 'fork' is more efficient and common on Unix/Linux systems
        # This must be set before any multiprocessing is used
        # If it's already been set elsewhere, this will raise a RuntimeError
        multiprocessing.set_start_method('fork')

    except RuntimeError:
        # If the start method was already set earlier, just ignore the error and continue
        # This avoids crashing the program with "context has already been set"
        pass

    # Start the main server orchestration logic
    launch_app()
