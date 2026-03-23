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

    # time_interval = DataSet.calculate_interval(frequency)

    repository_factory = RepositoryFactory(configurations, logger)

    # Instantiate shared environment specs repository
    environment_repository = repository_factory.build_environment_repository()

    accumulator_process = multiprocessing.Process(
        target=accumulator_entrypoint,
        args=(environment_repository, repository_factory, configurations, logger)
    )

    accumulator_process.start()

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
