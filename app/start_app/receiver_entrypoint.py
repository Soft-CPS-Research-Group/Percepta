import time
import threading
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.utils.registry_auto import discover_subclasses
from app.receivers.receiver_base import ReceiverBase
from app.utils.logger import LoggingUtils

# Dynamically build a registry of all available receivers in the package
RECEIVER_REGISTRY: dict = discover_subclasses(
    package="app.receivers",
    base_class=ReceiverBase,
    required_suffix="_receiver"
)


def start_receivers(environment_repository: EnvironmentRepository, configurations: dict, logger: LoggingUtils) -> list:
    """
    Start all registered receiver threads based on available environments.
    """
    threads = []

    for ReceiverClass in RECEIVER_REGISTRY.values():

        provider = getattr(ReceiverClass, "provider", None)
        if provider is not None:
            # Fetch environments specific to the current provider
            environments = environment_repository.get_environments_by_provider(provider)

            # If there are no environments for a certain provider, skip starting its receiver
            if environments is None:
                logger.info(f"There is no environment for provider {provider}.")
                continue
        else:
            # Fetch all environments if the receiver is provider-agnostic
            environments = environment_repository.get_environments()

        # Call pre-start hook after all receiver instances are started
        threading.Thread(
            target=ReceiverClass.pre_start,
            args=(configurations, logger),
            daemon=True
        ).start()

        for current_environment, environment_specs in environments.items():
            # Create a dedicated logger for each receiver instance
            logger_per_receiver = LoggingUtils(f"{provider}_receiver", configurations, current_environment)

            # Instantiate and start the receiver thread
            receiver = ReceiverClass(current_environment, environment_specs, configurations, logger_per_receiver)
            receiver.start()
            threads.append(receiver)

        # Call post-start hook after all receiver instances are started
        threading.Thread(
            target=ReceiverClass.post_start,
            args=(environments, configurations, logger),
            daemon=True
        ).start()

    return threads


def launch_receivers(environment_repository: EnvironmentRepository, configurations: dict, logger: LoggingUtils) -> None:
    """
    Launch all receivers and monitor their threads, handling graceful shutdown on KeyboardInterrupt.
    """
    threads = start_receivers(environment_repository, configurations, logger)

    try:
        while threads:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
                time.sleep(0.1)

    except KeyboardInterrupt:
        logger.info("Receiver Launcher: Stopping all threads...")
        # Stop all active receiver threads gracefully
        for thread in threads:
            thread.stop()
        # Wait for all threads to finish
        for thread in threads:
            thread.join()
        logger.info("Receiver Launcher: All threads stopped.")
