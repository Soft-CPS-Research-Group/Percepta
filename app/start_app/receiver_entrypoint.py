from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.utils.registry_auto import discover_subclasses
from app.receivers.receiver_base import ReceiverBase
from typing import Type, Dict
from app.utils.logger import LoggingUtils

# Dynamically build a registry of all available receivers in the package
RECEIVER_REGISTRY: Dict[str, Type[ReceiverBase]] = discover_subclasses(
    package="app.receivers",
    base_class=ReceiverBase,
    required_suffix="_receiver"
)

def start_receivers(environment_repository : EnvironmentRepository, configurations : dict, logger : LoggingUtils):
    threads = []

    for ReceiverClass in RECEIVER_REGISTRY.values():
        provider = getattr(ReceiverClass, "provider", None)

        if provider is not None:
            environments = environment_repository.get_environments_by_provider(provider)
        else:
            environments = environment_repository.get_environments()

        ReceiverClass.pre_start(configurations, logger)

        for current_environment, environment_specs in environments.items():
            logger_per_receiver = LoggingUtils(f"{provider}_receiver", configurations, current_environment)

            receiver = ReceiverClass(current_environment, environment_specs, configurations, logger_per_receiver)
            receiver.start()
            threads.append(receiver)

        ReceiverClass.post_start(environments, configurations, logger)

    return threads


import time

def launch_receivers(environment_repository : EnvironmentRepository, configurations : dict, logger : LoggingUtils):

    threads = start_receivers(environment_repository, configurations, logger)

    try:
        while threads:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
                time.sleep(0.1)
    except KeyboardInterrupt:
        logger.info("Receiver Launcher: Stopping all threads...")
        for thread in threads:
            thread.stop()
        for thread in threads:
            thread.join()
        logger.info("Receiver Launcher: All threads stopped.")

