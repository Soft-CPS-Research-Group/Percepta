import time
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.repositories.repository_factory import RepositoryFactory
from app.accumulator_context_factory import AccumulatorContextFactory

def launch_accumulator_service(environment_repository: EnvironmentRepository,
                               repository_factory: RepositoryFactory,
                               configurations: dict,
                               logger):

    logger.info("Starting Accumulator...")

    all_environments = environment_repository.get_environments()
    all_provider_configs = environment_repository.get_all_configurations()
    all_env_names = list(all_environments.keys())
    threads = []

    try:
        for current_environment, environment_specs in all_environments.items():
            group = environment_specs.get('group')
            time_series_repository = repository_factory.build_time_series_repository(group, current_environment)
            factory = AccumulatorContextFactory(current_environment, environment_specs, configurations, logger)
            forwarders = factory.build_forwarders(all_provider_configs)
            predictor = factory.build_predictor(forwarders, time_series_repository)
            output_handler = factory.build_output_handler(predictor)
            manager = factory.build_manager(time_series_repository, output_handler)
            accumulator = factory.build_accumulator(manager)
            # Start the accumulator (which runs as a thread)
            accumulator.start()

            threads.append(accumulator)

        # Loop to keep the main thread alive and monitor the threads
        while threads:
            for thread in threads[:]:  # iterate over a copy of the list
                if not thread.is_alive():
                    threads.remove(thread)
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping threads...")
        for thread in threads:
            thread.stop()  # assuming your accumulator has a stop() method

        for thread in threads:
            thread.join()
        print("All threads stopped.")
