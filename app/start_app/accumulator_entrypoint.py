import time
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.repositories.repository_factory import RepositoryFactory
from app.accumulator_context_factory import AccumulatorContextFactory

# TODO estudar melhor esta divisão de repositórios
def launch_accumulator(environment_repository : EnvironmentRepository, repository_factory : RepositoryFactory, configurations: dict, logger):

    logger.info("Starting Accumulator...")

    all_environments = environment_repository.get_environments()

    threads = []
    try:
        for current_environment, environment_specs in all_environments.items():
            group = environment_specs.get('group')
            time_series_repository = repository_factory.build_time_series_repository(group,current_environment)
            factory = AccumulatorContextFactory(current_environment, environment_specs, configurations, logger)
            predictor = factory.build_predictor(time_series_repository)
            manager = factory.build_manager(time_series_repository, predictor)
            accumulator = factory.build_accumulator(manager)

            accumulator.start()
            threads.append(accumulator)

        while threads:
            for thread in threads:
                if not thread.is_alive():
                    threads.remove(thread)
                time.sleep(0.1)

    except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping threads...")
        for thread in threads:
            thread.stop()

        for thread in threads:
            thread.join()
        print("All threads stopped.")
