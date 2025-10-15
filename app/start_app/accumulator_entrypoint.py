import time
from multiprocessing import Process
from app.repositories.irepositories.environment_repository import EnvironmentRepository
from app.repositories.repository_factory import RepositoryFactory
from app.accumulator_context_factory import AccumulatorContextFactory

def launch_accumulator_service(environment_repository: EnvironmentRepository,
                               repository_factory: RepositoryFactory,
                               configurations: dict,
                               logger):

    logger.info("Starting Accumulator...")

    all_environments = environment_repository.get_environments()
    processes = []

    try:
        for current_environment, environment_specs in all_environments.items():
            group = environment_specs.get('group')
            time_series_repository = repository_factory.build_time_series_repository(group, current_environment)
            factory = AccumulatorContextFactory(current_environment, environment_specs, configurations, logger)
            predictor = factory.build_predictor(time_series_repository)
            manager = factory.build_manager(time_series_repository, predictor)
            accumulator = factory.build_accumulator(manager)

            # Create a process for each accumulator
            p = Process(target=accumulator.start)
            p.start()
            processes.append(p)

        # Loop to keep the main process alive and monitor the child processes
        while processes:
            for p in processes:
                if not p.is_alive():
                    processes.remove(p)
            time.sleep(0.1)

    except KeyboardInterrupt:
        print("KeyboardInterrupt: Stopping processes...")
        for p in processes:
            p.terminate()  # terminate the process
        for p in processes:
            p.join()
        print("All processes stopped.")
