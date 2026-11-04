# launchers/algorithm_launcher.py
from typing import Any, Tuple, Dict, Callable
from app.core.utils.logger import LoggingUtils

from app.algorithms.energaize_algorithm import EnergAIzeAlgorithm
from app.fail_safe.energaize_fail_safe import EnergAIzeFailSafe

# Single unified mapping registry for the complete ML + Safety ecosystem pairing
ALGORITHM_PROVIDERS: Dict[str, Callable] = {}


def register_algorithm(name: str):
    """Decorator to register a unified algorithm and fail-safe provider."""

    def decorator(func):
        ALGORITHM_PROVIDERS[name] = func
        return func

    return decorator


class AlgorithmLauncher:
    """
    Specialized sub-launcher responsible for parsing algorithm definitions,
    executing their coupled factory providers, and returning the complete ML/Safety pair.
    """

    def __init__(self, configurations: dict, logger: LoggingUtils) -> None:
        self._configurations = configurations
        self._logger = logger

    def list_components(self, algorithm_params: dict) -> Tuple[Any, Any]:
        """
        Parses the algorithm configuration parameters, triggers its provider factory,
        and returns a paired (algorithm_instance, safety_instance) tuple.
        """
        if not algorithm_params:
            return None, None

        # EXACTLY WHAT YOU SAID: We fetch the algorithm name directly from the params
        name = algorithm_params.get('algorithm_module')

        if name in ALGORITHM_PROVIDERS:
            try:
                provider_func = ALGORITHM_PROVIDERS[name]

                # We pass the dictionary containing the rest of the variables to the provider
                algorithm_instance, safety_instance = provider_func(self, algorithm_params)

                self._logger.info(f"AlgorithmLauncher | Successfully loaded unified ecosystem pairing for: {name}")
                return algorithm_instance, safety_instance

            except Exception as e:
                self._logger.error(f"AlgorithmLauncher | Error building paired ecosystem [{name}]: {e}")
                raise
        else:
            self._logger.warning(f"AlgorithmLauncher | No provider found for algorithm type: {name}")
            return None, None

    @register_algorithm("EnergAIzeAlgorithm")
    def _build_energaize_algorithm(self, params: dict) -> Tuple[EnergAIzeAlgorithm, EnergAIzeFailSafe]:
        """Concrete factory provider returning the tightly-coupled EnergAIze ML and Safety modules."""
        algorithm = EnergAIzeAlgorithm(
            configurations=self._configurations,
            logger=self._logger,
            n_estimators=params.get('n_estimators', 100),
            weights_path=params.get('model_weights_path')
        )

        fail_safe = EnergAIzeFailSafe(
            logger=self._logger,
            max_deviation=params.get('max_deviation_threshold', 0.10)
        )

        return algorithm, fail_safe