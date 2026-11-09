# --- Standard Library Imports ---
from typing import Tuple

# --- Internal Framework Imports ---
from app.core.launchers.application_launchers.single_process_application_launcher import (
    SingleProcessApplicationLauncher
)


class UntilRECBridgeApplicationLauncher(SingleProcessApplicationLauncher):
    """Application Launcher that runs the pipeline only until the REC Bridge.

    It launches Data Ingestion, Data Processing and the REC Bridge of each environment, in the same way as the
    SingleProcessApplicationLauncher (same process, one thread per container, launched from the last to the first).
    Inference and Decision Dispatching are not launched, so the installation only collects and shares data with the
    REC, and the result of the REC Bridge is published to its output link ('rec_bridge_to_inference') for whoever
    consumes it. Use it for an installation that has no model and no Target Systems.

    The instance must configure the 'rec_bridge', as that is where the pipeline ends.
    """

    # From the last container that runs to the first. The stop order is the reverse.
    _LAUNCH_ORDER_UNTIL_REC_BRIDGE: Tuple[str, ...] = ("rec_bridge", "data_processing", "data_ingestion")

    def _select_launch_order(self) -> Tuple[str, ...]:
        """Chooses Data Ingestion, Data Processing and the REC Bridge.

        Returns:
            Tuple[str, ...]: The names of the containers to launch, from the last to the first.

        Raises:
            ValueError: If the instance has no 'rec_bridge', as there would be nothing to stop at.
        """
        if not self._instance_configuration.rec_bridge:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: this Application Launcher runs until the REC Bridge, so "
                f"the instance needs the 'rec_bridge' configuration."
            )
        return self._LAUNCH_ORDER_UNTIL_REC_BRIDGE
