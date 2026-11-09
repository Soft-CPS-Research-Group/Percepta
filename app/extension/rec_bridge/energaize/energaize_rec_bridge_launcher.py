# --- Internal Framework Imports ---
from app.extension.rec_bridge.energaize.energaize_rec_bridge_adapter import EnergAIzeRECBridgeAdapter
from app.extension.rec_bridge.rec_bridge_launcher import RECBridgeLauncher


class EnergAIzeRECBridgeLauncher(RECBridgeLauncher):
    """Launcher of the REC Bridge for EnergAIze: it uses the EnergAIze Adapter (see EnergAIzeRECBridgeAdapter)."""

    _LOG_PREFIX: str = "EnergAIze REC Bridge Launcher |"

    def _get_adapter_class(self) -> type:
        """Gives the EnergAIze Adapter."""
        return EnergAIzeRECBridgeAdapter
