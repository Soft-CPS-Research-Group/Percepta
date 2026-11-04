from app.start_app.launcher_base import LauncherBase
from app.core.utils.logger import LoggingUtils
from .forwarders.decision_forwarder_factory import build_forwarder


class PredictorLauncher(LauncherBase):
    def __init__(self, environment, environment_specs, all_provider_configs : dict, logger : LoggingUtils):
        super().__init__(logger)

        forwarder_logger = LoggingUtils("forwarder", self._configurations, environment)

        self.forwarders = build_forwarder(environment, environment_specs, all_provider_configs, self._configurations, forwarder_logger)

    def launch(self):
        pass

