from app.aggregators.aggregator_base import AggregatorBase
from app.utils.logger import LoggingUtils


class EnergyAggregator(AggregatorBase):
    """
    Aggregator responsible for calculating system-level metrics.
    For example: non_shiftable_load
    """

    def __init__(self, logger: LoggingUtils =None):
        super().__init__(logger)

    def aggregate(self, message):
        """
        message: {
            "grid_meters": {grid_id: {"energy_in": float, ...}, ...},
            "batteries": {battery_id: {"energy_in": float, "last_soc": float}, ...}
        }

        Returns:
            dict containing aggregated metrics including non_shiftable_load
        """
        # --- Sum total energy from grid meters ---
        grid_meters = message.get("observations", {}).get("grid_meters", {})
        print(f"grid meters {grid_meters}")
        total_grid_in = sum(g.get("energy_in_total", 0) for g in grid_meters.values())
        total_grid_out = sum(g.get("energy_out_total", 0) for g in grid_meters.values())

        # --- Sum total charging energy from batteries ---
        batteries = message.get("observations", {}).get("batteries", {})
        total_battery_charge = sum(b.get("energy_in", 0) for b in batteries.values())
        total_battery_discharge = sum(b.get("energy_out", 0) for b in batteries.values())

        solar_generation = message.get("solar_generation", 0)

        # --- Calculate non_shiftable_load ---
        non_shiftable_load = total_grid_in + solar_generation - total_grid_out + total_battery_discharge - total_battery_charge

        '''if self._logger:
            self._logger.info(
                f"Total Grid In: {total_grid_in}, Total Battery Charge: {total_battery_charge}, "
                f"Non-Shiftable Load: {non_shiftable_load}"
            )'''

        if non_shiftable_load < 0:
            non_shiftable_load = 0

        message["observations"]["non_shiftable_load"] = non_shiftable_load
