class EnergyAggregator:
    """
    Aggregator responsible for calculating system-level metrics.
    For example: non_shiftable_load
    """

    def __init__(self, logger=None):
        self._logger = logger

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
        grid_meters = message.get("grid_meters", {})
        total_grid_in = sum(g.get("energy_in", 0) for g in grid_meters.values())

        # --- Sum total charging energy from batteries ---
        batteries = message.get("batteries", {})
        total_battery_charge = sum(b.get("energy_in", 0) for b in batteries.values())

        # --- Calculate non_shiftable_load ---
        non_shiftable_load = total_grid_in - total_battery_charge

        if self._logger:
            self._logger.info(
                f"Total Grid In: {total_grid_in}, Total Battery Charge: {total_battery_charge}, "
                f"Non-Shiftable Load: {non_shiftable_load}"
            )

        message["non_shiftable_load"] = non_shiftable_load
