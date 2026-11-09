# --- Standard Library Imports ---
import asyncio
from datetime import date, datetime, time, timedelta, timezone
from typing import Awaitable, Callable, Dict, List, Optional
from zoneinfo import ZoneInfo

# --- Internal Framework Imports ---
from app.core.utils.async_periodic_scheduler import AsyncPeriodicScheduler
from app.core.utils.logger import LoggingUtils

# Function that returns the spot prices (EUR/MWh) of one market date: one list per series, one value per block.
# Example: {"pt_spot_price": [226.0, 225.0, ...], "es_spot_price": [226.0, 225.0, ...]}
PriceFetcher = Callable[[date], Awaitable[Dict[str, List[float]]]]


class OmieSpotPriceService:
    """Keeps the Spanish and Portuguese day-ahead spot prices published by OMIE, ready to be read at any time.

    OMIE publishes the prices of the next day around 12:45 (Madrid time), but not always at the same time.
    Each price is for a block of 15 minutes (96 blocks in a normal day, 92 or 100 when the clock changes)
    and is kept in the original unit (EUR/MWh).

    How it works:
    - On `start`, it fetches the prices available now: the current market day and, if the publication
      time has already passed, the next day too.
    - Then, a periodic job checks if there is something missing. After the publication time, the
      job fetches the prices of the next day. If OMIE has not published them yet, the job
      simply tries again on its next run.
    - The prices are stored by series ('pt_spot_price' and 'es_spot_price') and, inside each one, by UTC
      timestamp. Old prices are removed after a safety margin.
    - The service keeps both series.

    What OMIE returns:
    For each market day, OMIE publishes a text file, not structured data like JSON. It is requested with a GET
    (no login), to a URL that has the date inside it. Example, for 2026-10-15:
        GET https://www.omie.es/sites/default/files/dados/AGNO_2026/MES_10/TXT/INT_PBC_EV_H_1_15_10_2026_15_10_2026.TXT
    The pattern is:
        .../AGNO_{yyyy}/MES_{mm}/TXT/INT_PBC_EV_H_1_{dd}_{mm}_{yyyy}_{dd}_{mm}_{yyyy}.TXT
    If OMIE has not published that day yet, the request fails with an HTTP error status.

    The response is a CSV in iso-8859-1, with ';' as separator, ',' as decimal mark and labels in Spanish. Each
    row is one series and each column is one block of the day, so a row has 96 values in a normal day. Besides
    the prices, the file has other series (such as the power traded), which are not used here.
    Example (simplified, only the rows that are used):
        Precio marginal en el sistema español (EUR/MWh);226,00; ...
        Precio marginal en el sistema portugués (EUR/MWh);226,00;225,00;224,00;219,86; ... ;225,00;220,00

    Why an Input Handler is NOT used to fetch the prices:
    An Input Handler could be created for this, but it would only
    deliver the OMIE file as it is: an Input Handler does not translate data.
    The file must be translated into lists of prices, and that work would have to be done by this service.
    That is why the `pyomie` library is so useful here: it requests the file, finds in it the rows of the
    Portuguese and Spanish prices, and converts them into lists of numbers, so the service receives the prices
    ready to use. An Input Handler would add nothing; on the contrary, using one would mean more work.
    """

    # --- Class Attribute & Type Declarations ---
    _LOG_PREFIX: str  # Standardized logging identifier prefix
    _BLOCK_SECONDS: int  # Length of one OMIE price block (900 s = 15 minutes)
    _JOB_ID: str  # Identifier of the periodic refresh job

    _logger: LoggingUtils  # Logger of the service (system level, so no environment)
    _market_timezone: ZoneInfo  # Timezone of the OMIE market days (Europe/Madrid)
    _publication_time: time  # Time of day (market timezone) after which tomorrow's prices are fetched
    _refresh_interval_seconds: int  # How often the job checks for missing prices
    _retention: timedelta  # How long past prices are kept
    _price_fetcher: PriceFetcher  # Function that fetches the prices of one date
    _prices: Dict[str, Dict[datetime, float]]  # { series : { UTC timestamp of the block start : price in EUR/MWh } }
    _fetched_dates: set  # Market dates already stored
    _scheduler: Optional[AsyncPeriodicScheduler]  # Periodic refresh job
    _lock: asyncio.Lock  # Avoids two refreshes at the same time

    _LOG_PREFIX = "OMIE Spot Price Service |"
    _BLOCK_SECONDS = 900
    _JOB_ID = "omie_spot_price_refresh"

    def __init__(
            self,
            logger: LoggingUtils,
            market_timezone: str = "Europe/Madrid",
            publication_time: str = "13:00",
            refresh_interval_seconds: int = 600,
            retention_seconds: int = 7200,
            price_fetcher: Optional[PriceFetcher] = None
    ) -> None:
        """Initializes the service. Nothing is fetched until `start` is called.

        Args:
            logger (LoggingUtils): Logger of the service.
            market_timezone (str): Timezone of the market days. OMIE works in Spanish time. Defaults to "Europe/Madrid".
            publication_time (str): "HH:MM" in the market timezone. From this time on, the prices of the next day
                are fetched. OMIE publishes around 12:45 (Madrid time). Defaults to "13:00".
            refresh_interval_seconds (int): How often the job checks for missing prices. Defaults to 600.
            retention_seconds (int): How long past prices are kept. The margin keeps the block that is running now
                (it started up to 15 minutes ago), so the current price is never missing. Defaults to 7200 (2 hours).
            price_fetcher (PriceFetcher, optional): Function that fetches the prices of one date. Defaults to pyomie.
                It can be replaced in tests, so that no network is needed.

        Raises:
            ValueError: If any argument is invalid.
        """
        # --- Guard Clause 1: logger ---
        if not logger:
            raise ValueError(f"{self._LOG_PREFIX} Initialization error: 'logger' dependency is missing.")

        # --- Guard Clause 2: timezone ---
        try:
            self._market_timezone = ZoneInfo(market_timezone)
        except Exception as err:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: invalid market timezone '{market_timezone}': {err}"
            ) from err

        # --- Guard Clause 3: publication time ("HH:MM") ---
        try:
            hour_text, minute_text = str(publication_time).strip().split(":")
            self._publication_time = time(int(hour_text), int(minute_text))
        except ValueError as err:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'publication_time' must be 'HH:MM', got '{publication_time}'."
            ) from err

        # --- Guard Clause 4: refresh interval and retention ---
        if not isinstance(refresh_interval_seconds, int) or refresh_interval_seconds <= 0:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'refresh_interval_seconds' must be a positive integer."
            )

        if not isinstance(retention_seconds, int) or retention_seconds < 0:
            raise ValueError(
                f"{self._LOG_PREFIX} Initialization error: 'retention_seconds' must be zero or a positive integer."
            )

        self._logger = logger
        self._refresh_interval_seconds = refresh_interval_seconds
        self._retention = timedelta(seconds=retention_seconds)
        self._price_fetcher = price_fetcher or self._fetch_with_pyomie

        self._prices = {}
        self._fetched_dates = set()
        self._scheduler = None
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    async def start(self) -> None:
        """Fetches the prices available now and starts the periodic refresh job.

        A failure of the first fetch is NOT fatal: it is logged, and the job tries again on its next run.
        Calling `start` when the service is already running does nothing.
        """
        # --- Guard Clause 1: already running ---
        if self._scheduler:
            self._logger.debug(f"{self._LOG_PREFIX} Already running. Nothing to start.")
            return

        await self.refresh()

        # Why the job runs every 'refresh_interval_seconds' (600 s by default) and not once a day:
        #
        # OMIE normally publishes the prices of the next day around 12:45 (Madrid time), but this is not
        # guaranteed: some days it publishes later. If the job ran only once, at a fixed time, and the file was
        # not there yet, the prices of the next day would be missing until the following day.
        # So the job checks again every 10 minutes, and the prices are fetched as soon as OMIE publishes them.
        #
        # The same repetition also covers other cases without extra code:
        #   - the application starts after the publication time;
        #   - OMIE sends an invalid file (it is rejected and fetched again in the next run);
        #   - the prices that are no longer needed (older than the retention margin) are removed in every run.
        #
        # Most runs have nothing to fetch. In that case 'refresh' only compares dates in memory and does NOT
        # call OMIE (about 15 microseconds per run). With one run every 10 minutes there are 144 runs per day,
        # so the total is about 2 ms per day (144 x 15 microseconds). A shorter interval means more runs and a
        # proportionally higher total, which is still very small. OMIE is only called when a date is missing,
        # which is usually 1 to 3 requests per day.
        self._scheduler = AsyncPeriodicScheduler(timezone=self._market_timezone, logger=self._logger)
        self._scheduler.create_job(
            time_interval=self._refresh_interval_seconds,
            trigger_callback=self.refresh,
            job_id=self._JOB_ID
        )
        self._scheduler.start()

        self._logger.info(
            f"{self._LOG_PREFIX} Started. Checking for new prices every {self._refresh_interval_seconds}s. "
            f"The prices of the next day are only requested after {self._publication_time.strftime('%H:%M')} "
            f"({self._market_timezone.key}), the time they are published."
        )

    async def stop(self) -> None:
        """Stops the periodic refresh job. Safe to call more than once."""
        if self._scheduler:
            self._scheduler.stop()
            self._scheduler = None
            self._logger.debug(f"{self._LOG_PREFIX} Stopped.")

    # ------------------------------------------------------------------
    # Fetching
    # ------------------------------------------------------------------
    async def refresh(self, now: Optional[datetime] = None) -> None:
        """Fetches the market dates that are needed and still missing, then removes the old prices.

        This is the body of the periodic job, so it never raises: a failure only means that the same
        date is tried again on the next run.

        Expected Input Structure:
        -------------------------
        now = datetime(2026, 10, 15, 14, 10, tzinfo=ZoneInfo("Europe/Madrid"))     # or None for the real time

        Expected Output Structure:
        --------------------------
        None. The result is the change in the stored prices. Example, at 14:10 Madrid time, with 15 Oct
        already stored and 16 Oct published:
            before: _fetched_dates = {date(2026, 10, 15)}
            after:  _fetched_dates = {date(2026, 10, 15), date(2026, 10, 16)}   # 96 new blocks per series stored

        Args:
            now (datetime, optional): The current time (timezone-aware). Defaults to the real time.
                It exists so that tests can choose the time.
        """
        # Use the given time (tests) or the real one, always converted to UTC.
        # Example: now = None -> the current time; now = 2026-10-15 14:10 Madrid -> 2026-10-15 12:10 UTC
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

        # The lock lets only one refresh run at a time. Without it, a run that takes longer than the interval
        # (for example, OMIE is slow) could overlap with the next one and fetch the same date twice.
        async with self._lock:

            # Only the dates that are needed and not stored yet. Example: [date(2026, 10, 16)]
            for market_date in self._dates_to_fetch(now_utc):

                # Step 1: fetch the prices from OMIE
                try:
                    prices = await self._price_fetcher(market_date)
                except Exception as err:
                    # Example: OMIE has not published 2026-10-16 yet -> ClientResponseError 404.
                    # It is only a warning: nothing is stored, and the same date is tried in the next run.
                    # str() of some errors (for example, TimeoutError) is empty, so the type name is also shown
                    self._logger.warning(
                        f"{self._LOG_PREFIX} Prices of {market_date} not available yet "
                        f"({type(err).__name__}: {err}). Will try again in {self._refresh_interval_seconds}s."
                    )

                    # Skip step 2 for this date and go to the next date
                    continue

                # Step 2: store the prices. They are checked here, and an invalid file raises ValueError.
                try:
                    self._store_day(market_date, prices)
                except ValueError as err:
                    # Example: OMIE sent 24 prices instead of 96 in every series -> nothing is stored and the date
                    # is not marked as fetched, so the next run asks OMIE again. The job itself does not stop.
                    # (If only some series are wrong, _store_day keeps the good ones and does not raise.)
                    self._logger.error(
                        f"{self._LOG_PREFIX} Prices of {market_date} rejected: {err} "
                        f"Will try again in {self._refresh_interval_seconds}s."
                    )

            # Always done at the end, even when nothing was fetched: removes the prices that are too old
            self._purge(now_utc)

    def _dates_to_fetch(self, now_utc: datetime) -> List[date]:
        """Works out which market dates must be in memory now and are not.

        The current market day is always needed. The next day is needed after the publication time.

        Expected Input Structure:
        -------------------------
        now_utc = datetime(2026, 10, 15, 12, 10, tzinfo=timezone.utc)      # 14:10 in Madrid (summer time)

        Expected Output Structure:
        --------------------------
        [date(2026, 10, 16)]        # 15 Oct is already stored, 16 Oct is needed (after 13:00) and is missing
        []                          # nothing to fetch (before 13:00 with today stored, or both days stored)

        Args:
            now_utc (datetime): The current time in UTC.

        Returns:
            List[date]: Market dates still to fetch, in order. Example: [date(2026, 10, 16)]
        """
        # Current time in the market timezone, because the market days and the publication time are in Madrid time.
        # Example: now = 2026-10-15 12:10 UTC -> now_market = 2026-10-15 14:10 (Madrid, summer time)
        now_market = now_utc.astimezone(self._market_timezone)

        # The current market day is always needed. Example: [date(2026, 10, 15)]
        needed = [now_market.date()]

        # After the publication time (13:00 by default), OMIE should have the prices of the next day.
        # Example: 14:10 >= 13:00 -> needed = [date(2026, 10, 15), date(2026, 10, 16)]
        #          10:57 <  13:00 -> needed stays [date(2026, 10, 15)]
        if now_market.time() >= self._publication_time:
            needed.append(now_market.date() + timedelta(days=1))

        # Keep only the dates that are not stored yet, so a date is never fetched twice.
        # Example: needed = [15, 16 Oct] and 15 Oct is already stored -> returns [date(2026, 10, 16)]
        #          needed = [15, 16 Oct] and both are stored        -> returns [] (nothing to fetch)
        return [market_date for market_date in needed if market_date not in self._fetched_dates]

    def _store_day(self, market_date: date, series: Dict[str, List[float]]) -> None:
        """Converts the blocks of one market day to UTC timestamps and stores them, series by series.

        The blocks are counted in UTC from the start of the market day, never in local time. In the
        days when the clock changes, a local-time sum would put the blocks in the wrong hour.

        Each series is checked and stored on its own: a wrong series is rejected, but it never affects the
        others. Example: if 'es_spot_price' has the wrong number of prices, 'pt_spot_price' is still stored.
        The date is only marked as fetched when every series was valid, so a rejected series is fetched
        again in the next run.

        Expected Input Structure:
        -------------------------
        market_date = date(2026, 10, 15)
        series = {
            "pt_spot_price": [86.53, 85.1, ..., 91.2],          # EUR/MWh, 96 values (one per 15-minute block)
            "es_spot_price": [86.53, 85.1, ..., 91.2]
        }

        Expected Output Structure:
        --------------------------
        None. The prices are added to `self._prices`, by series and by UTC start of the block:
        {
            "pt_spot_price": {
                datetime(2026, 10, 14, 22, 0, tzinfo=UTC): 86.53,    # 00:00 Madrid time
                datetime(2026, 10, 14, 22, 15, tzinfo=UTC): 85.1,    # 00:15 Madrid time
                ...
            },
            "es_spot_price": {...}
        }
        If every series is valid, `date(2026, 10, 15)` is also added to `self._fetched_dates`.

        Args:
            market_date (date): The market day the prices belong to.
            series (Dict[str, List[float]]): One list of prices per series, one price per block.
                Example: {"pt_spot_price": [86.53, 85.1, ...], "es_spot_price": [86.53, 85.1, ...]}

        Raises:
            ValueError: If there are no series, or if none of them is valid (every series has a different
                number of prices from the blocks of the day). Nothing is stored in that case.
        """
        # --- Guard Clause 1: empty or invalid dictionary of series ---
        if not isinstance(series, dict) or not series:
            raise ValueError(f"no prices received for {market_date}.")

        # Start of the market day. Example: 2026-10-15 00:00 (Madrid time)
        day_start = datetime(market_date.year, market_date.month, market_date.day, tzinfo=self._market_timezone)

        # Start of the next market day. Example: 2026-10-16 00:00 (Madrid time)
        next_day_start = day_start + timedelta(days=1)

        # Measure the real length of the day by converting both midnights to UTC first.
        #
        # Why: when two datetimes share the same tzinfo (here, Europe/Madrid), Python subtracts
        # their wall-clock times and ignores any UTC offset change between them. Midnight to midnight
        # would always give 24 h, even on the days the clocks change. In UTC there are no clock
        # changes, so the subtraction gives the real elapsed time.
        #
        # Example (clocks go back on 2026-10-25, offset changes from +02:00 to +01:00):
        #   2026-10-25 00:00 Madrid (+02:00) = 2026-10-24 22:00 UTC
        #   2026-10-26 00:00 Madrid (+01:00) = 2026-10-25 23:00 UTC
        #   Difference in UTC = 25 h  (in Madrid time it would wrongly show 24 h)
        day_start_utc = day_start.astimezone(timezone.utc)
        next_day_start_utc = next_day_start.astimezone(timezone.utc)

        # OMIE sends one price for each block of 15 minutes (900 s), so the number of prices in a file depends
        # on the length of the day: day length in seconds // 900 = number of blocks.
        # The day is 24 h normally, 25 h on the last Sunday of October (clocks go back, one hour repeats)
        # and 23 h on the last Sunday of March (clocks go forward, one hour is skipped).
        # So: 24 h = 96 blocks, 25 h = 100 blocks, 23 h = 92 blocks. Example: 25 h = 90000 s, and 90000 // 900 = 100
        expected_blocks = int((next_day_start_utc - day_start_utc).total_seconds() // self._BLOCK_SECONDS)

        # Series stored, and what was wrong with the ones that were rejected
        # Example: stored = ['pt_spot_price'], problems = ["series 'es_spot_price' has 95 prices but 96 blocks were expected"]
        stored: List[str] = []
        problems: List[str] = []

        for series_name, prices in series.items():

            # --- Guard Clause 2: number of prices of this series ---
            # Compare the prices received with the blocks the day should have. A different number means the OMIE
            # file changed or is incomplete. Example: OMIE used to send hourly prices (24 per day); if that
            # happened again, they would be stored as 15-minute blocks and the price of 03:00 would be saved at
            # 00:45, with nobody noticing. Wrong prices are worse than no prices, so this series is not stored.
            # The other series are not affected: each one is checked on its own.
            if not isinstance(prices, (list, tuple)) or len(prices) != expected_blocks:
                received = len(prices) if isinstance(prices, (list, tuple)) else 0
                problems.append(
                    f"series '{series_name}' has {received} prices but {expected_blocks} blocks were expected"
                )
                continue

            # The blocks of this series. If the series is new, an empty dict is created and kept in the service
            # Example: first time 'es_spot_price' is stored -> self._prices['es_spot_price'] = {}
            series_blocks = self._prices.setdefault(series_name, {})

            # The file has no times, only a list: the first price is the block that starts at the beginning of the
            # day, the second one starts 15 minutes later, and so on. The start of each block is the start of the
            # day (in UTC) plus 'index' blocks of 900 s.
            # Example (2026-10-15, summer time): index 0 -> 2026-10-14 22:00 UTC (= 00:00 Madrid)
            #                                    index 1 -> 2026-10-14 22:15 UTC (= 00:15 Madrid)
            for index, price in enumerate(prices):
                block_start = day_start_utc + timedelta(seconds=index * self._BLOCK_SECONDS)

                # The key is the start of the block in UTC. 'float' makes sure the price is a number, not text.
                # If the same block already exists (for example, the date was fetched again), it is overwritten.
                series_blocks[block_start] = float(price)

            stored.append(series_name)

        # Nothing valid at all: the caller (refresh) logs the problems and tries again in the next run
        if not stored:
            raise ValueError(f"{market_date}: {'; '.join(problems)}.")

        # Some series were valid and some were not. The valid ones are already stored and can be used.
        # The date is NOT marked as fetched, so the next run asks OMIE again, and the rejected series
        # can be stored when the file is correct (the series that are already stored are only overwritten).
        if problems:
            self._logger.error(
                f"{self._LOG_PREFIX} {market_date}: {'; '.join(problems)}. Stored only {stored}; "
                f"the date will be fetched again."
            )
            return

        # Every series was valid. Remember that this date is stored, so the next refresh does not fetch it again
        self._fetched_dates.add(market_date)
        self._logger.info(
            f"{self._LOG_PREFIX} Stored {expected_blocks} prices of each series {sorted(stored)} for {market_date}."
        )

    def _purge(self, now_utc: datetime) -> None:
        """Removes prices older than the retention margin, and forgets the dates that are no longer useful.

        Expected Input Structure:
        -------------------------
        now_utc = datetime(2026, 10, 15, 14, 0, tzinfo=timezone.utc)       # with retention = 7200 s

        Expected Output Structure:
        --------------------------
        None. The stored data changes in place. Example:
            before: _prices["pt_spot_price"] has the blocks from 2026-10-15 11:45 UTC onwards
                    _fetched_dates = {date(2026, 10, 13), date(2026, 10, 14), date(2026, 10, 15), date(2026, 10, 16)}
            after:  _prices["pt_spot_price"] has the blocks from 2026-10-15 12:00 UTC onwards
                    _fetched_dates = {date(2026, 10, 14), date(2026, 10, 15), date(2026, 10, 16)}

        Args:
            now_utc (datetime): The current time in UTC.
        """
        # Oldest block that is still kept: now minus the retention margin.
        # Example: now = 2026-10-15 14:00 UTC and retention = 7200 s (2 h) -> limit = 2026-10-15 12:00 UTC
        limit = now_utc - self._retention

        # A new dict is built with the blocks that start at 'limit' or later, and replaces the old one
        # (the dict is not changed while it is being read, which would raise an error).
        # Example: the block of 2026-10-15 11:45 UTC is removed; the block of 12:00 UTC is kept.
        # The margin keeps the block that is running now: it started up to 15 minutes ago, so without a margin
        # (retention = 0) it would be removed, and the current price would be missing.
        # The same is done to each series. Example: {'pt_spot_price': {...}, 'es_spot_price': {...}}
        self._prices = {
            series_name: {block: price for block, price in blocks.items() if block >= limit}
            for series_name, blocks in self._prices.items()
        }

        # Yesterday, as a market date (Madrid time). Example: now = 2026-10-15 14:00 UTC -> yesterday = 2026-10-14
        yesterday = now_utc.astimezone(self._market_timezone).date() - timedelta(days=1)

        # Forget the market dates older than yesterday, so this set does not grow forever.
        # Example: with yesterday = 2026-10-14, the set {13, 14, 15, 16 Oct} becomes {14, 15, 16 Oct}.
        # This set only stops today and tomorrow from being fetched twice. Yesterday is never fetched again
        # (only today and tomorrow are needed), so keeping it is just a small safety margin.
        self._fetched_dates = {market_date for market_date in self._fetched_dates if market_date >= yesterday}

    # ------------------------------------------------------------------
    # Reading
    # ------------------------------------------------------------------
    def get_future_prices(self, now: Optional[datetime] = None) -> Dict[str, Dict[datetime, float]]:
        """Returns, for each series, the price of the current block and of all the next blocks that are known.

        Expected Input Structure:
        -------------------------
        now = datetime(2026, 10, 15, 13, 37, 42, tzinfo=timezone.utc)      # or None for the real time

        Args:
            now (datetime, optional): The current time (timezone-aware). Defaults to the real time.

        Returns:
            Dict[str, Dict[datetime, float]]: { series : { UTC block start : price in EUR/MWh } }, each one sorted
                by time. A series with no future price is left out, so it is empty when no price is known yet.
                Example:
                {
                    "pt_spot_price": {datetime(2026, 10, 15, 13, 0, tzinfo=UTC): 86.53, ...},
                    "es_spot_price": {datetime(2026, 10, 15, 13, 0, tzinfo=UTC): 86.53, ...}
                }
        """
        # Use the given time (tests) or the real one, always converted to UTC.
        # Example: now = None -> the current time, 2026-10-15 13:37:42 UTC
        now_utc = (now or datetime.now(timezone.utc)).astimezone(timezone.utc)

        # Seconds that have passed since the start of the current block. The blocks start at minutes 0, 15, 30
        # and 45 of each hour, so the remainder of (minutes x 60 + seconds) divided by 900 is the time inside the block.
        # Example: 13:37:42 -> (37 x 60 + 42) % 900 = 2262 % 900 = 462 s into the block
        seconds_into_block = (now_utc.minute * 60 + now_utc.second) % self._BLOCK_SECONDS

        # Start of the current block: the time now, without microseconds, minus the seconds already passed.
        # Example: 13:37:42 - 462 s = 13:30:00 (the block of 13:30 to 13:45)
        current_block = now_utc.replace(microsecond=0) - timedelta(seconds=seconds_into_block)

        # For each series, keep the current block and all the next ones. The blocks before the current one are in
        # the past, so they are left out. Each result is sorted by time.
        # Example: now = 13:37 UTC, so current_block = 13:30. Stored prices of one series (UTC):
        #   13:00 -> 86.5   (past, left out)
        #   13:15 -> 85.1   (past, left out)
        #   13:30 -> 84.0   (current block, kept)
        #   13:45 -> 83.2   (kept)
        #   ...           (all the other stored blocks are kept, up to the last one)
        # Result: {13:30: 84.0, 13:45: 83.2, ...}
        # The last stored block is the last one of tomorrow (23:45 Madrid time) after OMIE publishes
        # tomorrow's prices, and the last one of today before that.
        future_prices: Dict[str, Dict[datetime, float]] = {}
        for series_name, blocks in self._prices.items():
            future_blocks = {block: blocks[block] for block in sorted(blocks) if block >= current_block}

            # A series with nothing left is not returned
            if future_blocks:
                future_prices[series_name] = future_blocks

        return future_prices

    def get_future_prices_payload(self, now: Optional[datetime] = None) -> Dict[str, Dict[str, float]]:
        """Returns the future prices of every series, as the payload of an envelope.

        The service is the one that knows what it stores, so it is also the one that names the series
        ('pt_spot_price' and 'es_spot_price', the 'match_key' values that the source mapping rules can use)
        and writes the times as text. It does not choose a series: it returns both. The Input Handler only has
        to put this in an envelope and send it.

        Expected Input Structure:
        -------------------------
        now = datetime(2026, 10, 15, 13, 37, 42, tzinfo=timezone.utc)      # or None for the real time

        Expected Output Structure:
        --------------------------
        {
            "pt_spot_price": {                                   # EUR/MWh, by UTC start of the 15-minute block
                "2026-10-15T13:30:00+00:00": 84.0,
                "2026-10-15T13:45:00+00:00": 83.2
            },
            "es_spot_price": {
                "2026-10-15T13:30:00+00:00": 84.0,
                "2026-10-15T13:45:00+00:00": 83.2
            }
        }

        Args:
            now (datetime, optional): The current time (timezone-aware). Defaults to the real time.

        Returns:
            Dict[str, Dict[str, float]]: The payload, or an empty dict when no price is known yet,
                so the caller can skip sending it.
        """
        future_prices = self.get_future_prices(now)

        # The times become ISO 8601 text, because the envelope must be JSON-friendly.
        # Example: datetime(2026, 10, 15, 13, 30, tzinfo=UTC) -> "2026-10-15T13:30:00+00:00"
        # With no prices, future_prices is empty, so the result is {} and there is nothing to send.
        return {
            series_name: {block.isoformat(): price for block, price in blocks.items()}
            for series_name, blocks in future_prices.items()
        }

    # ------------------------------------------------------------------
    # Default fetcher
    # ------------------------------------------------------------------
    @staticmethod
    async def _fetch_with_pyomie(market_date: date) -> Dict[str, List[float]]:
        """Fetches the Spanish and Portuguese spot prices of one market date from OMIE, using the `pyomie` library.

        The imports are inside the function, so that the service can be used (and tested) without them.

        No Input Handler is used here on purpose: `pyomie` already does the HTTP request and parses the
        OMIE CSV, so going through an HTTP Input Handler would only repeat that work.

        Expected Input Structure:
        -------------------------
        market_date = date(2026, 10, 15)

        Expected Output Structure:
        --------------------------
        {
            "pt_spot_price": [226.0, 225.0, 224.0, 219.86, ..., 225.0, 220.0],    # EUR/MWh, 96 values
            "es_spot_price": [226.0, ...]
        }

        Args:
            market_date (date): The market day.

        Returns:
            Dict[str, List[float]]: One list per series, one price per 15-minute block, in EUR/MWh.
                Example: {"pt_spot_price": [226.0, 225.0, ...], "es_spot_price": [226.0, 225.0, ...]}

        Raises:
            aiohttp.ClientResponseError: With status 404 when OMIE has not published the date yet.
        """
        import aiohttp
        from pyomie.main import spot_price

        # `spot_price` makes the GET request (see "What OMIE returns") and parses the file. It returns an
        # `OMIEResults`, which has the data fetched (`contents`) and some information about the fetch.
        # Expected return structure (simplified):
        # OMIEResults(
        #     updated_at=datetime(2026, 10, 15, 8, 30, tzinfo=UTC),    # when the file was fetched
        #     market_date=date(2026, 10, 15),                           # the day the prices belong to
        #     contents=SpotData(
        #         url="https://www.omie.es/.../INT_PBC_EV_H_1_15_10_2026_15_10_2026.TXT",
        #         market_date="2026-10-15",
        #         header="...",                                         # first line of the file
        #         es_spot_price=[226.0, ...],                           # Spain, EUR/MWh, one per block
        #         pt_spot_price=[226.0, 225.0, 224.0, 219.86, ..., 225.0, 220.0],   # Portugal, EUR/MWh, one per block
        #         ...                                                   # the other series (power traded, in MW)
        #     ),
        #     raw="..."                                                 # the text of the file, as received
        # )
        # Only `contents.pt_spot_price` and `contents.es_spot_price` are used here.
        async with aiohttp.ClientSession() as session:
            results = await spot_price(session, market_date)

        # The OMIE file has the prices of the Spanish and of the Portuguese system (they can differ when the
        # interconnection is congested). Both are kept, and each parameter chooses the one it wants with the
        # 'match_key' of its rule ('pt_spot_price' or 'es_spot_price').
        # Example: on 2026-10-15 both were 226.0 EUR/MWh at 00:00, but on other days they can be different
        return {
            "pt_spot_price": list(results.contents.pt_spot_price),
            "es_spot_price": list(results.contents.es_spot_price)
        }