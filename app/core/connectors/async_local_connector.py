# --- Standard Library Imports ---
import asyncio
import inspect
from typing import Any, Callable, Optional


class AsyncLocalConnector:
    """Unified in-memory transport connector.

    Supports both buffered in-memory queue pipelines (for Input Handlers)
    and direct callable execution dispatching (for Output Handlers).

    The connector does not log. Errors are raised and whoever calls it decides what to do with them.

    How the queue works
    -------------------
    The queue ('asyncio.Queue') is a list in memory where the first payload to enter is the first to leave. It is
    the buffer between whoever produces payloads and whoever consumes them, so that neither has to wait for the
    other. It is used by the Input Handler:

        producer ── push(payload) ──►  [ p1 | p2 | p3 ]  ◄── await get() ── consumer (Input Handler)
                    (put_nowait)           the queue                          then task_done()

    - 'push()' puts a payload at the end of the queue and returns at once, without waiting. It never blocks the
      producer. If the payload cannot be added (the connector is closed, or the queue is full), it raises an error
      instead of dropping it silently.
    - 'get()' takes the payload at the start of the queue. If the queue is empty, it waits (without blocking the
      event loop) until a payload arrives, which is why the consumer is a loop of 'await get()'. With the connector
      closed, it raises a 'RuntimeError', like 'push()'.
    - 'task_done()' tells the queue that the payload taken with 'get()' was processed.
    - The size of the queue is the 'max_queue_size' of the configuration. Without it, the queue has no limit (the
      default of 'asyncio.Queue'), so it is never full, but the memory grows if the consumer is slower than the
      producer. With a limit, a 'push()' to a full queue raises 'asyncio.QueueFull'.
    - It is not thread-safe: 'push()' and 'get()' have to be called from the thread of the event loop that the
      queue belongs to.

    How the direct dispatch works
    -----------------------------
    'dispatch()' sends a payload by calling a function. That function is the 'target_callable' of the configuration:
    the connector keeps it, and each time the Output Handler has something to send, it calls 'dispatch(payload)', which
    calls 'target_callable(payload)' and gives back what the function returns. The function belongs to whoever
    receives the payload (e.g., the method of the next container that receives the data), so the payload goes from
    one place to the other with a normal function call, in memory, without passing through the network.

    A function cannot be written in a configuration file, so the 'target_callable' is not in the JSON: whoever
    creates the connector (e.g., the launcher) gives it in 'server_configurations'.

    Example: {"target_callable": next_container.receive_message}, where 'receive_message' is an 'async def' that
    receives the payload. After this, 'await connector.dispatch({"temperature": 21.5})' is the same as
    'await next_container.receive_message({"temperature": 21.5})', and returns what it returns.

        Output Handler ── dispatch(payload) ──►  target_callable(payload)  ──► result
                                                 (a function of the next container)

    - 'dispatch()' calls the 'target_callable' with the payload and returns what it returns.
    - If the 'target_callable' is a coroutine function ('async def'), it is awaited in the event loop. If it is a
      normal function, it is run in another thread ('asyncio.to_thread'), so that it does not block the event loop
      while it runs. In that case it runs outside the event loop, so it must not use objects that belong to the
      loop, such as the queue of this connector (which is not thread-safe).
    - Nothing is buffered: if the 'target_callable' is slow, the 'dispatch()' that called it waits for it. 
    - If the connector is closed, or has no 'target_callable', 'dispatch()' raises a 'RuntimeError'. An error of the
      'target_callable' itself goes up as it is.

    Who puts data in the queue (examples)
    -------------------------------------
    The queue is only filled by 'push()', which is called by the 'on_message' of the Local Input Handler. Who calls
    that 'on_message' depends on the case. There are always two different connectors: the one of the sender (which
    uses 'dispatch()') and the one of the receiver (which uses the queue).

    1. Data that is produced inside the application (the OMIE Telemetry System). There is no Output Handler: the job
       'send_prices' reads the prices and gives them to the Input Handler itself.

           job send_prices ── await input_handler.on_message(prices) ──► connector.push() ──► [queue] ──► consumer loop

    2. Two containers in the same process (e.g., Data Ingestion sends to Data Processing, in memory). The
       'target_callable' of the sender's connector is the 'on_message' of the receiver's Input Handler. So the
       'dispatch()' of one container ends in the 'push()' of the other:

           Container A                                         Container B
           Output Handler (connector A)                        Input Handler (connector B)
           dispatch(payload) ──► target_callable(payload) ──► on_message(payload) ──► push() ──► [queue] ──► consumer

       The 'on_message' is a coroutine function ('async def'), so the 'dispatch()' awaits it in the event loop. If it
       were a normal function, it would run in another thread, where the queue could not be used safely.

    How to use it: precautions
    --------------------------
    - Two roles, one class: the connector does not know if it was created for an Input Handler (queue) or for an
      Output Handler (direct dispatch), and it does not check. Give each one only what it needs: the input, the
      'max_queue_size' (and no 'target_callable'); the output, the 'target_callable'. An output without a
      'target_callable' is only noticed in the first 'dispatch()', which raises a 'RuntimeError'. Each connector
      belongs to ONE handler: do not share it between handlers (both would close it in their 'stop()').
    - Event loop and threads: 'push()' and 'get()' must be called from the thread of the event loop that the queue
      belongs to. Everything that touches the queue must run in that loop: give a coroutine function ('async def') as
      the 'target_callable' (the 'on_message' of the Input Handler is one). A normal function runs in another thread,
      where it must not use the queue or any other object of the loop.
    - The in-memory link between containers only works if both containers run in the SAME event loop (the same
      thread). The event loop is what runs the asynchronous tasks of a thread, and each thread has its own. In the
      single-process launcher, each container runs in its own thread, with its own loop:

          Thread A (loop A): Container A ── Output Handler ── dispatch() ──┐
                                                                           │ calls on_message(payload), which does
          Thread B (loop B): Container B ── queue ◄── push() ◄─────────────┘ the push, FROM THREAD A
                             consumer waiting in get() (loop B)

      The 'dispatch()' of A runs in thread A and calls the 'on_message' of B, so the queue of B is changed from
      thread A, and a queue is only safe when it is changed by the thread of the loop it belongs to (B). The
      consumer of B is waiting in 'get()' to be woken up by loop B, and a 'put_nowait' that comes from another
      thread puts the payload in the queue but may not wake it up: the payload stays there, stopped, until something
      else wakes loop B (or the queue is left in a wrong state). With both containers in the same loop, the call is
      made by the thread of the queue and it is safe. That is why the single-process launcher only accepts links
      over the network between containers.
    - Handle the errors: the connector does not log, so whoever calls it has to catch and log the errors, or the
      payload is lost without a trace. 'push()' raises 'RuntimeError' (closed) and 'asyncio.QueueFull' (full), and
      the payload is not kept or retried. 'dispatch()' raises 'RuntimeError' (closed, or no 'target_callable') and
      the errors of the 'target_callable' itself.
    - Set the size of the queue: without 'max_queue_size' the queue has no limit, and if the consumer is slower than
      the producer the memory grows until the process runs out of it. With a limit, a full queue refuses new payloads
      ('asyncio.QueueFull'), which are lost if the caller does not handle it.
    - One consumer per queue: each payload is taken by a single 'get()'. With two consumers on the same queue, each
      payload goes to only one of them, and the order in which they process them is not guaranteed.
    - The payload is not copied by the connector: the producer and the consumer hold the same object until it is
      processed. The Input Handler copies it only when it takes it from the queue (see 'PayloadNormalizer.from_local'),
      so the sender must not change the payload after 'push()' or 'dispatch()', or the message changes with it.
    - Memory only: nothing is stored on disk and there is no acknowledgement or retry. If the process stops, the
      payloads that were in the queue are lost, and a payload that was delivered ('push()' returned) is not known to
      have been processed.
    - Closing: call 'close()' when the handler stops. After it, 'push()' and 'dispatch()' raise an error. The payloads
      still in the queue stay there and a 'get()' that is waiting keeps waiting (a new 'get()' raises an error), so
      stop the consumer first, by
      cancelling its task.
    """

    _LOG_PREFIX: str = "Async Local Connector |"

    def __init__(self, server_configurations: dict) -> None:
        """Initializes the unified in-memory connector.

        Expected `server_configurations` Data Structure:
        -----------------------------------------------
        # For Output Handlers (Direct Execution Dispatch):
        {
            "target_callable": <Callable or Coroutine>  # Mandatory for outbound dispatch
        }

        # For Input Handlers (Buffered Queue Ingestion):
        {
            "max_queue_size": 10000                     # (int, optional) Maximum buffer capacity. Without it, the
                                                        # queue has no limit (the default of asyncio.Queue)
        }

        Args:
            server_configurations (dict): Configuration mapping for callable sinks or queue limits.

        Raises:
            ValueError: If 'target_callable' is not callable or 'max_queue_size' is negative.
            TypeError: If 'server_configurations' is not a dictionary or 'max_queue_size' is not an integer.
        """
        # --- Guard Clause 1: Validate server_configurations ---
        if not isinstance(server_configurations, dict):
            raise TypeError(
                f"{self._LOG_PREFIX} Initialization error: 'server_configurations' must be a dictionary, "
                f"got {type(server_configurations).__name__}."
            )

        self._is_connected = True

        # 1. Setup target callable sink (used by Output Handlers)
        target = server_configurations.get("target_callable")
        if target is not None:
            if not callable(target):
                raise ValueError(
                    f"{self._LOG_PREFIX} Initialization error: 'target_callable' must be an executable "
                    f"function or coroutine, got {type(target).__name__}."
                )
            self._target_callable: Optional[Callable] = target
        else:
            self._target_callable = None

        # 2. Setup buffered queue (used by Input Handlers)
        # 'max_queue_size' is optional. Only if it is configured it is passed to the queue, so that without it the
        # queue uses its own default (no limit: 'maxsize' 0), and the connector does not assume a value. A boolean is
        # not accepted, even though Python sees True as the integer 1. A value of 0 is the same as not having a limit.
        queue_options: dict = {}
        max_queue_size = server_configurations.get("max_queue_size")
        if max_queue_size is not None:
            if isinstance(max_queue_size, bool) or not isinstance(max_queue_size, int):
                raise TypeError(
                    f"{self._LOG_PREFIX} Initialization error: 'max_queue_size' must be an integer, "
                    f"got {type(max_queue_size).__name__}."
                )
            if max_queue_size < 0:
                raise ValueError(
                    f"{self._LOG_PREFIX} Initialization error: 'max_queue_size' cannot be negative, "
                    f"got {max_queue_size}."
                )
            queue_options["maxsize"] = max_queue_size

        self._queue: asyncio.Queue = asyncio.Queue(**queue_options)

    def is_connected(self) -> bool:
        """Indicates whether the local connector is active and open."""
        return self._is_connected

    # =========================================================================
    # Ingress / Queue Methods (Used by AsyncLocalInputHandler)
    # =========================================================================

    def push(self, payload: Any) -> None:
        """Pushes an incoming payload into the internal queue without blocking.

        The payload is not dropped silently: if it cannot be buffered, an error is raised and whoever calls decides
        what to do with it (e.g., log it).

        Args:
            payload (Any): The telemetry payload to buffer in memory.

        Raises:
            RuntimeError: If the connector is closed.
            asyncio.QueueFull: If the in-memory queue reached its capacity limit.
        """
        if not self._is_connected:
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot push payload: connector is closed.")

        # Puts the payload at the end of the queue ('self._queue' is an 'asyncio.Queue', a queue in memory where the
        # first payload to enter is the first to leave) and returns at once, without waiting. 'put_nowait' is the
        # version of 'put' that never waits, so it needs no 'await' and this method can be a normal function:
        # - If there is room, the payload is added and the Input Handler, which is waiting in 'get()' (see its
        #   consumer loop), wakes up and takes it.
        # - If the queue is full ('max_queue_size' payloads waiting), it raises 'asyncio.QueueFull' straight away. 'put'
        #   would wait for room instead, which is not wanted here: the caller is not supposed to be stopped by a slow
        #   consumer, and the payload is not dropped silently either, because the error goes up to the caller. Without
        #   'max_queue_size' the queue has no limit, so it is never full.
        # A queue of 'asyncio' is not thread-safe, so this must be called from the thread of the event loop that
        # the queue belongs to (calling it from another thread could leave the consumer in 'get()' without waking up).
        self._queue.put_nowait(payload)

    async def get(self) -> Any:
        """Pulls the next payload from the in-memory queue buffer.

        It waits until there is a payload, so it is meant to be called in a loop by the consumer (the Input Handler).

        Returns:
            Any: The dequeued payload object, e.g., {"pt_spot_price": {"2026-10-15T13:30:00+00:00": 84.0}}.

        Raises:
            RuntimeError: If the connector is closed when it is called. A 'get()' that was already waiting when the
                connector was closed is not woken up (see 'close()').
        """
        # --- Guard Clause 1: Checks that the connector is open, like 'push()' and 'dispatch()'. Without it, a 'get()'
        # called after the close would return a payload that was left in the queue or, with the queue empty, would wait
        # forever, because nobody is going to push anything any more. It only checks when it is called: it does not
        # wake up a 'get()' that was already waiting.
        if not self._is_connected:
            raise RuntimeError(f"{self._LOG_PREFIX} Cannot get payload: connector is closed.")

        # Takes the payload at the start of the queue (the oldest one, since the first to enter is the first to
        # leave) and removes it from the queue. If the queue is empty, the 'await' suspends this method, without
        # blocking the event loop (the other tasks keep running), until a 'push()' puts a payload in the queue. Then
        # it wakes up and returns that payload. It has no time limit: with nothing arriving, it waits forever, which
        # is why the consumer stops by cancelling the task that is waiting here (see the 'stop()' of the Input Handler).
        return await self._queue.get()

    def task_done(self) -> None:
        """Signals to the underlying queue that a previously enqueued task is complete.

        It has to be called once for each payload taken with 'get()', after it was processed (the consumer does it in
        a 'finally', so it is called even if the processing fails).

        It does not check if the connector is closed, on purpose: a payload taken before the 'close()' must still be
        marked as done, and an error in the 'finally' of the consumer would hide the error that was already going up.
        """
        # Tells the queue that the payload taken with the last 'get()' was processed. Taking it from the queue (with
        # 'get()') does not mean that it was processed: the queue only knows the payload left it. The queue keeps a
        # count of the payloads that were put and not yet marked as done, which is what 'asyncio.Queue.join()' waits
        # for (to wait until everything is processed). Nothing in this application calls 'join()', so here it only
        # keeps the count right. Calling it more times than payloads were taken raises a ValueError.
        self._queue.task_done()

    # =========================================================================
    # Egress / Dispatch Methods (Used by AsyncLocalOutputHandler)
    # =========================================================================

    async def dispatch(self, payload: Any) -> Any:
        """Dispatches payload directly to the registered callable sink target.

        Args:
            payload (Any): The payload to forward to the target callable.

        Returns:
            Any: The direct execution result returned by the target callable.

        Raises:
            RuntimeError: If dispatch is attempted when disconnected or without a target_callable.
        """
        if not self._is_connected or self._target_callable is None:
            raise RuntimeError(
                f"{self._LOG_PREFIX} Dispatch failed: connector is closed or no target_callable is configured."
            )

        # The 'target_callable' can be of two kinds, and each one is called in a different way. In both, the
        # 'dispatch()' waits until the function ends and returns what it returns (or raises its error), but the event
        # loop is never blocked while it waits: it keeps running the other tasks.
        if inspect.iscoroutinefunction(self._target_callable):
            # Case 1: a coroutine function ('async def'). It is called and awaited in the event loop itself. The
            # 'await' suspends this 'dispatch()' until the function reaches its 'return' (or raises an error), and
            # whenever the function waits for something ('await' inside it), the loop runs the other tasks. It
            # only waits for this function, not for what happens afterwards on the other side: e.g., if it only
            # puts the payload in a queue, it ends as soon as the payload is in the queue.
            # Because it runs in the event loop, it can use the objects that belong to the loop (e.g., the queue).
            return await self._target_callable(payload)
        else:
            # Case 2: a normal function ('def'). If it were called here, it would stop the whole event loop until it
            # ended (e.g., a function that takes 2 seconds would freeze every task for 2 seconds). So it is run in
            # another thread ('asyncio.to_thread'), and this 'dispatch()' waits for the thread to finish, also with
            # an 'await', so the loop stays free meanwhile. There is no time limit: if the function never ends, this
            # 'dispatch()' waits forever. Because it runs outside the event loop, it must not use the objects that
            # belong to the loop, such as the queue (which is not thread-safe).
            return await asyncio.to_thread(self._target_callable, payload)

    # =========================================================================
    # Lifecycle Teardown
    # =========================================================================

    async def close(self) -> None:
        """Releases internal resources and closes the connector.

        After it, 'is_connected()' is False, and 'push()', 'get()' and 'dispatch()' raise a 'RuntimeError'. It can be
        called more than once: the next calls do nothing new. The payloads that are still in the queue are not
        removed, and a 'get()' that is already waiting is not woken up (the consumer is stopped by cancelling its
        task, see the 'stop()' of the Input Handler).
        """
        # There is nothing to close on a network, so there is nothing to await here. It is a coroutine function only
        # to have the same methods as the other connectors, so that the handlers close any connector in the same way.

        # Marks the connector as closed. It is what 'push()', 'get()', 'dispatch()' and 'is_connected()' look at, so
        # from now on a 'push()', a 'get()' or a 'dispatch()' raises an error and 'is_connected()' says False. The
        # queue itself is not touched: its payloads stay there, and whoever is already waiting in 'get()' keeps waiting.
        self._is_connected = False

        # Drops the reference to the function of the other container. The connector does not need it any more (a
        # 'dispatch()' after the close raises an error), and keeping it would keep that function (and what it
        # references, e.g., the Input Handler of the other container) in memory for as long as this connector exists.
        self._target_callable = None