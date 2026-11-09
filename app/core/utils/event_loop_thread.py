# --- Standard Library Imports ---
import asyncio
import threading
from typing import Any, Coroutine, Optional, TypeVar

T = TypeVar("T")


class EventLoopThread:
    """A thread that runs its own asyncio event loop, where coroutines of other threads can be executed.

    An asyncio event loop belongs to the thread that runs it, and the objects created in it (connections, locks,
    queues, tasks, schedulers) can only be used inside that same loop. So, to run a part of the application in a
    separate thread, that part has to be launched, used and stopped INSIDE the loop of that thread. This class gives
    the other threads (here, the main thread of the application) a way to do it:

        thread = EventLoopThread("inference_R-H-01")
        thread.start()
        result = await thread.run(launcher.launch_environment(...))      # runs in the thread, not in the caller's loop
        await thread.close()

    While the thread is alive, everything that the coroutines started in the background (e.g., the consumer of a
    RabbitMQ queue or a periodic request) keeps running in its loop.

    The thread is a daemon thread, so that a bug that skips `close` does not keep the program from ending.
    """

    def __init__(self, name: str) -> None:
        """Prepares the thread. It only starts running when `start` is called.

        Args:
            name (str): Name of the thread, shown in the logs and in debuggers (e.g., "inference_R-H-01").
        """
        self._name: str = name
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._thread: Optional[threading.Thread] = None
        self._ready = threading.Event()
        self._closed: bool = False

    @property
    def name(self) -> str:
        """str: Name of the thread."""
        return self._name

    @property
    def is_running(self) -> bool:
        """bool: True if the thread was started and not closed yet."""
        return self._loop is not None and not self._closed

    def start(self) -> None:
        """Starts the thread and waits until its event loop is running.

        Raises:
            RuntimeError: If the thread was already started.
        """
        # Guard Clause: Checks that the thread was not started before (a thread cannot be started twice).
        if self._thread is not None:
            raise RuntimeError(f"Event Loop Thread '{self._name}' was already started.")

        self._thread = threading.Thread(target=self._run_loop, name=self._name, daemon=True)
        self._thread.start()
        self._ready.wait()

    def _run_loop(self) -> None:
        """Body of the thread: creates the event loop, runs it until `close` stops it, and then closes it."""
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        self._loop = loop
        self._ready.set()
        try:
            loop.run_forever()
        finally:
            loop.close()

    async def run(self, coroutine: Coroutine[Any, Any, T]) -> T:
        """Runs a coroutine in the event loop of the thread and waits for its result without blocking the caller's loop.

        Args:
            coroutine (Coroutine): The coroutine to run (e.g., `launcher.launch_environment(...)`). It is executed in the
                thread, so everything it creates belongs to the loop of the thread.

        Returns:
            Any: What the coroutine returns.

        Raises:
            RuntimeError: If the thread was not started or was already closed.
            Exception: Any exception raised by the coroutine.
        """
        # Guard Clause: Checks that the loop of the thread is running, so that the coroutine has where to run. The
        # coroutine is closed to avoid the warning "coroutine was never awaited".
        if not self.is_running:
            coroutine.close()
            raise RuntimeError(f"Event Loop Thread '{self._name}' is not running.")

        future = asyncio.run_coroutine_threadsafe(coroutine, self._loop)
        return await asyncio.wrap_future(future)

    async def close(self) -> None:
        """Cancels what is still running in the loop of the thread, stops the loop and waits for the thread to end.

        It does nothing if the thread was not started or was already closed.
        """
        # Guard Clause: Checks that there is something to close.
        if not self.is_running:
            return
        self._closed = True

        try:
            await asyncio.wrap_future(asyncio.run_coroutine_threadsafe(self._cancel_remaining_tasks(), self._loop))
        finally:
            self._loop.call_soon_threadsafe(self._loop.stop)
            # 'join' blocks, so it runs in another thread to not freeze the loop of the caller
            await asyncio.to_thread(self._thread.join)

    @staticmethod
    async def _cancel_remaining_tasks() -> None:
        """Cancels the tasks that the loop still has (except this one) and waits for them to end."""
        current = asyncio.current_task()
        tasks = [task for task in asyncio.all_tasks() if task is not current]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.get_running_loop().shutdown_asyncgens()
