import os
from enum import Enum, IntEnum
from typing import Optional, List, Any, Callable, Tuple
import asyncio
import random

from aiolimiter import AsyncLimiter
import backoff
import httpx
from loguru import logger
from dotenv import load_dotenv

from outages.scraper.dashboard import DashboardApp

MAX_REQUESTS_PER_SECOND = 2    # Maximum requests per second per proxy
MAX_REQUESTS_PER_MINUTE = 60   # Maximum requests per minute per proxy
MAX_REQUESTS_PER_HOUR = 1000  # Maximum requests per hour per proxy
MAX_CONCURRENT_REQUESTS_PER_PROXY = 1  # Maximum concurrent requests per proxy
MAX_RETRIES = 5  # Maximum number of retries for each request
SLEEP_BETWEEN_RETRIES = 5  # Sleep time between retries in seconds
MAX_ACTIVE_PROXIES = 10  # Maximum number of active proxies

# Load environment variables from the .env file
load_dotenv()

# Get proxy list from environment variables and split into a list
proxy_list_raw = os.getenv("SOCKS5_PROXIES", "").split(",")


def parse_proxy(proxy_str: str) -> Optional[str]:
    """
    Parse the proxy string and format it as a SOCKS5 proxy URL with authentication.

    The input format is:
    IP:Port:Username:Password

    Args:
        proxy_str (str): The proxy string to parse.

    Returns:
        Optional[str]: The formatted proxy URL or None if parsing fails.
    """
    # logger.debug(f"Parsing proxy string: {proxy_str}")
    try:
        ip, port, username, password = proxy_str.strip().split(":")
        return f"socks5://{username}:{password}@{ip}:{port}"
    except ValueError as e:
        logger.error(f"Invalid format of {proxy_str}: {e}")
        # return None
        raise e


# Parse and build the proxies list for SOCKS5
proxy_list = [parse_proxy(proxy) for proxy in proxy_list_raw if proxy.strip()]

# Filter out any None values from failed parsing
proxy_list = [proxy for proxy in proxy_list if proxy] if proxy_list else [None]

class NoContentError(Exception):
    """Custom exception for handling 204 No Content responses."""
    pass

DEFAULT_EXCEPTIONS_TO_CATCH = (httpx.HTTPStatusError, httpx.RequestError, NoContentError)

# Global rate limiter for 10 requests per second across all proxies
global_limiter = AsyncLimiter(10, 1)

# Create a global priority queue
global_queue = asyncio.PriorityQueue()

# Define priority levels for the queue
class PriorityLevels(IntEnum):
    """
    Priority levels for the queue. Lower values indicate higher priority.
    """
    COUNTY = 1
    REGION = 2
    EVENT = 3

class QueueItem:
    """
    QueueItem class represents an item in a priority queue.

    Attributes:
        priority (PriorityLevels): The priority of the queue item.
        future (asyncio.Future): A future object associated with the queue item.
        fetch_func (callable): The function to be executed.
        args (tuple): The positional arguments to be passed to fetch_func.
        kwargs (dict): The keyword arguments to be passed to fetch_func.
        retry_count (int): Number of times the task has been retried.
    Methods:
        __init__(priority, future, fetch_func, args, kwargs):
            Initializes a QueueItem with the given priority, future, fetch_func, args, and kwargs.
        __lt__(other):
            Compares the priority of this QueueItem with another QueueItem.
    """

    def __init__(
        self,
        priority: PriorityLevels,
        future: asyncio.Future,
        fetch_func: Callable,
        args: Tuple[Any, ...],
        kwargs: dict,
        retry_count: int = 0
    ) -> None:
        """
        Initialize a QueueItem.

        Args:
            priority (PriorityLevels): The priority of the queue item.
            future (asyncio.Future): A future object associated with the queue item.
            fetch_func (Callable): The function to be executed.
            args (Tuple[Any, ...]): The positional arguments to be passed to fetch_func.
            kwargs (dict): The keyword arguments to be passed to fetch_func.
            retry_count (int): The number of retries this task has had.
        """
        self.priority = priority
        self.future = future
        self.fetch_func = fetch_func
        self.args = args
        self.kwargs = kwargs
        self.retry_count = retry_count

    # Comparison operators compare only based on priority
    def __lt__(self, other):
        return self.priority < other.priority

class ProxyWorker:
    """
    A class to manage requests through a specific proxy with rate limiting and backoff.

    Attributes:
        proxy (str): The proxy URL.
        semaphore (asyncio.Semaphore): Semaphore to limit concurrent requests per proxy.
        second_limiter (AsyncLimiter): Rate limiter to limit requests per second per proxy.
        minute_limiter (AsyncLimiter): Rate limiter to limit requests per minute per proxy.
        ten_minute_limiter (AsyncLimiter): Rate limiter to limit requests per 10 minutes per proxy.
        backoff_decorator: Backoff decorator to handle retries per proxy.
        client (httpx.AsyncClient): HTTP client for making requests.
    """

    def __init__(
        self,
        index: int,
        proxy: str,
        queue: asyncio.PriorityQueue,
        manager: 'TaskManager',
        max_concurrent_requests: int = MAX_CONCURRENT_REQUESTS_PER_PROXY,
        max_requests_per_second: int = MAX_REQUESTS_PER_SECOND,
        max_requests_per_minute: int = MAX_REQUESTS_PER_MINUTE,
        max_requests_per_hour: int = MAX_REQUESTS_PER_HOUR,
        max_retries: int = MAX_RETRIES,
        exceptions_to_catch: tuple = DEFAULT_EXCEPTIONS_TO_CATCH,
        dashboard: DashboardApp = None,
    ):
        """
        Initialize the ProxyWorker.

        Args:
            index (int): The index of the worker.
            proxy (str): The proxy URL.
            queue (asyncio.PriorityQueue): The global task queue.
            manager (TaskManager): The TaskManager instance.
            max_concurrent_requests (int): Maximum concurrent requests per proxy.
            max_requests_per_second (int): Maximum requests per second per proxy.
            max_requests_per_minute (int): Maximum requests per minute per proxy.
            max_requests_per_hour (int): Maximum requests per hour per proxy.
            max_retries (int): Maximum number of retries for each request.
            exceptions_to_catch (tuple): Exceptions to catch and backoff on.
            dashboard (DashboardApp): The dashboard app instance.
        """
        
        self.index = index
        self.proxy = proxy
        self.queue = queue
        self.semaphore = asyncio.Semaphore(max_concurrent_requests)
        self.second_limiter = AsyncLimiter(max_requests_per_second, 1)
        self.minute_limiter = AsyncLimiter(max_requests_per_minute, 60)
        self.hour_limiter = AsyncLimiter(max_requests_per_hour, 3600) # Hour
        self.backoff_count = 0
        self.backoff_seconds = 0
        self.dashboard = dashboard
        self.manager = manager  # Reference to the TaskManager
        
        self.run_task = None
        self.client = None  # Will be initialized in start()

    async def __aenter__(self):
        await self.start()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        await self.stop()

    async def start(self):
        """Start the worker by initializing the client and the run task."""
        
        self.client = httpx.AsyncClient(proxies=self.proxy)
        self.run_task = asyncio.create_task(self.run())

    async def stop(self):
        """Stop the worker by cancelling the run task and closing the client."""
        
        if self.run_task:
            self.run_task.cancel()
            try:
                await self.run_task
            except asyncio.CancelledError:
                pass
        if self.client:
            await self.client.aclose()

    async def _on_backoff(self, details):
        """
        Handler for the backoff event.

        Args:
            details (dict): Backoff details, including `wait`.
        """
        self.backoff_count += 1
        self.backoff_seconds = details['wait']
        if self.dashboard:
            self.dashboard.send_update(self.index, -1, f"Backoff: {details['tries']} tries, waiting {details['wait']}s")

    async def run(self):
        try:
            while True:
                queue_item = await self.queue.get()
                try:
                    result = await self._fetch_internal(queue_item.fetch_func, *queue_item.args, **queue_item.kwargs)
                    queue_item.future.set_result(result)
                except Exception as e:
                    logger.warning(f"Task with priority {queue_item.priority} failed: {e}")
                    # Resubmit the task to the task manager if not exceeded max retries
                    if queue_item.retry_count < MAX_RETRIES:
                        queue_item.retry_count += 1
                        # Sleep for a random duration around the base sleep time to avoid synchronized retries
                        # The factor (1 + 0.5 * (2 * random.random() - 1)) introduces a random jitter
                        # around the base sleep time (SLEEP_BETWEEN_RETRIES). This helps to prevent
                        # synchronized retries from multiple workers, which could lead to a thundering herd problem.
                        # 
                        # Explanation of parameters:
                        # 1: Base multiplier, ensuring the sleep time is at least the base sleep time.
                        # 0.5: Amplitude of the jitter, allowing the sleep time to vary by ±50% of the base sleep time.
                        # 2: Scales the random value to the range [-1, 1].
                        # -1: Shifts the random value to the range [-0.5, 0.5].
                        # around the base sleep time (SLEEP_BETWEEN_RETRIES). This helps to prevent
                        # synchronized retries from multiple workers, which could lead to a thundering herd problem.
                        await asyncio.sleep(SLEEP_BETWEEN_RETRIES * (1 + 0.5 * (2 * random.random() - 1)))
                        await self.queue.put(queue_item)
                    else:
                        queue_item.future.set_exception(e)
                    # Handle worker failure
                    await self.manager.handle_worker_failure(self)
                    return  # Exit the run loop after handling failure
                finally:
                    self.queue.task_done()
        except asyncio.CancelledError:
            pass

    async def _fetch_internal(
        self,
        fetch_func: Callable,
        *args,
        **kwargs
    ):
        """
        Internal method to fetch data using rate limiting and backoff.

        Args:
            fetch_func (Callable): The function to call to perform the request.
            *args: Arguments to pass to the function.
            **kwargs: Keyword arguments to pass to the function.

        Returns:
            The result of the function call.
        """
        
        # if not global_limiter.has_capacity():
            # logger.warning("Global rate limit exceeded, waiting...")
        async with global_limiter:  # Global rate limiter across all proxies
            async with self.semaphore:  # Concurrency limit per proxy
                # Enforce per-second, per-minute, and per-10-minute limits
                if not self.second_limiter.has_capacity() and self.dashboard:
                    self.dashboard.send_update(self.index, -2, f"Backoff {self.backoff_seconds}s. Rate limit exceeded (per second), waiting...")
                async with self.second_limiter:
                    if not self.minute_limiter.has_capacity() and self.dashboard:
                        self.dashboard.send_update(self.index, -3, f"Backoff {self.backoff_seconds}s. Rate limit exceeded (per minute), waiting...")
                    async with self.minute_limiter:
                        if not self.hour_limiter.has_capacity() and self.dashboard:
                            self.dashboard.send_update(self.index, -4, f"Backoff {self.backoff_seconds}s. Rate limit exceeded (per hour), waiting...")  
                        async with self.hour_limiter:
                            try:
                                decorated_fetch = self.backoff_decorator(fetch_func)
                                response = await decorated_fetch(*args, client=self.client, **kwargs)
                                if self.dashboard:
                                    self.dashboard.send_update(self.index, 200, f"Backoff {self.backoff_seconds}s.")
                                
                                return response, 200  # HTTP 200 OK for success
                            except httpx.HTTPStatusError as e:
                                # Return actual HTTP status code
                                #TODO convert the status code to the StatusCodes enum
                                if self.dashboard:
                                    self.dashboard.send_update(self.index, e.response.status_code, f"Backoff {self.backoff_seconds}s.")
                                logger.debug(f"Fetch failed with proxy {self.proxy}: {e}")
                                raise
                            except NoContentError:
                                # HTTP 204 No Content
                                if self.dashboard:
                                    self.dashboard.send_update(self.index, 204, f"Backoff {self.backoff_seconds}s.")
                                logger.debug(f"Fetch failed with proxy {self.proxy}: 204 No Content")
                                raise
                            except httpx.RequestError:
                                # Request failed, status code httpx.RequestError
                                if self.dashboard:
                                    self.dashboard.send_update(self.index, -5, f"Backoff {self.backoff_seconds}s.")
                                logger.debug(f"Fetch failed with proxy {self.proxy}: RequestError")
                                raise

    @property
    def backoff_decorator(self):
        return backoff.on_exception(
            backoff.expo,  # Exponential backoff
            exception=DEFAULT_EXCEPTIONS_TO_CATCH,  # Retry on these exceptions
            max_tries=MAX_RETRIES,  # Maximum number of retries
            jitter=backoff.full_jitter,  # Add randomness to prevent synchronized retries
            on_backoff=self._on_backoff
        )

class TaskManager:
    """
    TaskManager class to manage the global task queue and ProxyWorkers.
    """

    def __init__(self, proxies: List[str], dashboard: DashboardApp):
        
        self.queue = asyncio.PriorityQueue()
        self.proxies = proxies
        self.dashboard = dashboard
        self.max_active_proxies = MAX_ACTIVE_PROXIES
        self.workers_active = []
        self.workers_inactive = asyncio.Queue()
        self.worker_lock = asyncio.Lock()

    async def __aenter__(self):
        
        for i, proxy in enumerate(self.proxies):
            worker = ProxyWorker(
                index=i,
                proxy=proxy,
                queue=self.queue,
                manager=self,
                dashboard=self.dashboard,
            )
            if i < self.max_active_proxies:
                await worker.start()
                self.workers_active.append(worker)
            else:
                await self.workers_inactive.put(worker)
        return self

    async def __aexit__(self, exc_type, exc, tb):
        
        async with self.worker_lock:
            for worker in self.workers_active:
                await worker.stop()
            while not self.workers_inactive.empty():
                worker = await self.workers_inactive.get()
                # No need to stop inactive workers as they haven't been started
                pass

    async def submit_task(self, priority: PriorityLevels, fetch_func: Callable, *args, **kwargs) -> asyncio.Future:
        """
        Submit a task to the global queue.

        Args:
            priority (int): The priority of the task (lower value means higher priority).
            fetch_func (Callable): The function to execute.
            *args: Positional arguments for fetch_func.
            **kwargs: Keyword arguments for fetch_func.

        Returns:
            asyncio.Future: A future that will be set when the task is completed.
        """
        
        future = asyncio.Future()
        queue_item = QueueItem(priority, future, fetch_func, args, kwargs)
        await self.queue.put(queue_item)
        return await future

    async def wait_for_completion(self):
        
        await self.queue.join()

    async def handle_worker_failure(self, worker: ProxyWorker):
        
        async with self.worker_lock:
            if worker in self.workers_active:
                logger.debug(f"Worker {worker.proxy} failed, stopping...")
                self.workers_active.remove(worker)
                await worker.stop()
                await self.workers_inactive.put(worker)
                if not self.workers_inactive.empty():
                    new_worker = await self.workers_inactive.get()
                    await new_worker.start()
                    self.workers_active.append(new_worker)

def create_task_manager(dashboard: DashboardApp) -> TaskManager:
    """
    Creates a TaskManager instance with the given proxies.

    Returns:
        TaskManager: An instance of TaskManager.
    """
    
    manager = TaskManager(
        proxies=proxy_list,
        dashboard=dashboard,
    )
    return manager
