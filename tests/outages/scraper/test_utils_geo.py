# test_utils_http.py

import asyncio
from unittest.mock import AsyncMock, patch, MagicMock

import pytest

from outages.scraper.utils_http import (
    parse_proxy,
    ProxyWorker,
    TaskManager,
    NoContentError,
    proxy_list
)
import httpx
import contextlib

# Test parse_proxy function
def test_parse_proxy_valid():
    proxy_str = "127.0.0.1:8080:user:pass"
    expected = "socks5://user:pass@127.0.0.1:8080"
    assert parse_proxy(proxy_str) == expected

def test_parse_proxy_missing_parts():
    proxy_str = "127.0.0.1:8080"
    assert parse_proxy(proxy_str) is None

def test_parse_proxy_empty_string():
    proxy_str = ""
    assert parse_proxy(proxy_str) is None

def test_parse_proxy_invalid_format():
    proxy_str = "invalid:proxy:string"
    assert parse_proxy(proxy_str) is None

# Async tests require the pytest-asyncio marker
@pytest.mark.asyncio
async def test_proxy_worker_fetch_success():
    # Mock fetch_func to return a successful result
    async def mock_fetch_func(url, client):
        return 'success'

    # Create a queue and add a task
    queue = asyncio.PriorityQueue()
    future = asyncio.get_event_loop().create_future()
    await queue.put((1, future, mock_fetch_func, ('http://example.com',), {}))

    # Create a ProxyWorker with a mocked httpx.AsyncClient
    with patch('httpx.AsyncClient') as MockClient:
        async with contextlib.AsyncExitStack() as stack:
            worker = ProxyWorker(proxy=None, queue=queue)
            await stack.enter_async_context(worker)
            # Let the worker process the queue
            await asyncio.sleep(0.1)

            # Check the result
            result = await future
            assert result == ('success', 200)

@pytest.mark.asyncio
async def test_proxy_worker_fetch_http_error():
    # Mock fetch_func to raise an HTTPStatusError
    async def mock_fetch_func(url, client):
        response = httpx.Response(404)
        raise httpx.HTTPStatusError('Not Found', request=None, response=response)

    # Create a queue and add a task
    queue = asyncio.PriorityQueue()
    future = asyncio.get_event_loop().create_future()
    await queue.put((1, future, mock_fetch_func, ('http://example.com',), {}))

    # Create a ProxyWorker
    with patch('httpx.AsyncClient') as MockClient:
        async with contextlib.AsyncExitStack() as stack:
            worker = ProxyWorker(proxy=None, queue=queue)
            await stack.enter_async_context(worker)
            # Let the worker process the queue
            await asyncio.sleep(0.1)

            # Check the result
            result = await future
            assert result == (None, 404)

@pytest.mark.asyncio
async def test_proxy_worker_fetch_request_error():
    # Mock fetch_func to raise a RequestError
    async def mock_fetch_func(url, client):
        raise httpx.RequestError('Request failed')

    # Create a queue and add a task
    queue = asyncio.PriorityQueue()
    future = asyncio.get_event_loop().create_future()
    await queue.put((1, future, mock_fetch_func, ('http://example.com',), {}))

    # Create a ProxyWorker
    with patch('httpx.AsyncClient') as MockClient:
        async with contextlib.AsyncExitStack() as stack:
            worker = ProxyWorker(proxy=None, queue=queue)
            await stack.enter_async_context(worker)
            # Let the worker process the queue
            await asyncio.sleep(0.1)

            # Check the result
            result = await future
            assert result == (None, 'httpx.RequestError')

@pytest.mark.asyncio
async def test_task_manager_submit_task():
    # Mock fetch_func to return a successful result
    async def mock_fetch_func(url, client):
        return 'success'

    # Create a TaskManager
    manager = TaskManager(proxies=[None])  # No proxies for testing
    async with manager:
        # Submit a task
        future = await manager.submit_task(1, mock_fetch_func, 'http://example.com')
        # Wait for the task to complete
        result = await future
        assert result == ('success', 200)

@pytest.mark.asyncio
async def test_task_manager_multiple_tasks():
    # Mock fetch_func to return a successful result
    async def mock_fetch_func(url, client):
        return url

    urls = [f'http://example.com/{i}' for i in range(5)]
    tasks = []

    # Create a TaskManager
    manager = TaskManager(proxies=[None])  # No proxies for testing
    async with manager:
        # Submit multiple tasks
        for priority, url in enumerate(urls):
            future = await manager.submit_task(priority, mock_fetch_func, url)
            tasks.append(future)

        # Wait for all tasks to complete
        results = await asyncio.gather(*tasks)
        expected_results = [(url, 200) for url in urls]
        assert results == expected_results

@pytest.mark.asyncio
async def test_proxy_worker_backoff():
    # Mock fetch_func to fail the first two times, then succeed
    call_count = 0
    async def mock_fetch_func(url, client):
        nonlocal call_count
        call_count += 1
        if call_count < 3:
            raise httpx.RequestError('Temporary failure')
        return 'success'

    # Create a queue and add a task
    queue = asyncio.PriorityQueue()
    future = asyncio.get_event_loop().create_future()
    await queue.put((1, future, mock_fetch_func, ('http://example.com',), {}))

    # Create a ProxyWorker
    with patch('httpx.AsyncClient') as MockClient:
        async with contextlib.AsyncExitStack() as stack:
            worker = ProxyWorker(proxy=None, queue=queue)
            await stack.enter_async_context(worker)
            # Let the worker process the queue
            await asyncio.sleep(1)  # Increase sleep time to allow backoff retries

            # Check the result
            result = await future
            assert result == ('success', 200)
            assert call_count == 3  # Ensures backoff retries happened

@pytest.mark.asyncio
async def test_task_manager_wait_for_completion():
    # Mock fetch_func to return a successful result
    async def mock_fetch_func(url, client):
        await asyncio.sleep(0.1)  # Simulate network delay
        return url

    urls = [f'http://example.com/{i}' for i in range(3)]
    tasks = []

    # Create a TaskManager
    manager = TaskManager(proxies=[None])  # No proxies for testing
    async with manager:
        # Submit multiple tasks
        for priority, url in enumerate(urls):
            future = await manager.submit_task(priority, mock_fetch_func, url)
            tasks.append(future)

        # Wait for the queue to be fully processed
        await manager.wait_for_completion()

        # Ensure all tasks are completed
        results = [task.result() for task in tasks]
        expected_results = [(url, 200) for url in urls]
        assert results == expected_results
