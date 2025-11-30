import time
from abc import ABC, abstractmethod
from io import BytesIO
import aiohttp
import httpx
import pycurl
import requests
import urllib3

from setup_virtuoso import get_sparql_endpoint

TIMEOUT_TOTAL = 30.0
TIMEOUT_CONNECT = 5.0
TIMEOUT_READ = 25.0

QUERY_HEADERS = {"Accept": "application/sparql-results+json"}
CONSTRUCT_HEADERS = {"Accept": "text/turtle"}
UPDATE_HEADERS = {"Content-Type": "application/sparql-update"}


class SPARQLPackage(ABC):
    """Base class for SPARQL benchmark packages."""

    name: str

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def teardown(self) -> None:
        pass

    @abstractmethod
    def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        """Execute SPARQL query. Returns (response_body, elapsed_time)."""
        pass

    @abstractmethod
    def update(self, sparql: str) -> float:
        """Execute SPARQL update. Returns elapsed_time."""
        pass


class AsyncSPARQLPackage(ABC):
    """Base class for async SPARQL benchmark packages."""

    name: str

    @abstractmethod
    async def setup(self) -> None:
        pass

    @abstractmethod
    async def teardown(self) -> None:
        pass

    @abstractmethod
    async def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        pass

    @abstractmethod
    async def update(self, sparql: str) -> float:
        pass


class HttpxSyncPackage(SPARQLPackage):
    name = "httpx_sync"

    def setup(self) -> None:
        self.client = httpx.Client(timeout=TIMEOUT_TOTAL)
        self.endpoint = get_sparql_endpoint()

    def teardown(self) -> None:
        self.client.close()

    def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        headers = CONSTRUCT_HEADERS if is_construct else QUERY_HEADERS
        start = time.perf_counter()
        response = self.client.post(
            self.endpoint,
            data={"query": sparql},
            headers=headers,
        )
        elapsed = time.perf_counter() - start
        return response.content, elapsed

    def update(self, sparql: str) -> float:
        start = time.perf_counter()
        self.client.post(
            self.endpoint,
            content=sparql,
            headers=UPDATE_HEADERS,
        )
        return time.perf_counter() - start


class HttpxAsyncPackage(AsyncSPARQLPackage):
    name = "httpx_async"

    async def setup(self) -> None:
        self.client = httpx.AsyncClient(timeout=TIMEOUT_TOTAL)
        self.endpoint = get_sparql_endpoint()

    async def teardown(self) -> None:
        await self.client.aclose()

    async def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        headers = CONSTRUCT_HEADERS if is_construct else QUERY_HEADERS
        start = time.perf_counter()
        response = await self.client.post(
            self.endpoint,
            data={"query": sparql},
            headers=headers,
        )
        elapsed = time.perf_counter() - start
        return response.content, elapsed

    async def update(self, sparql: str) -> float:
        start = time.perf_counter()
        await self.client.post(
            self.endpoint,
            content=sparql,
            headers=UPDATE_HEADERS,
        )
        return time.perf_counter() - start


class AiohttpPackage(AsyncSPARQLPackage):
    name = "aiohttp"

    async def setup(self) -> None:
        timeout = aiohttp.ClientTimeout(total=TIMEOUT_TOTAL)
        self.session = aiohttp.ClientSession(timeout=timeout)
        self.endpoint = get_sparql_endpoint()

    async def teardown(self) -> None:
        await self.session.close()

    async def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        headers = CONSTRUCT_HEADERS if is_construct else QUERY_HEADERS
        start = time.perf_counter()
        async with self.session.post(
            self.endpoint,
            data={"query": sparql},
            headers=headers,
        ) as response:
            content = await response.read()
        elapsed = time.perf_counter() - start
        return content, elapsed

    async def update(self, sparql: str) -> float:
        start = time.perf_counter()
        async with self.session.post(
            self.endpoint,
            data=sparql,
            headers=UPDATE_HEADERS,
        ) as response:
            await response.read()
        return time.perf_counter() - start


class RequestsPackage(SPARQLPackage):
    name = "requests"

    def setup(self) -> None:
        self.session = requests.Session()
        self.endpoint = get_sparql_endpoint()
        self.timeout = (TIMEOUT_CONNECT, TIMEOUT_READ)

    def teardown(self) -> None:
        self.session.close()

    def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        headers = CONSTRUCT_HEADERS if is_construct else QUERY_HEADERS
        start = time.perf_counter()
        response = self.session.post(
            self.endpoint,
            data={"query": sparql},
            headers=headers,
            timeout=self.timeout,
        )
        elapsed = time.perf_counter() - start
        return response.content, elapsed

    def update(self, sparql: str) -> float:
        start = time.perf_counter()
        self.session.post(
            self.endpoint,
            data=sparql,
            headers=UPDATE_HEADERS,
            timeout=self.timeout,
        )
        return time.perf_counter() - start


class Urllib3Package(SPARQLPackage):
    name = "urllib3"

    def setup(self) -> None:
        self.http = urllib3.PoolManager()
        self.endpoint = get_sparql_endpoint()
        self.timeout = urllib3.Timeout(connect=TIMEOUT_CONNECT, read=TIMEOUT_READ)

    def teardown(self) -> None:
        self.http.clear()

    def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        headers = CONSTRUCT_HEADERS if is_construct else QUERY_HEADERS
        start = time.perf_counter()
        response = self.http.request(
            "POST",
            self.endpoint,
            fields={"query": sparql},
            headers=headers,
            timeout=self.timeout,
        )
        elapsed = time.perf_counter() - start
        return response.data, elapsed

    def update(self, sparql: str) -> float:
        start = time.perf_counter()
        self.http.request(
            "POST",
            self.endpoint,
            body=sparql.encode(),
            headers=UPDATE_HEADERS,
            timeout=self.timeout,
        )
        return time.perf_counter() - start


class PycurlPackage(SPARQLPackage):
    name = "pycurl"

    def setup(self) -> None:
        self.curl = pycurl.Curl()
        self.endpoint = get_sparql_endpoint()
        self.curl.setopt(pycurl.TIMEOUT, int(TIMEOUT_TOTAL))
        self.curl.setopt(pycurl.CONNECTTIMEOUT, int(TIMEOUT_CONNECT))

    def teardown(self) -> None:
        self.curl.close()

    def query(self, sparql: str, is_construct: bool = False) -> tuple[bytes, float]:
        buffer = BytesIO()
        accept = "text/turtle" if is_construct else "application/sparql-results+json"

        self.curl.setopt(pycurl.URL, self.endpoint)
        self.curl.setopt(pycurl.POST, 1)
        self.curl.setopt(pycurl.POSTFIELDS, f"query={sparql}")
        self.curl.setopt(pycurl.HTTPHEADER, [f"Accept: {accept}"])
        self.curl.setopt(pycurl.WRITEDATA, buffer)

        start = time.perf_counter()
        self.curl.perform()
        elapsed = time.perf_counter() - start

        return buffer.getvalue(), elapsed

    def update(self, sparql: str) -> float:
        buffer = BytesIO()

        self.curl.setopt(pycurl.URL, self.endpoint)
        self.curl.setopt(pycurl.POST, 1)
        self.curl.setopt(pycurl.POSTFIELDS, sparql)
        self.curl.setopt(pycurl.HTTPHEADER, ["Content-Type: application/sparql-update"])
        self.curl.setopt(pycurl.WRITEDATA, buffer)

        start = time.perf_counter()
        self.curl.perform()
        return time.perf_counter() - start


SYNC_PACKAGES = [HttpxSyncPackage, RequestsPackage, Urllib3Package, PycurlPackage]
ASYNC_PACKAGES = [HttpxAsyncPackage, AiohttpPackage]


def run_sync_package(package_class: type[SPARQLPackage], sparql: str, is_construct: bool = False, is_update: bool = False, iterations: int = 50) -> list[tuple[bytes | None, float]]:
    """Run a sync package for N iterations."""
    package = package_class()
    package.setup()
    results = []

    for _ in range(iterations):
        if is_update:
            elapsed = package.update(sparql)
            results.append((None, elapsed))
        else:
            content, elapsed = package.query(sparql, is_construct)
            results.append((content, elapsed))

    package.teardown()
    return results


async def run_async_package(package_class: type[AsyncSPARQLPackage], sparql: str, is_construct: bool = False, is_update: bool = False, iterations: int = 50) -> list[tuple[bytes | None, float]]:
    """Run an async package for N iterations."""
    package = package_class()
    await package.setup()
    results = []

    for _ in range(iterations):
        if is_update:
            elapsed = await package.update(sparql)
            results.append((None, elapsed))
        else:
            content, elapsed = await package.query(sparql, is_construct)
            results.append((content, elapsed))

    await package.teardown()
    return results
