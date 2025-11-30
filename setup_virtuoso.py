import sys
import time

import httpx

from virtuoso_utilities.launch_virtuoso import (
    check_container_exists,
    launch_virtuoso,
    remove_container,
)

CONTAINER_NAME = "sparql-benchmark"
HTTP_PORT = 8090
ISQL_PORT = 1011
MEMORY = "4g"


def setup_virtuoso() -> None:
    """Launch Virtuoso container with write permissions enabled."""
    launch_virtuoso(
        name=CONTAINER_NAME,
        http_port=HTTP_PORT,
        isql_port=ISQL_PORT,
        memory=MEMORY,
        enable_write_permissions=True,
        force_remove=True,
    )
    _wait_for_http_ready()


def _wait_for_http_ready(timeout: int = 30) -> None:
    """Wait for SPARQL HTTP endpoint to be ready."""
    endpoint = get_sparql_endpoint()
    print("Waiting for SPARQL HTTP endpoint...")
    start = time.time()

    while time.time() - start < timeout:
        try:
            response = httpx.get(endpoint, timeout=5.0)
            if response.status_code == 200:
                print("SPARQL HTTP endpoint is ready.")
                return
        except httpx.RequestError:
            pass
        time.sleep(1)

    raise RuntimeError("SPARQL HTTP endpoint readiness check timed out.")


def stop_virtuoso() -> None:
    """Stop and remove Virtuoso container."""
    if check_container_exists(CONTAINER_NAME):
        remove_container(CONTAINER_NAME)
        print(f"Container '{CONTAINER_NAME}' removed.")
    else:
        print(f"Container '{CONTAINER_NAME}' does not exist.")


def get_sparql_endpoint() -> str:
    """Return the SPARQL endpoint URL."""
    return f"http://localhost:{HTTP_PORT}/sparql"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop_virtuoso()
    else:
        setup_virtuoso()
