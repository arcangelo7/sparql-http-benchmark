import subprocess
import sys

CONTAINER_NAME = "sparql-benchmark"
HTTP_PORT = 8090
ISQL_PORT = 1011
MEMORY = "4g"


def setup_virtuoso() -> None:
    """Launch Virtuoso container with write permissions enabled."""
    subprocess.run(
        [
            "virtuoso-launch",
            "--name", CONTAINER_NAME,
            "--memory", MEMORY,
            "--http-port", str(HTTP_PORT),
            "--isql-port", str(ISQL_PORT),
            "--enable-write-permissions",
            "--detach",
            "--wait-ready",
        ],
        check=True,
    )


def stop_virtuoso() -> None:
    """Stop and remove Virtuoso container."""
    subprocess.run(
        ["docker", "stop", CONTAINER_NAME],
        check=False,
        capture_output=True,
    )
    subprocess.run(
        ["docker", "rm", CONTAINER_NAME],
        check=False,
        capture_output=True,
    )


def get_sparql_endpoint() -> str:
    """Return the SPARQL endpoint URL."""
    return f"http://localhost:{HTTP_PORT}/sparql"


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "stop":
        stop_virtuoso()
    else:
        setup_virtuoso()