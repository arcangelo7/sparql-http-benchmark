import argparse
import os
import sys
import time

import httpx

from virtuoso_utilities.launch_virtuoso import (
    build_docker_run_command,
    check_container_exists,
    check_docker_installed,
    enable_write_permissions,
    get_optimal_buffer_values,
    remove_container,
    run_docker_command,
    update_ini_memory_settings,
    wait_for_virtuoso_ready,
)

CONTAINER_NAME = "sparql-benchmark"
HTTP_PORT = 8090
ISQL_PORT = 1011
MEMORY = "4g"
DATA_DIR = "./virtuoso-data"
DBA_PASSWORD = "dba"


def _build_args() -> argparse.Namespace:
    """Build argparse.Namespace with configuration for virtuoso-utilities."""
    number_of_buffers, max_dirty_buffers = get_optimal_buffer_values(MEMORY)

    return argparse.Namespace(
        name=CONTAINER_NAME,
        http_port=HTTP_PORT,
        isql_port=ISQL_PORT,
        data_dir=DATA_DIR,
        extra_volumes=None,
        memory=MEMORY,
        cpu_limit=0,
        dba_password=DBA_PASSWORD,
        force_remove=True,
        network=None,
        wait_ready=True,
        detach=True,
        enable_write_permissions=True,
        estimated_db_size_gb=0,
        virtuoso_version=None,
        virtuoso_sha=None,
        max_dirty_buffers=max_dirty_buffers,
        number_of_buffers=number_of_buffers,
    )


def setup_virtuoso() -> None:
    """Launch Virtuoso container with write permissions enabled."""
    if not check_docker_installed():
        raise RuntimeError("Docker command not found. Please install Docker.")

    args = _build_args()

    host_data_dir_abs = os.path.abspath(args.data_dir)
    ini_file_path = os.path.join(host_data_dir_abs, "virtuoso.ini")

    docker_cmd, unique_paths_to_allow = build_docker_run_command(args)
    dirs_allowed_str = ",".join(unique_paths_to_allow) if unique_paths_to_allow else None

    update_ini_memory_settings(
        ini_file_path,
        host_data_dir_abs,
        args.number_of_buffers,
        args.max_dirty_buffers,
        dirs_allowed=dirs_allowed_str,
    )

    if check_container_exists(args.name):
        print(f"Container '{args.name}' already exists. Removing...")
        remove_container(args.name)

    run_docker_command(docker_cmd, check=False)

    print("Waiting for Virtuoso readiness...")
    ready = wait_for_virtuoso_ready(
        args.name,
        "localhost",
        args.isql_port,
        args.dba_password,
        timeout=120,
    )

    if not ready:
        raise RuntimeError("Virtuoso readiness check timed out or failed.")

    if not enable_write_permissions(args):
        print(
            "Warning: One or more commands to enable write permissions failed.",
            file=sys.stderr,
        )

    _wait_for_http_ready()
    print(f"Virtuoso launched successfully on http://localhost:{HTTP_PORT}/sparql")


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
