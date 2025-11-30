import asyncio
import csv
import random
import sys
import time

import httpx

from factory import (
    ASYNC_PACKAGES,
    SYNC_PACKAGES,
    run_async_package,
    run_sync_package,
)
from model import BenchmarkResult
from queries import QUERIES, UPDATES
from setup_virtuoso import get_sparql_endpoint, setup_virtuoso, stop_virtuoso
from test_data import generate_insert_sparql

NUM_RUNS = 11
ITERATIONS_PER_QUERY = 50
RESULTS_FILE = "benchmark_results.csv"


def load_test_data() -> None:
    """Load test data into Virtuoso via SPARQL INSERT."""
    endpoint = get_sparql_endpoint()
    insert_query = generate_insert_sparql(1000)

    with httpx.Client(timeout=60.0) as client:
        response = client.post(
            endpoint,
            content=insert_query,
            headers={"Content-Type": "application/sparql-update"},
        )
        response.raise_for_status()

    print("Test data loaded successfully")


def calculate_result(
    library: str,
    operation: str,
    query_name: str,
    results: list[tuple[bytes | None, float]],
) -> BenchmarkResult:
    """Calculate benchmark metrics from raw results."""
    times = [r[1] for r in results]
    total_time = sum(times)
    successful = len([r for r in results if r is not None or operation in ("INSERT", "DELETE", "UPDATE")])
    response_size = len(results[0][0]) if results[0][0] else 0

    return BenchmarkResult(
        library=library,
        operation=operation,
        query_name=query_name,
        requests_per_sec=len(results) / total_time,
        total_time=total_time,
        avg_request_time=total_time / len(results),
        response_size_bytes=response_size,
        success_rate=successful / len(results),
    )


def run_benchmark() -> list[BenchmarkResult]:
    """Run the complete benchmark suite."""
    all_results = []

    for run in range(NUM_RUNS):
        print(f"\nBenchmark run: {run + 1}/{NUM_RUNS}")

        if run == 0:
            print("  (warmup run - results discarded)")

        sync_packages = list(SYNC_PACKAGES)
        async_packages = list(ASYNC_PACKAGES)
        random.shuffle(sync_packages)
        random.shuffle(async_packages)

        for package_class in sync_packages:
            print(f"  Running {package_class.name}...")

            for query_name, query_info in QUERIES.items():
                is_construct = query_info["operation"] == "CONSTRUCT"
                results = run_sync_package(
                    package_class,
                    query_info["sparql"],
                    is_construct=is_construct,
                    iterations=ITERATIONS_PER_QUERY,
                )
                if run > 0:
                    all_results.append(calculate_result(
                        package_class.name,
                        query_info["operation"],
                        query_name,
                        results,
                    ))

            for update_name, update_info in UPDATES.items():
                results = run_sync_package(
                    package_class,
                    update_info["sparql"],
                    is_update=True,
                    iterations=ITERATIONS_PER_QUERY,
                )
                if run > 0:
                    all_results.append(calculate_result(
                        package_class.name,
                        update_info["operation"],
                        update_name,
                        results,
                    ))

        for package_class in async_packages:
            print(f"  Running {package_class.name}...")

            for query_name, query_info in QUERIES.items():
                is_construct = query_info["operation"] == "CONSTRUCT"
                results = asyncio.run(run_async_package(
                    package_class,
                    query_info["sparql"],
                    is_construct=is_construct,
                    iterations=ITERATIONS_PER_QUERY,
                ))
                if run > 0:
                    all_results.append(calculate_result(
                        package_class.name,
                        query_info["operation"],
                        query_name,
                        results,
                    ))

            for update_name, update_info in UPDATES.items():
                results = asyncio.run(run_async_package(
                    package_class,
                    update_info["sparql"],
                    is_update=True,
                    iterations=ITERATIONS_PER_QUERY,
                ))
                if run > 0:
                    all_results.append(calculate_result(
                        package_class.name,
                        update_info["operation"],
                        update_name,
                        results,
                    ))

    return all_results


def save_results(results: list[BenchmarkResult]) -> None:
    """Save benchmark results to CSV."""
    with open(RESULTS_FILE, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow([
            "library",
            "operation",
            "query_name",
            "requests_per_sec",
            "total_time",
            "avg_request_time",
            "response_size_bytes",
            "success_rate",
        ])
        for result in results:
            writer.writerow([
                result.library,
                result.operation,
                result.query_name,
                result.requests_per_sec,
                result.total_time,
                result.avg_request_time,
                result.response_size_bytes,
                result.success_rate,
            ])

    print(f"\nResults saved to {RESULTS_FILE}")


def main() -> None:
    print("SPARQL HTTP Benchmark")
    print("=" * 50)

    print("\nSetting up Virtuoso...")
    setup_virtuoso()

    print("\nLoading test data...")
    load_test_data()

    print("\nStarting benchmark...")
    results = run_benchmark()

    print("\nSaving results...")
    save_results(results)

    print("\nStopping Virtuoso...")
    stop_virtuoso()

    print("\nBenchmark complete!")


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--skip-setup":
        print("Skipping Virtuoso setup...")
        results = run_benchmark()
        save_results(results)
    else:
        main()
