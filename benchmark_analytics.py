import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

RESULTS_FILE = "benchmark_results.csv"


def load_results() -> pd.DataFrame:
    """Load benchmark results from CSV."""
    return pd.read_csv(RESULTS_FILE)


def plot_requests_per_second(df: pd.DataFrame) -> None:
    """Plot requests per second by library and operation."""
    plt.figure(figsize=(14, 8))

    pivot = df.pivot_table(
        values="requests_per_sec",
        index="query_name",
        columns="library",
        aggfunc="mean",
    )

    pivot.plot(kind="bar", ax=plt.gca())
    plt.title("Requests per second by query type")
    plt.xlabel("Query type")
    plt.ylabel("Requests/sec")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Library", bbox_to_anchor=(1.02, 1), loc="upper left")
    plt.tight_layout()
    plt.savefig("rps_by_query.png", dpi=150)
    plt.close()


def plot_avg_request_time(df: pd.DataFrame) -> None:
    """Plot average request time by library."""
    plt.figure(figsize=(10, 6))

    avg_times = df.groupby("library")["avg_request_time"].mean().sort_values()

    sns.barplot(x=avg_times.index, y=avg_times.values)
    plt.title("Average request time by library")
    plt.xlabel("Library")
    plt.ylabel("Time (seconds)")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    plt.savefig("avg_time_by_library.png", dpi=150)
    plt.close()


def plot_by_operation_type(df: pd.DataFrame) -> None:
    """Plot performance grouped by operation type (read vs write)."""
    plt.figure(figsize=(12, 6))

    df["op_type"] = df["operation"].apply(
        lambda x: "write" if x in ("INSERT", "DELETE", "UPDATE") else "read"
    )

    pivot = df.pivot_table(
        values="requests_per_sec",
        index="library",
        columns="op_type",
        aggfunc="mean",
    )

    pivot.plot(kind="bar", ax=plt.gca())
    plt.title("Requests per second: read vs write operations")
    plt.xlabel("Library")
    plt.ylabel("Requests/sec")
    plt.xticks(rotation=45, ha="right")
    plt.legend(title="Operation type")
    plt.tight_layout()
    plt.savefig("rps_read_vs_write.png", dpi=150)
    plt.close()


def plot_heatmap(df: pd.DataFrame) -> None:
    """Plot heatmap of requests per second."""
    plt.figure(figsize=(12, 8))

    pivot = df.pivot_table(
        values="requests_per_sec",
        index="query_name",
        columns="library",
        aggfunc="mean",
    )

    sns.heatmap(pivot, annot=True, fmt=".1f", cmap="YlGnBu")
    plt.title("Requests per second heatmap")
    plt.tight_layout()
    plt.savefig("rps_heatmap.png", dpi=150)
    plt.close()


def print_summary(df: pd.DataFrame) -> None:
    """Print summary statistics."""
    print("\nSummary statistics")
    print("=" * 50)

    print("\nAverage requests/sec by library:")
    print(df.groupby("library")["requests_per_sec"].mean().sort_values(ascending=False))

    print("\nAverage request time (ms) by library:")
    avg_ms = df.groupby("library")["avg_request_time"].mean() * 1000
    print(avg_ms.sort_values())

    print("\nBest library by query type:")
    for query_name in df["query_name"].unique():
        query_df = df[df["query_name"] == query_name]
        best = query_df.loc[query_df["requests_per_sec"].idxmax()]
        print(f"  {query_name}: {best['library']} ({best['requests_per_sec']:.1f} req/s)")


def main() -> None:
    print("Generating analytics...")

    df = load_results()

    plot_requests_per_second(df)
    plot_avg_request_time(df)
    plot_by_operation_type(df)
    plot_heatmap(df)

    print_summary(df)

    print("\nCharts saved:")
    print("  - rps_by_query.png")
    print("  - avg_time_by_library.png")
    print("  - rps_read_vs_write.png")
    print("  - rps_heatmap.png")


if __name__ == "__main__":
    main()
