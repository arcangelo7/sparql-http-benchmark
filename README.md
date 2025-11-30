# SPARQL HTTP Benchmark

Benchmark for comparing Python HTTP libraries on SPARQL 1.1 operations against a Virtuoso endpoint.

## Libraries tested

- **httpx** (sync and async)
- **aiohttp** (async)
- **requests** (sync)
- **urllib3** (sync)
- **pycurl** (sync)

## SPARQL operations

### Read operations
- SELECT (simple, with FILTER, with OPTIONAL)
- ASK
- CONSTRUCT

### Write operations
- INSERT DATA
- DELETE DATA
- DELETE/INSERT (UPDATE)

## Requirements

- Python 3.12+
- Docker (for Virtuoso)
- [virtuoso-utilities](https://github.com/slotruglio/virtuoso-utilities)

## Installation

```bash
uv sync
```

## Usage

Run the complete benchmark (launches Virtuoso automatically):

```bash
uv run python benchmark.py
```

Run benchmark with existing Virtuoso instance:

```bash
uv run python benchmark.py --skip-setup
```

Generate analytics charts:

```bash
uv run python benchmark_analytics.py
```

## Configuration

Edit constants in `benchmark.py`:

- `NUM_RUNS`: Number of benchmark runs (default: 11, first is warmup)
- `ITERATIONS_PER_QUERY`: Requests per query type (default: 50)

Edit constants in `setup_virtuoso.py`:

- `HTTP_PORT`: Virtuoso SPARQL endpoint port (default: 8890)
- `MEMORY`: Container memory limit (default: 4g)

## Fairness

All libraries are configured with equivalent settings:

| Library | Connection pooling | Timeout |
|---------|-------------------|---------|
| httpx | `Client()` / `AsyncClient()` | 30s |
| aiohttp | `ClientSession()` | 30s |
| requests | `Session()` | 5s connect, 25s read |
| urllib3 | `PoolManager()` | 5s connect, 25s read |
| pycurl | Reused `Curl()` object | 30s |

## Output

- `benchmark_results.csv`: Raw benchmark data
- `rps_by_query.png`: Requests/sec by query type
- `avg_time_by_library.png`: Average request time by library
- `rps_read_vs_write.png`: Read vs write performance
- `rps_heatmap.png`: Performance heatmap
