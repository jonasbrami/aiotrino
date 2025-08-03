# Trino Client Benchmarks

Simple benchmark suite to compare Trino client performance across different encodings.

## Quick Start

```bash
# Run all benchmarks with defaults
./run_all.sh

# Compare results
python compare_all_results.py
```

## Benchmarks Included

- **Python JSON+ZSTD**: aiotrino client with JSON+ZSTD encoding
- **Python Arrow+ZSTD**: aiotrino client with Arrow+ZSTD encoding  
- **Java JDBC**: Standard Trino JDBC driver

## Configuration

All benchmarks use environment variables for configuration:

| Variable | Default | Description |
|----------|---------|-------------|
| `TRINO_HOST` | localhost | Trino server hostname |
| `TRINO_PORT` | 8092 | Trino server port |
| `TRINO_USER` | test | Trino username |
| `QUERY_TEMPLATE` | `SELECT * FROM tpcds.sf100000.store_sales LIMIT {}` | SQL query template |
| `MIN_POWER` | 10 | Minimum power of 2 (2^10 = 1,024 rows) |
| `MAX_POWER` | 24 | Maximum power of 2 (2^24 = 16M rows) |
| `POWER_STEP` | 2 | Step between powers |

## Examples

```bash
# Quick test with smaller dataset
MAX_POWER=16 ./run_all.sh

# Custom query (just 3 columns)
QUERY_TEMPLATE="SELECT ss_item_sk, ss_quantity, ss_sales_price FROM tpcds.sf100000.store_sales LIMIT {}" ./run_all.sh

# Different Trino server
TRINO_HOST=my-server TRINO_PORT=8080 ./run_all.sh

# Individual benchmark
ENCODING=arrow+zstd MAX_POWER=20 python run_benchmark.py
```

## Individual Tools

### Python Benchmark
```bash
# Show help
python run_benchmark.py --help

# Run with specific encoding
ENCODING=json+zstd python run_benchmark.py
```

### Java Benchmark
```bash
cd java-jdbc
mvn compile exec:java
```

### Compare Results
```bash
python compare_all_results.py
```

## Output Files

- `benchmark_results_*.csv`: Raw benchmark data
- `benchmark_comparison.png`: Performance comparison graph

## Requirements

- Python 3.8+ with aiotrino package
- Java 11+ with Maven
- Running Trino server with TPC-DS data
- matplotlib and pandas for visualization
