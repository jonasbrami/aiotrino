# Aiotrino Encoding Performance Benchmark

This script benchmarks the performance of different aiotrino encodings across various data sizes and types.

## Prerequisites

```bash
pip install matplotlib pandas aiotrino pyarrow
```

## Usage

1. **Start your Trino server** (localhost:8080 by default)

2. **Run the benchmark from the project root:**
```bash
cd tests/benchmark
python benchmark_encoding_performance.py
```

Or from the project root:
```bash
python tests/benchmark/benchmark_encoding_performance.py
```

3. **Configuration** (edit variables at top of script):
```python
TRINO_HOST = "localhost"
TRINO_PORT = 8080
TRINO_USER = "test"
```

## Output Files

- `aiotrino_encoding_performance.png` - 5-panel performance graphs (now includes payload size per row)
- `aiotrino_query_type_comparison.png` - Query type comparison chart
- `aiotrino_payload_size_comparison.png` - Dedicated payload size and compression analysis
- `aiotrino_benchmark_results.csv` - Raw data for analysis

## Example Output

```
🚀 Starting aiotrino encoding benchmark...
Testing encodings: json, json+zstd, json+lz4, arrow, arrow+zstd
Row counts: 100, 500, 1000, 2500, 5000, 10000, 25000, 50000
Query types: integers, strings, mixed, wide

[  1/160] Testing json         | integers |    100 rows ... ✅ 0.023s (4,348 rows/s)
[  2/160] Testing json         | integers |    500 rows ... ✅ 0.067s (7,463 rows/s)
...

📈 BENCHMARK SUMMARY
================================================================================

🏆 BEST PERFORMERS:
Throughput:    arrow+zstd   - 234,567 rows/s (50,000 rows, mixed)
Fastest:       arrow        - 0.012s (100 rows, integers)  
Compression:   json+zstd    - 0.34 MB (10,000 rows, strings)

📊 AVERAGE PERFORMANCE (all tests):
           fetch_time  rows_per_second  data_size_mb  mb_per_second
encoding                                                           
arrow           0.145        98234.567         1.234         12.456
arrow+zstd      0.089       156789.012         0.789         15.678
json            0.234        45678.901         2.345          8.901
json+lz4        0.167        67890.123         1.456         10.123
json+zstd       0.178        61234.567         1.123          9.234
```

## Generated Graphs

### 1. Performance Overview (graphs/aiotrino_encoding_performance.png)
A comprehensive 6-panel visualization showing:
- **Fetch Time vs Rows**: How serialization time scales with data size (log-log scale)
- **Throughput vs Rows**: Processing speed in rows/second across different data sizes
- **Data Size vs Rows**: Actual serialized payload sizes showing compression effectiveness
- **Transfer Rate vs Rows**: Data transfer performance in MB/second
- **Bytes per Row**: Payload efficiency normalized per row
- **Relative Throughput**: Performance comparison at maximum row count (baseline: JSON = 1.0x)

### 2. Payload Size Comparison (graphs/aiotrino_payload_size_comparison.png)
A clean scatter plot visualization:
- **X-axis**: Number of rows (logarithmic scale with comma formatting)
- **Y-axis**: Payload size in MB (logarithmic scale)
- **Color coding**: Each encoding has a distinct color for easy comparison
- **Purpose**: Clearly shows compression efficiency and how payload size scales with data volume

### 3. Query Type Performance (graphs/aiotrino_query_type_comparison.png)
Bar chart comparing throughput across different query types at the largest row count tested.

## Visual Examples

The payload size comparison graph makes it easy to see:
- JSON (blue) has the largest payload sizes
- Arrow+Zstd (purple) provides the best compression
- Compression benefits increase with larger datasets
- All encodings scale linearly on the log-log plot

## Customization

Edit the script to:
- Change row counts: `ROW_COUNTS = [100, 1000, 10000, ...]`
- Add encodings: `ENCODINGS = ["json", "arrow", ...]`
- Modify queries: `QUERIES = {"custom": "SELECT ... LIMIT {rows}"}` 