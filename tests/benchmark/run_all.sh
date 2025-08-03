#!/bin/bash

# Simple script to run all Trino benchmarks
set -e

echo "🚀 Running all Trino benchmarks..."
echo

# Get configuration from environment or use defaults
export TRINO_HOST=${TRINO_HOST:-localhost}
export TRINO_PORT=${TRINO_PORT:-8092}
export TRINO_USER=${TRINO_USER:-test}
# Default Python query template (with {})
export QUERY_TEMPLATE=${QUERY_TEMPLATE:-"SELECT * FROM tpcds.sf100000.store_sales LIMIT {}"}
# Java query template (with %d) - convert {} to %d for Java
export JAVA_QUERY_TEMPLATE=${JAVA_QUERY_TEMPLATE:-$(echo "$QUERY_TEMPLATE" | sed 's/{}/\%d/g')}
export MIN_POWER=${MIN_POWER:-10}
export MAX_POWER=${MAX_POWER:-24}
export POWER_STEP=${POWER_STEP:-2}

# Activate Python virtual environment if it exists
if [ -f "../../.venv/bin/activate" ]; then
    echo "📦 Activating Python virtual environment..."
    source ../../.venv/bin/activate
fi

# Run Python benchmarks
echo "=== Python JSON+ZSTD Benchmark ==="
ENCODING=json+zstd MAX_POWER=22 python run_benchmark.py

echo
echo "=== Python Arrow+ZSTD Benchmark ==="
ENCODING=arrow+zstd python run_benchmark.py

echo
echo "=== Java JDBC Benchmark ==="
cd java-jdbc
# Export Java-specific query template
export QUERY_TEMPLATE="$JAVA_QUERY_TEMPLATE"
mvn -q compile exec:java
cd ..

echo
echo "🎉 All benchmarks completed!"
echo
echo "📊 Generated files:"
ls -la benchmark_results_*.csv

echo
echo "💡 To compare results, run:"
echo "  python compare_all_results.py" 