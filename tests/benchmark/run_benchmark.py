#!/usr/bin/env python3
"""Simple benchmark using environment variables for configuration."""

import asyncio
import time
import csv
import os
import sys
import aiotrino

def get_config():
    """Get configuration from environment variables."""
    config = {
        'host': os.environ.get('TRINO_HOST', 'localhost'),
        'port': int(os.environ.get('TRINO_PORT', '8092')),
        'user': os.environ.get('TRINO_USER', 'test'),
        'query_template': os.environ.get('QUERY_TEMPLATE', 'SELECT * FROM tpcds.sf100000.store_sales LIMIT {}'),
        'encoding': os.environ.get('ENCODING', 'arrow+zstd'),
        'min_power': int(os.environ.get('MIN_POWER', '10')),
        'max_power': int(os.environ.get('MAX_POWER', '24')),
        'step': int(os.environ.get('POWER_STEP', '2')),
    }
    
    return config

async def run_benchmark():
    """Run the benchmark with current configuration."""
    config = get_config()
    
    conn = aiotrino.dbapi.Connection(
        host=config['host'],
        port=config['port'],
        user=config['user'],
        source="benchmark",
        max_attempts=1,
        encoding=config['encoding']
    )
    
    results = []
    cursor_style = "segment" if config['encoding'].startswith("arrow") else "row"
    
    print(f"🚀 Running benchmark with {config['encoding']} encoding...")
    print()
    
    for power in range(config['min_power'], config['max_power'] + 1, config['step']):
        rows = 2 ** power
        
        # If min_power == max_power, use query template as-is without templating
        if config['min_power'] == config['max_power']:
            query = config['query_template']
        else:
            query = config['query_template'].format(rows)
        
        async with await conn.cursor(cursor_style=cursor_style) as cur:
            start_time = time.perf_counter()
            await cur.execute(query)
            
            if config['encoding'].startswith("arrow"):
                data = await cur.fetchall_arrow()
                actual_rows = len(data)
            else:
                data = await cur.fetchall()
                actual_rows = len(data)
            
            elapsed_time = time.perf_counter() - start_time
            throughput = actual_rows / elapsed_time
            
            print(f"2^{power:2d}: {actual_rows:>8,} rows in {elapsed_time*1000:>6.0f} ms ({throughput:>8.0f} rows/s)")
            
            results.append({
                'power': power,
                'rows': actual_rows,
                'time_ms': elapsed_time * 1000,
                'rows_per_sec': throughput
            })
    
    await conn.close()
    
    # Write CSV file
    filename = f"benchmark_results_{config['encoding'].replace('+', '_')}.csv"
    with open(filename, 'w', newline='') as csvfile:
        fieldnames = ['power', 'rows', 'time_ms', 'rows_per_sec']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for result in results:
            writer.writerow(result)
    
    print()
    print(f"📊 Results exported to {filename}")

def main():
    """Main function."""
    if len(sys.argv) > 1 and sys.argv[1] in ['-h', '--help']:
        print("""
Simple Trino Benchmark Tool

Environment Variables:
  TRINO_HOST        Trino server host (default: localhost)
  TRINO_PORT        Trino server port (default: 8092)
  TRINO_USER        Trino user (default: test)
  QUERY_TEMPLATE    Query template with {} for row count (default: SELECT * FROM tpcds.sf100000.store_sales LIMIT {})
  ENCODING          Trino encoding (default: arrow+zstd)
  MIN_POWER         Minimum power of 2 (default: 10)
  MAX_POWER         Maximum power of 2 (default: 24)
  POWER_STEP        Step between powers (default: 2)

Examples:
  # Basic run
  python run_benchmark.py
  
  # Custom encoding
  ENCODING=json+zstd python run_benchmark.py
  
  # Custom query and smaller scale
  QUERY_TEMPLATE="SELECT ss_item_sk, ss_quantity FROM tpcds.sf100000.store_sales LIMIT {}" MAX_POWER=16 python run_benchmark.py
  
  # Custom Trino server
  TRINO_HOST=my-trino-server TRINO_PORT=8080 python run_benchmark.py
        """)
        return
    
    try:
        asyncio.run(run_benchmark())
    except KeyboardInterrupt:
        print("\n⚠️  Benchmark interrupted by user")
    except Exception as e:
        print(f"❌ Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main() 