#!/usr/bin/env python3
"""
Standalone benchmark script for measuring aiotrino encoding performance.

Usage:
    python benchmark_encoding_performance.py

Requirements:
    pip install matplotlib pandas aiotrino pyarrow
"""

import asyncio
import time
import sys
import os
from typing import Dict, List, Tuple
from pathlib import Path
import matplotlib.pyplot as plt
import pandas as pd
import aiotrino
from aiotrino.dbapi import Connection
import pyarrow as pa
import pyarrow.ipc as ipc
import io

from unittest.mock import patch
from unittest.mock import patch, AsyncMock
from aiotrino.client import ZStdQueryDataDecoder
from aiotrino.dbapi import SegmentCursor
from aiotrino.client import SpooledSegment, InlineSegment
# Configuration
TRINO_HOST = "localhost"
TRINO_PORT = int(os.environ.get("TRINO_DEFAULT_PORT", 8092))
TRINO_USER = "test"

# Test parameters
ENCODINGS = ["json", "json+zstd", "json+lz4", "arrow", "arrow+zstd"]
# ENCODINGS = ["arrow+zstd"]
ROW_COUNTS = [2**i for i in range(10, 24, 2)]
# Data type queries to test
# For larger row counts, we use cross joins to generate more data
QUERIES = {
    # "integers": """
    #     SELECT custkey 
    #     FROM tpch.sf1.customer 
    #     {cross_join}
    #     LIMIT {rows}
    # """,
    # "strings": """
    #     SELECT name 
    #     FROM tpch.sf1.customer
    #     {cross_join}
    #     LIMIT {rows}
    # """,
    # "mixed": """
    #     SELECT c1.custkey, c1.name, c1.acctbal, c1.mktsegment 
    #     FROM tpch.sf1.customer c1
    #     {cross_join}
    #     LIMIT {rows}
    # """,
    # "wide": """
    #     SELECT c1.* 
    #     FROM tpch.sf1.customer c1
    #     {cross_join}
    #     LIMIT {rows}
    # """,
    "tpcds_store_sales": """
        SELECT *
        FROM tpcds.sf100000.store_sales
        LIMIT {rows}
    """
}

async def benchmark_encoding(encoding: str, query_type: str, query_template: str, row_count: int) -> Dict:
    
    """Benchmark a specific encoding with given parameters."""

    conn = aiotrino.dbapi.Connection(
        host=TRINO_HOST,
        port=TRINO_PORT, 
        user=TRINO_USER,
        source="benchmark",
        max_attempts=1,
        encoding=encoding
    )
    
    query = f"select * from tpcds.sf100000.store_sales LIMIT {row_count}"
    
    async with await conn.cursor(cursor_style="segment" if encoding.startswith("arrow") else "row") as cur:
        # Execute query and wait for it to complete
        fetch_start = time.perf_counter()
        await cur.execute(query)
        with patch.object(SpooledSegment, "get_data", side_effect=SpooledSegment.get_data, autospec=True) as get_data_spooled_mock:
            with patch.object(InlineSegment, "get_data", side_effect=InlineSegment.get_data, autospec=True) as get_data_inline_mock:
                if encoding.startswith("arrow"):
                    data = await cur.fetchall_arrow()
                else:
                    data = await cur.fetchall()
                if get_data_spooled_mock.call_count > 0:
                    data_size_spooled = sum(call.args[0].metadata['segmentSize'] for call in get_data_spooled_mock.call_args_list if call)
                else:
                    data_size_spooled = 0
                if get_data_inline_mock.call_count > 0:
                    data_size_inline = sum(call.args[0].metadata['segmentSize'] for call in get_data_inline_mock.call_args_list if call)
                else:
                    data_size_inline = 0
                data_size = data_size_spooled + data_size_inline
        
            # Get query stats before fetching
        
            query_stats = cur.stats.copy() if cur.stats else {}
            query_cpu_time = query_stats.get('cpuTimeMillis', 0) / 1000.0  # Convert to seconds
            query_wall_time = query_stats.get('wallTimeMillis', 0) / 1000.0  # Convert to seconds
                
        fetch_time = time.perf_counter() - fetch_start
        actual_rows = len(data)
        
        await conn.close()
        
        return {
            "encoding": encoding,
            "query_type": query_type,
            "target_rows": row_count,
            "actual_rows": actual_rows,
            "query_cpu_time": query_cpu_time,
            "query_wall_time": query_wall_time,
            "fetch_decode_time": fetch_time,
            "total_time": query_wall_time + fetch_time,
            "data_size_mb": data_size / (1024 * 1024),
            "rows_per_second": actual_rows / fetch_time if fetch_time > 0 else 0,
            "mb_per_second": (data_size / (1024 * 1024)) / fetch_time if fetch_time > 0 else 0,
            "query_id": cur.query_id if hasattr(cur, 'query_id') else None,
        }


async def run_benchmarks() -> pd.DataFrame:
    """Run all benchmark combinations."""
    print("🚀 Starting aiotrino encoding benchmark...")
    print(f"Testing encodings: {', '.join(ENCODINGS)}")
    print(f"Row counts: {', '.join(map(str, ROW_COUNTS))}")
    print(f"Query types: {', '.join(QUERIES.keys())}")
    print("-" * 60)
    
    results = []
    total_tests = len(ENCODINGS) * len(QUERIES) * len(ROW_COUNTS)
    current_test = 0
    
    for encoding in ENCODINGS:
        for query_type, query_template in QUERIES.items():
            for row_count in ROW_COUNTS:
                current_test += 1
                print(f"[{current_test:3d}/{total_tests}] Testing {encoding:12} | {query_type:8} | {row_count:6,} rows", end=" ... ")
                
                result = await benchmark_encoding(encoding, query_type, query_template, row_count)
                if result:
                    results.append(result)
                    print(f"✅ Fetch: {result['fetch_decode_time']:.3f}s, Size: {result['data_size_mb']:.1f}MB ({result['rows_per_second']:,.0f} rows/s), {result['actual_rows']:,.0f} rows")
                else:
                    print("❌ FAILED")
              
    
    return pd.DataFrame(results)


def create_performance_graphs(df: pd.DataFrame):
    """Create performance visualization graphs."""
    plt.style.use('seaborn-v0_8' if 'seaborn-v0_8' in plt.style.available else 'default')
    
    # Get the script directory and create graphs directory
    script_dir = Path(__file__).parent
    graphs_dir = script_dir / "graphs"
    graphs_dir.mkdir(exist_ok=True)
    
    # Create a 2x3 subplot layout to include the new payload size graph
    fig = plt.figure(figsize=(20, 16))
    
    # Original 4 plots + 2 new plots
    ax1 = plt.subplot(2, 3, 1)
    ax2 = plt.subplot(2, 3, 2)
    ax3 = plt.subplot(2, 3, 3)
    ax4 = plt.subplot(2, 3, 4)
    ax5 = plt.subplot(2, 3, 5)  # Payload size comparison plot
    ax6 = plt.subplot(2, 3, 6)  # Relative throughput at max rows
    
    # Plot 1: Fetch/Decode Time vs Rows (excluding query time)
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding].groupby('target_rows')['fetch_decode_time'].mean()
        ax1.plot(enc_data.index, enc_data.values, marker='o', label=encoding, linewidth=2)
    
    ax1.set_xlabel('Number of Rows')
    ax1.set_ylabel('Fetch/Decode Time (seconds)')
    ax1.set_title('Fetch/Decode Time vs Row Count')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.set_xscale('log')
    ax1.set_yscale('log')
    
    # Plot 2: Throughput vs Rows
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding].groupby('target_rows')['rows_per_second'].mean()
        ax2.plot(enc_data.index, enc_data.values, marker='s', label=encoding, linewidth=2)
    
    ax2.set_xlabel('Number of Rows')
    ax2.set_ylabel('Throughput (rows/second)')
    ax2.set_title('Throughput vs Row Count')
    ax2.legend()
    ax2.grid(True, alpha=0.3)
    ax2.set_xscale('log')
    
    # Plot 3: Actual Data Size vs Rows
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding].groupby('target_rows')['data_size_mb'].mean()
        ax3.plot(enc_data.index, enc_data.values, marker='^', label=encoding, linewidth=2)
    
    ax3.set_xlabel('Number of Rows')
    ax3.set_ylabel('Actual Data Size (MB)')
    ax3.set_title('Actual Serialized Data Size vs Row Count')
    ax3.legend()
    ax3.grid(True, alpha=0.3)
    ax3.set_xscale('log')
    ax3.set_yscale('log')
    
    # Plot 4: Data Transfer Rate vs Rows
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding].groupby('target_rows')['mb_per_second'].mean()
        ax4.plot(enc_data.index, enc_data.values, marker='d', label=encoding, linewidth=2)
    
    ax4.set_xlabel('Number of Rows')
    ax4.set_ylabel('Transfer Rate (MB/second)')
    ax4.set_title('Data Transfer Rate vs Row Count')
    ax4.legend()
    ax4.grid(True, alpha=0.3)
    ax4.set_xscale('log')
    
    # Plot 5: Payload Size Comparison (New!)
    # Show payload size per 1000 rows to normalize across different row counts
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding].copy()
        enc_data['bytes_per_row'] = (enc_data['data_size_mb'] * 1024 * 1024) / enc_data['actual_rows']
        bytes_per_row = enc_data.groupby('target_rows')['bytes_per_row'].mean()
        ax5.plot(bytes_per_row.index, bytes_per_row.values, marker='p', label=encoding, linewidth=2, markersize=8)
    
    ax5.set_xlabel('Number of Rows')
    ax5.set_ylabel('Payload Size (bytes per row)')
    ax5.set_title('Payload Efficiency: Bytes per Row by Encoding')
    ax5.legend()
    ax5.grid(True, alpha=0.3)
    ax5.set_xscale('log')
    
    # Add horizontal line for reference (average bytes per row)
    avg_bytes_per_row = (df['data_size_mb'].sum() * 1024 * 1024) / df['actual_rows'].sum()
    ax5.axhline(y=avg_bytes_per_row, color='gray', linestyle='--', alpha=0.5, label=f'Overall avg: {avg_bytes_per_row:.1f} bytes/row')
    
    # Plot 6: Relative Throughput at Maximum Row Count
    # Get the maximum row count
    max_rows = df['target_rows'].max()
    max_data = df[df['target_rows'] == max_rows]
    
    # Calculate relative throughput (normalize to JSON as baseline = 1.0)
    json_throughput = max_data[max_data['encoding'] == 'json']['rows_per_second'].mean()
    
    encodings = []
    relative_throughputs = []
    colors_list = []
    
    # Define the order and colors for consistency
    encoding_order = ['json', 'json+zstd', 'json+lz4', 'arrow', 'arrow+zstd']
    colors = {
        'json': '#1f77b4',          # blue
        'json+zstd': '#ff7f0e',     # orange
        'json+lz4': '#2ca02c',      # green
        'arrow': '#d62728',         # red
        'arrow+zstd': '#9467bd'     # purple
    }
    
    for encoding in encoding_order:
        enc_data = max_data[max_data['encoding'] == encoding]
        if not enc_data.empty:
            throughput = enc_data['rows_per_second'].mean()
            relative = throughput / json_throughput
            encodings.append(encoding)
            relative_throughputs.append(relative)
            colors_list.append(colors.get(encoding, 'gray'))
    
    # Create bar chart
    bars = ax6.bar(encodings, relative_throughputs, color=colors_list, alpha=0.8, edgecolor='black', linewidth=1.5)
    
    # Add value labels on bars
    for i, (bar, value) in enumerate(zip(bars, relative_throughputs)):
        height = bar.get_height()
        ax6.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{value:.2f}x', ha='center', va='bottom', fontsize=10, fontweight='bold')
    
    ax6.axhline(y=1.0, color='gray', linestyle='--', alpha=0.5)
    ax6.set_ylabel('Relative Throughput (vs JSON)', fontsize=12)
    ax6.set_title(f'Relative Throughput at {max_rows:,} Rows', fontsize=12, fontweight='bold')
    ax6.set_ylim(0, max(relative_throughputs) * 1.15)  # Add some space for labels
    ax6.grid(True, alpha=0.3, axis='y')
    ax6.set_xticklabels(encodings, rotation=45, ha='right')
    
    plt.tight_layout()
    output_path = graphs_dir / 'aiotrino_encoding_performance.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"📊 Performance graphs saved to: {output_path}")
    
    # 2. Query type comparison
    fig2, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    # Compare encodings at largest row count
    max_rows = df['target_rows'].max()
    max_data = df[df['target_rows'] == max_rows]
    
    query_types = max_data['query_type'].unique()
    encodings = max_data['encoding'].unique()
    
    x = range(len(query_types))
    width = 0.15
    
    for i, encoding in enumerate(encodings):
        enc_data = max_data[max_data['encoding'] == encoding]
        throughputs = [enc_data[enc_data['query_type'] == qt]['rows_per_second'].mean() for qt in query_types]
        ax.bar([xi + i*width for xi in x], throughputs, width, label=encoding)
    
    ax.set_xlabel('Query Type')
    ax.set_ylabel('Throughput (rows/second)')
    ax.set_title(f'Throughput Comparison by Query Type ({max_rows:,} rows)')
    ax.set_xticks([xi + width*2 for xi in x])
    ax.set_xticklabels(query_types)
    ax.legend()
    ax.grid(True, alpha=0.3)
    
    plt.tight_layout()
    output_path2 = graphs_dir / 'aiotrino_query_type_comparison.png'
    plt.savefig(output_path2, dpi=300, bbox_inches='tight')
    print(f"📊 Query type comparison saved to: {output_path2}")

    # 3. Create payload size comparison chart
    create_payload_size_comparison(df, graphs_dir)


def create_payload_size_comparison(df: pd.DataFrame, graphs_dir: Path):
    """Create a simple scatter plot showing payload size vs row count for each encoding."""
    plt.figure(figsize=(12, 8))
    
    # Define a color palette for encodings
    colors = {
        'json': '#1f77b4',          # blue
        'json+zstd': '#ff7f0e',     # orange
        'json+lz4': '#2ca02c',      # green
        'arrow': '#d62728',         # red
        'arrow+zstd': '#9467bd'     # purple
    }
    
    # Create scatter plot for each encoding
    for encoding in df['encoding'].unique():
        enc_data = df[df['encoding'] == encoding]
        # Filter out zero values for better visualization
        non_zero_data = enc_data[enc_data['data_size_mb'] > 0]
        
        plt.scatter(non_zero_data['target_rows'], 
                   non_zero_data['data_size_mb'],
                   label=encoding,
                   color=colors.get(encoding, 'black'),
                   s=100,  # marker size
                   alpha=0.7,
                   edgecolors='black',
                   linewidth=1)
    
    plt.xlabel('Number of Rows', fontsize=14)
    plt.ylabel('Payload Size (MB)', fontsize=14)
    plt.title('Payload Size by Encoding', fontsize=16, fontweight='bold')
    plt.legend(loc='upper left', fontsize=12, framealpha=0.9)
    plt.grid(True, alpha=0.3)
    plt.xscale('log')
    plt.yscale('log')
    
    # Add minor gridlines for better readability
    plt.grid(True, which='minor', alpha=0.1)
    
    # Format the axes
    ax = plt.gca()
    ax.xaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{int(x):,}'))
    ax.yaxis.set_major_formatter(plt.FuncFormatter(lambda x, p: f'{x:.1f}' if x >= 0.1 else f'{x:.2f}'))
    
    plt.tight_layout()
    output_path = graphs_dir / 'aiotrino_payload_size_comparison.png'
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"📊 Payload size comparison saved to: {output_path}")


def print_summary(df: pd.DataFrame):
    """Print benchmark summary."""
    print("\n" + "="*80)
    print("📈 BENCHMARK SUMMARY")
    print("="*80)
    
    # Get script directory for saving CSV
    script_dir = Path(__file__).parent
    
    # Best performing encoding per metric
    print("\n🏆 BEST PERFORMERS:")
    
    # Highest throughput
    best_throughput = df.loc[df['rows_per_second'].idxmax()]
    print(f"Throughput:    {best_throughput['encoding']:12} - {best_throughput['rows_per_second']:,.0f} rows/s ({best_throughput['target_rows']:,} rows, {best_throughput['query_type']})")
    
    # Lowest fetch time
    best_time = df.loc[df['fetch_decode_time'].idxmin()]
    print(f"Fastest:       {best_time['encoding']:12} - {best_time['fetch_decode_time']:.3f}s ({best_time['target_rows']:,} rows, {best_time['query_type']})")
    
    # Best compression
    valid_sizes = df[df['data_size_mb'] > 0]
    if not valid_sizes.empty:
        best_compression = valid_sizes.loc[valid_sizes['data_size_mb'].idxmin()]
        print(f"Compression:   {best_compression['encoding']:12} - {best_compression['data_size_mb']:.2f} MB ({best_compression['target_rows']:,} rows, {best_compression['query_type']})")
    
    # Average performance by encoding
    print(f"\n📊 AVERAGE PERFORMANCE (all tests):")
    avg_perf = df.groupby('encoding').agg({
        'fetch_decode_time': 'mean',
        'query_cpu_time': 'mean',
        'rows_per_second': 'mean', 
        'data_size_mb': 'mean',
        'mb_per_second': 'mean'
    }).round(3)
    
    print(avg_perf.to_string())
    
    # Export detailed results
    csv_path = script_dir / 'aiotrino_benchmark_results.csv'
    df.to_csv(csv_path, index=False)
    print(f"\n💾 Detailed results saved to: {csv_path}")


async def main():
    """Main benchmark execution."""
    print("🔧 Checking Trino connection...")
   
    df = await run_benchmarks()
    
    if df.empty:
        print("❌ No benchmark results collected!")
        sys.exit(1)
    
    # Generate visualizations and summary
    create_performance_graphs(df)
    print_summary(df)
    
    print("\n🎉 Benchmark complete!")
    print("📁 Output files are in:")
    script_dir = Path(__file__).parent
    print(f"   {script_dir}")
    print("   ├── graphs/")
    print("   │   ├── aiotrino_encoding_performance.png")
    print("   │   ├── aiotrino_query_type_comparison.png")
    print("   │   └── aiotrino_payload_size_comparison.png")
    print("   └── aiotrino_benchmark_results.csv")


if __name__ == "__main__":
    asyncio.run(main()) 