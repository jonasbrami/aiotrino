#!/usr/bin/env python3
"""Simple script to compare all benchmark results."""

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
import sys
import argparse
from math import log2

def read_all_results(directory="."):
    """Read all benchmark CSV files from the specified directory."""
    results = {}
    
    # Find all benchmark result files in the specified directory
    search_pattern = os.path.join(directory, "benchmark_results_*.csv")
    csv_files = glob.glob(search_pattern)
    
    for csv_file in csv_files:
        # Extract client name from filename
        name_part = csv_file.replace("benchmark_results_", "").replace(".csv", "")
        
        # Clean up the name for display
        if "json_zstd" in name_part:
            client_name = "Python JSON+ZSTD"
        elif "arrow_zstd" in name_part:
            client_name = "Python Arrow+ZSTD"
        elif "java_jdbc" in name_part:
            client_name = "Java JDBC"
        else:
            client_name = name_part.replace("_", " ").title()
        
        if os.path.exists(csv_file):
            df = pd.read_csv(csv_file)
            # Always compute throughput from rows and time_ms
            if {'rows','time_ms'}.issubset(df.columns):
                # Ensure numeric types and compute throughput
                df['rows'] = pd.to_numeric(df['rows'], errors='coerce')
                df['time_ms'] = pd.to_numeric(df['time_ms'], errors='coerce')
                df['rows_per_sec'] = df.apply(
                    lambda r: (r['rows'] / (r['time_ms'] / 1000.0)) if r['time_ms'] and r['time_ms'] > 0 else float('nan'), axis=1
                )
                results[client_name] = df
                print(f"✅ Loaded {client_name}: {len(df)} results")
            else:
                print(f"⚠️  Skipping {client_name}: missing required columns (rows, time_ms)")
        else:
            print(f"❌ Missing {csv_file}")
    
    return results

def create_comparison_graph(results, output_directory="."):
    """Create a comparison graph and save it in the specified directory."""
    if not results:
        print("❌ No results to compare!")
        return
    
    plt.figure(figsize=(12, 8))
    
    # Plot 1: Throughput vs Dataset Size
    plt.subplot(2, 2, 1)
    for client_name, df in results.items():
        plt.plot(df['rows'], df['rows_per_sec'], marker='o', label=client_name, linewidth=2)
    
    plt.xlabel('Number of Rows')
    plt.ylabel('Throughput (rows/sec)')
    plt.title('Throughput vs Dataset Size')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 2: Time vs Dataset Size
    plt.subplot(2, 2, 2)
    for client_name, df in results.items():
        plt.plot(df['rows'], df['time_ms'], marker='s', label=client_name, linewidth=2)
    
    plt.xlabel('Number of Rows')
    plt.ylabel('Time (ms)')
    plt.title('Execution Time vs Dataset Size')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.xscale('log')
    plt.yscale('log')
    
    # Plot 3: Throughput by Number of rows
    plt.subplot(2, 2, 3)
    for client_name, df in results.items():
        plt.plot(df['rows'], df['rows_per_sec'], marker='^', label=client_name, linewidth=2)
    
    plt.xlabel('Number of Rows')
    plt.ylabel('Throughput (rows/sec)')
    plt.title('Throughput by Number of rows')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
        
    # Plot 4: Summary Table
    plt.subplot(2, 2, 4)
    plt.axis('off')
    
    summary_data = []
    for client_name, df in results.items():
        avg_throughput = df['rows_per_sec'].mean()
        max_throughput = df['rows_per_sec'].max()
        total_rows = df['rows'].sum()
        total_time = df['time_ms'].sum() / 1000  # seconds
        summary_data.append([
            client_name[:15],  # Truncate long names
            f'{avg_throughput:,.0f}',
            f'{max_throughput:,.0f}',
            f'{total_rows:,.0f}',
            f'{total_time:.1f}s'
        ])
    
    if summary_data:
        table = plt.table(cellText=summary_data,
                         colLabels=['Client', 'Avg T/put', 'Max T/put', 'Total Rows', 'Total Time'],
                         cellLoc='center',
                         loc='center')
        table.auto_set_font_size(False)
        table.set_fontsize(8)
        table.scale(1.2, 1.5)
        plt.title('Performance Summary', pad=20)
    
    plt.tight_layout()
    output_path = os.path.join(output_directory, 'benchmark_comparison.png')
    plt.savefig(output_path, dpi=300, bbox_inches='tight')
    print(f"📊 Graph saved as {output_path}")

def main():
    """Main function."""
    parser = argparse.ArgumentParser(description="Compare benchmark results from CSV files in a directory.")
    parser.add_argument("directory", help="Directory containing benchmark CSV files")
    args = parser.parse_args()
    
    if not os.path.isdir(args.directory):
        print(f"❌ Directory '{args.directory}' does not exist!")
        sys.exit(1)
    
    print(f"📈 Comparing benchmark results in directory: {args.directory}")
    
    results = read_all_results(args.directory)
    
    if not results:
        print(f"❌ No benchmark results found in {args.directory}!")
        print("Run benchmarks first with --outdir pointing to this directory")
        return
    
    create_comparison_graph(results, args.directory)
    
    print("\n🏆 Performance Summary:")
    max_rows = max([df['rows'].max() for df in results.values() if not df.empty])
    print(f"Results at maximum scale ({max_rows} rows):")
    
    max_scale_data = []
    for client_name, df in results.items():
        max_row = df[df['rows'] == max_rows]
        if not max_row.empty:
            throughput = max_row['rows_per_sec'].iloc[0]
            time_ms = max_row['time_ms'].iloc[0]
            max_scale_data.append((client_name, throughput, time_ms))
    
    # Sort by throughput descending
    max_scale_data.sort(key=lambda x: x[1], reverse=True)
    
    print(f"{'Client':<20} | {'Throughput (rows/s)':<20} | {'Time (ms)':<12}")
    print(f"{'-'*20} | {'-'*20} | {'-'*12}")
    for client, throughput, time_ms in max_scale_data:
        print(f"{client:<20} | {throughput:>15,.0f} | {time_ms:>8,.0f}")
    
    print(f"\n🎉 Comparison complete!")

if __name__ == "__main__":
    main() 