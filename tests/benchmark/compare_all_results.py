#!/usr/bin/env python3
"""Simple script to compare all benchmark results."""

import pandas as pd
import matplotlib.pyplot as plt
import os
import glob
from math import log2
def read_all_results():
    """Read all benchmark CSV files."""
    results = {}
    
    # Find all benchmark result files
    csv_files = glob.glob("benchmark_results_*.csv")
    
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
            results[client_name] = df
            print(f"✅ Loaded {client_name}: {len(df)} results")
        else:
            print(f"❌ Missing {csv_file}")
    
    return results

def create_comparison_graph(results):
    """Create a comparison graph."""
    if not results:
        print("❌ No results to compare!")
        return
    
    plt.figure(figsize=(15, 10))
    
    # Plot 1: Throughput vs Dataset Size
    plt.subplot(2, 3, 1)
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
    plt.subplot(2, 3, 2)
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
    plt.subplot(2, 3, 3)
    for client_name, df in results.items():
        plt.plot(df['rows'], df['rows_per_sec'], marker='^', label=client_name, linewidth=2)
    
    plt.xlabel('Number of Rows')
    plt.ylabel('Throughput (rows/sec)')
    plt.title('Throughput by Number of rows')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    
    # Plot 4: Max Throughput Comparison
    plt.subplot(2, 3, 4)
    max_data = {}
    for client_name, df in results.items():
        max_data[client_name] = df['rows_per_sec'].max()
    
    clients = list(max_data.keys())
    throughputs = list(max_data.values())
    
    bars = plt.bar(clients, throughputs, alpha=0.8, edgecolor='black')
    plt.ylabel('Max Throughput (rows/sec)')
    plt.title('Maximum Throughput Comparison')
    plt.xticks(rotation=45)
    
    # Add value labels on bars
    for bar, value in zip(bars, throughputs):
        plt.text(bar.get_x() + bar.get_width()/2., bar.get_height() + max(throughputs)*0.01,
                f'{value:,.0f}', ha='center', va='bottom', fontweight='bold')
    
    # Plot 5: Performance Evolution
    plt.subplot(2, 3, 5)
    for client_name, df in results.items():
        efficiency = df['rows'] / df['time_ms']  # rows per millisecond
        plt.plot(df['rows'], efficiency, marker='*', label=client_name, linewidth=2)
    
    plt.xlabel('Number of Rows')
    plt.ylabel('Efficiency (rows/ms)')
    plt.title('Processing Efficiency')
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.yscale('log')
    
    # Plot 6: Summary Table
    plt.subplot(2, 3, 6)
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
    plt.savefig('benchmark_comparison.png', dpi=300, bbox_inches='tight')
    print("📊 Graph saved as benchmark_comparison.png")

def main():
    """Main function."""
    print("📈 Comparing all benchmark results...")
    
    results = read_all_results()
    
    if not results:
        print("❌ No benchmark results found!")
        print("Run benchmarks first:")
        print("  ./run_all.sh")
        print("  # or individual:")
        print("  python run_benchmark.py")
        print("  cd java-jdbc && mvn exec:java")
        return
    
    create_comparison_graph(results)
    
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