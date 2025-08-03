import java.sql.*;
import java.io.FileWriter;
import java.io.IOException;

public class SimpleBenchmark {
    public static void main(String[] args) throws SQLException, IOException {
        // Get configuration from environment variables
        String host = System.getenv().getOrDefault("TRINO_HOST", "localhost");
        String port = System.getenv().getOrDefault("TRINO_PORT", "8092");
        String user = System.getenv().getOrDefault("TRINO_USER", "test");
        String queryTemplate = System.getenv().getOrDefault("QUERY_TEMPLATE", "SELECT * FROM store_sales LIMIT %d");
        int minPower = Integer.parseInt(System.getenv().getOrDefault("MIN_POWER", "10"));
        int maxPower = Integer.parseInt(System.getenv().getOrDefault("MAX_POWER", "24"));
        int step = Integer.parseInt(System.getenv().getOrDefault("POWER_STEP", "2"));
        
        String url = String.format("jdbc:trino://%s:%s/tpcds/sf100000", host, port);
        
        StringBuilder csvContent = new StringBuilder("power,rows,time_ms,rows_per_sec\n");
        
        System.out.println("🚀 Running Java JDBC benchmark...");
        System.out.println();
        
        try (Connection conn = DriverManager.getConnection(url, user, "")) {
            for (int power = minPower; power <= maxPower; power += step) {
                int rows = (int) Math.pow(2, power);
                
                // If min_power == max_power, use query template as-is without templating
                String query;
                if (minPower == maxPower) {
                    query = queryTemplate;
                } else {
                    query = String.format(queryTemplate, rows);
                }
                
                long start = System.currentTimeMillis();
                try (Statement stmt = conn.createStatement();
                     ResultSet rs = stmt.executeQuery(query)) {
                    
                    int count = 0;
                    while (rs.next()) {
                        count++;
                    }
                    
                    long time = System.currentTimeMillis() - start;
                    double throughput = count * 1000.0 / time;
                    
                    System.out.printf("2^%2d: %,8d rows in %,6d ms (%,8.0f rows/s)%n", 
                                    power, count, time, throughput);
                    
                    csvContent.append(String.format("%d,%d,%d,%.2f%n", 
                        power, count, time, throughput));
                }
            }
        }
        
        // Write CSV file to parent directory
        try (FileWriter writer = new FileWriter("../benchmark_results_java_jdbc.csv")) {
            writer.write(csvContent.toString());
        }
        System.out.println();
        System.out.println("📊 Results exported to benchmark_results_java_jdbc.csv");
    }
} 