/**
 * Copyright 2023 Jerónimo López Bezanilla
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.jerolba.carpet.jdbc;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

import com.jerolba.carpet.ColumnNamingStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Example demonstrating how to use DynamicJdbcExporter with GaussDB.
 * 
 * This is not a test but an example showing various usage patterns.
 * 
 * To run this example:
 * 1. Set environment variables:
 *    export GAUSSDB_URL="jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb"
 *    export GAUSSDB_USERNAME="your_username"
 *    export GAUSSDB_PASSWORD="your_password"
 * 
 * 2. Run the main method
 */
public class GaussDBExampleUsage {

    public static void main(String[] args) {
        // Example 1: Simple export
        simpleExport();

        // Example 2: Export with configuration
        exportWithConfiguration();

        // Example 3: Export complex query with joins
        exportComplexQuery();
    }

    /**
     * Example 1: Simple export from a single table
     */
    public static void simpleExport() {
        String url = System.getenv().getOrDefault("GAUSSDB_URL", 
            "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb");
        String username = System.getenv("GAUSSDB_USERNAME");
        String password = System.getenv("GAUSSDB_PASSWORD");

        if (username == null || password == null) {
            System.out.println("⚠️  Skipping example: GAUSSDB_USERNAME and GAUSSDB_PASSWORD not set");
            return;
        }

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            
            // Simple SELECT query
            String sql = "SELECT employee_id, employee_name, department, salary, hire_date " +
                        "FROM employees " +
                        "WHERE department = 'Engineering' " +
                        "ORDER BY employee_id";
            
            File outputFile = new File("gaussdb_employees_simple.parquet");
            
            // Export to Parquet
            DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
            
            System.out.println("✅ Simple export completed: " + outputFile.getAbsolutePath());
            
            // Clean up
            outputFile.delete();
            
        } catch (Exception e) {
            System.err.println("❌ Error during simple export: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example 2: Export with advanced configuration
     */
    public static void exportWithConfiguration() {
        String url = System.getenv().getOrDefault("GAUSSDB_URL", 
            "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb");
        String username = System.getenv("GAUSSDB_USERNAME");
        String password = System.getenv("GAUSSDB_PASSWORD");

        if (username == null || password == null) {
            System.out.println("⚠️  Skipping example: GAUSSDB_USERNAME and GAUSSDB_PASSWORD not set");
            return;
        }

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            
            // Query with multiple data types
            String sql = "SELECT " +
                        "  id, " +
                        "  name, " +
                        "  email, " +
                        "  age, " +
                        "  salary, " +
                        "  is_active, " +
                        "  created_at, " +
                        "  updated_at " +
                        "FROM employees " +
                        "WHERE created_at >= CURRENT_DATE - INTERVAL '1 year'";
            
            File outputFile = new File("gaussdb_employees_configured.parquet");
            
            // Configure export settings
            DynamicExportConfig config = new DynamicExportConfig()
                .withBatchSize(5000)              // Process 5000 rows at a time
                .withFetchSize(1000)              // Fetch 1000 rows from database at once
                .withCompressionCodec(CompressionCodecName.SNAPPY)  // Use SNAPPY compression
                .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE)  // Convert to snake_case
                .withConvertCamelCase(true);      // Convert camelCase columns
            
            // Export with configuration
            DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
            
            System.out.println("✅ Configured export completed: " + outputFile.getAbsolutePath());
            
            // Clean up
            outputFile.delete();
            
        } catch (Exception e) {
            System.err.println("❌ Error during configured export: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example 3: Export complex query with JOINs and aggregations
     */
    public static void exportComplexQuery() {
        String url = System.getenv().getOrDefault("GAUSSDB_URL", 
            "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb");
        String username = System.getenv("GAUSSDB_USERNAME");
        String password = System.getenv("GAUSSDB_PASSWORD");

        if (username == null || password == null) {
            System.out.println("⚠️  Skipping example: GAUSSDB_USERNAME and GAUSSDB_PASSWORD not set");
            return;
        }

        try (Connection connection = DriverManager.getConnection(url, username, password)) {
            
            // Complex query with JOINs and aggregations
            String sql = """
                SELECT 
                    d.department_name,
                    d.location,
                    COUNT(e.employee_id) as employee_count,
                    AVG(e.salary) as avg_salary,
                    MIN(e.hire_date) as earliest_hire,
                    MAX(e.hire_date) as latest_hire
                FROM departments d
                LEFT JOIN employees e ON d.department_id = e.department_id
                WHERE d.active = true
                GROUP BY d.department_id, d.department_name, d.location
                HAVING COUNT(e.employee_id) > 0
                ORDER BY employee_count DESC
                """;
            
            File outputFile = new File("gaussdb_department_summary.parquet");
            
            // Configure for optimal performance with aggregated data
            DynamicExportConfig config = new DynamicExportConfig()
                .withBatchSize(100)               // Smaller batch for aggregated results
                .withCompressionCodec(CompressionCodecName.GZIP);  // Better compression for summary data
            
            // Export
            DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
            
            System.out.println("✅ Complex query export completed: " + outputFile.getAbsolutePath());
            
            // Clean up
            outputFile.delete();
            
        } catch (Exception e) {
            System.err.println("❌ Error during complex query export: " + e.getMessage());
            e.printStackTrace();
        }
    }
}
