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
package com.jerolba.carpet.samples;

import java.sql.*;
import java.io.*;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.jerolba.carpet.CarpetReader;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import com.jerolba.carpet.ColumnNamingStrategy;

/**
 * Example demonstrating dynamic JDBC to Parquet export using DuckDB and PostgreSQL.
 * This example shows how to export data without predefined Java record classes.
 */
public class DynamicJdbcExportExample {

    public static void main(String[] args) {
        try {
            // Create DuckDB in-memory database connection
            try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {

                // Set up sample data
                setupSampleData(connection);

                // Example 1: Simple export
                System.out.println("=== Example 1: Simple Export ===");
                simpleExportExample(connection);

                // Example 2: Configured export
                System.out.println("\n=== Example 2: Configured Export ===");
                configuredExportExample(connection);

                // Example 3: Complex queries and joins
                System.out.println("\n=== Example 3: Complex Query Export ===");
                complexQueryExample(connection);

                // Example 4: Schema analysis
                System.out.println("\n=== Example 4: Schema Analysis ===");
                schemaAnalysisExample(connection);

                // Example 5: PostgreSQL export (requires PostgreSQL server)
                System.out.println("\n=== Example 5: PostgreSQL Export ===");
                postgreSqlExample();

                System.out.println("\nAll examples completed successfully!");

            }
        } catch (SQLException | IOException e) {
            System.err.println("Example failed: " + e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * Example 1: Basic export without configuration
     */
    private static void simpleExportExample(Connection connection) throws SQLException, IOException {
        // Define SQL query
        String sql = "SELECT id, name, department, salary FROM employees";

        // Create output file
        File outputFile = new File("employees_simple.parquet");

        // Export without any predefined record class
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        System.out.println("Exported " + outputFile.getName());

        // Verify by reading back
        verifyExport(outputFile);

        // Clean up
        outputFile.delete();
    }

    /**
     * Example 2: Export with custom configuration
     */
    private static void configuredExportExample(Connection connection) throws SQLException, IOException {
        // Complex query with various data types
        String sql = """
            SELECT
                e.id,
                e.name,
                e.department,
                e.salary,
                p.project_name,
                p.budget,
                p.start_date
            FROM employees e
            LEFT JOIN projects p ON e.id = p.manager_id
            ORDER BY e.department, e.name
            """;

        File outputFile = new File("employee_projects_configured.parquet");

        // Configure export settings
        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(500)
            .withFetchSize(500)
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE);

        // Export with configuration
        DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        System.out.println("Exported " + outputFile.getName() + " with GZIP compression");

        // Verify export
        verifyExport(outputFile);

        // Clean up
        outputFile.delete();
    }

    /**
     * Example 3: Export complex queries with aggregations
     */
    private static void complexQueryExample(Connection connection) throws SQLException, IOException {
        // Aggregation query with GROUP BY and calculations
        String sql = """
            SELECT
                d.department_name,
                COUNT(e.id) as employee_count,
                AVG(e.salary) as avg_salary,
                SUM(p.budget) as total_budget,
                MAX(e.salary) as max_salary,
                MIN(e.salary) as min_salary
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            LEFT JOIN projects p ON d.id = p.department_id
            GROUP BY d.department_name
            HAVING COUNT(e.id) > 0
            ORDER BY employee_count DESC
            """;

        File outputFile = new File("department_summary.parquet");

        // Use default configuration
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        System.out.println("Exported complex aggregation query to " + outputFile.getName());

        // Verify and show results
        List<Map<String, Object>> records = verifyExport(outputFile);

        // Print summary
        System.out.println("Department Summary:");
        for (Map<String, Object> record : records) {
            System.out.printf("  %s: %d employees, avg salary: %.2f, total budget: %.2f%n",
                record.get("department_name"),
                record.get("employee_count"),
                record.get("avg_salary"),
                record.get("total_budget"));
        }

        // Clean up
        outputFile.delete();
    }

    /**
     * Example 4: Schema analysis and metadata extraction
     */
    private static void schemaAnalysisExample(Connection connection) throws SQLException {
        // Analyze a complex query schema
        String sql = """
            SELECT
                e.id,
                e.name,
                e.salary,
                e.hire_date,
                d.department_name,
                p.project_name,
                p.budget,
                p.is_active
            FROM employees e
            JOIN departments d ON e.department_id = d.id
            LEFT JOIN projects p ON e.id = p.manager_id
            LIMIT 1
            """;

        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = resultSet.getMetaData();

            System.out.println("Query Schema Analysis:");
            System.out.println("======================");

            List<DynamicJdbcExporter.ColumnInfo> columns =
                DynamicJdbcExporter.analyzeResultSet(metaData);

            for (DynamicJdbcExporter.ColumnInfo column : columns) {
                System.out.printf("Column: %-20s | Type: %-15s | Java: %-25s | Nullable: %s%n",
                    column.label(),
                    column.typeName(),
                    column.className(),
                    column.nullable() ? "YES" : "NO");
            }
        }
    }

    /**
     * Helper method to verify export and read back results
     */
    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> verifyExport(File outputFile) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(outputFile, (Class<Map<String, Object>>) (Class<?>) Map.class);
        List<Map<String, Object>> records = reader.toList();
        System.out.printf("  - Records exported: %d%n", records.size());

        if (!records.isEmpty()) {
            System.out.println("  - Sample record:");
            Map<String, Object> firstRecord = records.get(0);
            for (Map.Entry<String, Object> entry : firstRecord.entrySet()) {
                System.out.printf("    %s: %s (%s)%n",
                    entry.getKey(),
                    entry.getValue(),
                    entry.getValue() != null ? entry.getValue().getClass().getSimpleName() : "null");
            }
        }

        return records;
    }

    /**
     * Set up sample data for demonstrations
     */
    private static void setupSampleData(Connection connection) throws SQLException {
        Statement stmt = connection.createStatement();

        // Create departments table
        stmt.execute("""
            CREATE TABLE departments (
                id INT PRIMARY KEY,
                department_name VARCHAR(100)
            )
        """);

        stmt.execute("""
            INSERT INTO departments VALUES
            (1, 'Engineering'),
            (2, 'Marketing'),
            (3, 'Sales'),
            (4, 'HR')
        """);

        // Create employees table
        stmt.execute("""
            CREATE TABLE employees (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                department_id INT,
                salary DECIMAL(10, 2),
                hire_date DATE
            )
        """);

        stmt.execute("""
            INSERT INTO employees VALUES
            (1, 'John Doe', 1, 85000.00, '2020-01-15'),
            (2, 'Jane Smith', 1, 95000.00, '2019-03-22'),
            (3, 'Bob Johnson', 2, 65000.00, '2021-06-10'),
            (4, 'Alice Brown', 1, 110000.00, '2018-11-05'),
            (5, 'Charlie Wilson', 3, 75000.00, '2022-02-28'),
            (6, 'Diana Davis', 4, 60000.00, '2021-09-15')
        """);

        // Create projects table
        stmt.execute("""
            CREATE TABLE projects (
                id INT PRIMARY KEY,
                project_name VARCHAR(200),
                department_id INT,
                manager_id BIGINT,
                budget DECIMAL(12, 2),
                start_date DATE,
                is_active BOOLEAN
            )
        """);

        stmt.execute("""
            INSERT INTO projects VALUES
            (1, 'Database Migration', 1, 1, 150000.00, '2024-01-01', true),
            (2, 'Mobile App Development', 1, 2, 200000.00, '2024-02-15', true),
            (3, 'Marketing Campaign', 2, 3, 50000.00, '2024-03-01', true),
            (4, 'Sales Analytics Platform', 3, 5, 80000.00, '2024-01-15', false),
            (5, 'HR Management System', 4, 6, 120000.00, '2024-04-01', true)
        """);

        System.out.println("Sample data setup completed.");
    }

    /**
     * Example 5: PostgreSQL export example
     * Note: This requires a running PostgreSQL server and the connection details to be configured
     */
    private static void postgreSqlExample() throws SQLException, IOException {
        // PostgreSQL connection configuration - update these values for your setup
        String url = "jdbc:postgresql://localhost:5432/testdb";
        String user = "postgres";
        String password = "password";

        // For demonstration, we'll check if PostgreSQL driver is available and show example usage
        try {
            // Check if PostgreSQL driver is available
            Class.forName("org.postgresql.Driver");
            System.out.println("PostgreSQL JDBC driver is available.");

            System.out.println("PostgreSQL Example (requires actual PostgreSQL server):");
            System.out.println("=========================================================");
            System.out.println("Connection URL: " + url);
            System.out.println("This example would:");
            System.out.println("1. Connect to PostgreSQL database");
            System.out.println("2. Execute: SELECT * FROM employees WHERE department = 'Engineering'");
            System.out.println("3. Export results to 'postgresql_employees.parquet'");
            System.out.println("4. Use GZIP compression and batch size of 500");

            // Example code (commented out since it requires actual database)
            /*
            Properties props = new Properties();
            props.setProperty("user", user);
            props.setProperty("password", password);
            props.setProperty("ssl", "false");

            try (Connection pgConnection = DriverManager.getConnection(url, props)) {
                String sql = "SELECT * FROM employees WHERE department = 'Engineering'";
                File outputFile = new File("postgresql_employees.parquet");

                DynamicExportConfig config = new DynamicExportConfig()
                    .withBatchSize(500)
                    .withCompressionCodec(CompressionCodecName.GZIP)
                    .withFetchSize(500);

                DynamicJdbcExporter.exportWithConfig(pgConnection, sql, outputFile, config);

                System.out.println("PostgreSQL export completed successfully!");

                // Verify export
                List<Map<String, Object>> records = verifyExport(outputFile);
                System.out.println("Exported " + records.size() + " records from PostgreSQL");

                // Clean up
                outputFile.delete();
            }
            */

        } catch (ClassNotFoundException e) {
            System.out.println("PostgreSQL JDBC driver not found. Add dependency: org.postgresql:postgresql:42.7.3");
        }

        // MySQL Example
        try {
            Class.forName("com.mysql.cj.jdbc.Driver");

            System.out.println("\n=== MySQL Example ===");

            String mysqlUrl = "jdbc:mysql://localhost:3306/testdb";
            String mysqlUser = "root";
            String mysqlPassword = "password";

            try (Connection mysqlConnection = DriverManager.getConnection(mysqlUrl, mysqlUser, mysqlPassword)) {

                // Setup sample data
                setupSampleData(mysqlConnection);

                // Export with custom configuration
                String sql = "SELECT e.employee_id, e.employee_name, e.salary, e.hire_date, " +
                           "e.is_active, d.department_name, e.created_at " +
                           "FROM employees e " +
                           "JOIN departments d ON e.department_id = d.department_id " +
                           "WHERE e.department_id = 1";

                File outputFile = new File("mysql_employees_export.parquet");

                DynamicExportConfig config = new DynamicExportConfig()
                    .withBatchSize(1000)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withFetchSize(500);

                DynamicJdbcExporter.exportWithConfig(mysqlConnection, sql, outputFile, config);

                System.out.println("MySQL export completed successfully!");

                // Verify export
                List<Map<String, Object>> records = verifyExport(outputFile);
                System.out.println("Exported " + records.size() + " records from MySQL");

                // Clean up
                outputFile.delete();
            }

        } catch (ClassNotFoundException e) {
            System.out.println("MySQL JDBC driver not found. Add dependency: com.mysql:mysql-connector-j:8.0.33");
        }

        // SQLite Example
        try {
            Class.forName("org.sqlite.JDBC");

            System.out.println("\n=== SQLite Example ===");

            // SQLite uses file-based databases, no server needed
            String sqliteUrl = "jdbc:sqlite:sample.db";

            try (Connection sqliteConnection = DriverManager.getConnection(sqliteUrl)) {

                // Setup sample data (creates tables if they don't exist)
                setupSampleData(sqliteConnection);

                // Export with custom configuration
                String sql = "SELECT e.employee_id, e.employee_name, e.salary, e.hire_date, " +
                           "e.is_active, d.department_name, e.created_at " +
                           "FROM employees e " +
                           "JOIN departments d ON e.department_id = d.department_id " +
                           "WHERE e.department_id = 1";

                File outputFile = new File("sqlite_employees_export.parquet");

                DynamicExportConfig config = new DynamicExportConfig()
                    .withBatchSize(1000)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withFetchSize(500);

                DynamicJdbcExporter.exportWithConfig(sqliteConnection, sql, outputFile, config);

                System.out.println("SQLite export completed successfully!");

                // Verify export
                List<Map<String, Object>> records = verifyExport(outputFile);
                System.out.println("Exported " + records.size() + " records from SQLite");

                // Clean up
                outputFile.delete();
            }

        } catch (ClassNotFoundException e) {
            System.out.println("SQLite JDBC driver not found. Add dependency: org.xerial:sqlite-jdbc:3.45.1.0");
        }

        System.out.println("To use these examples:");
        System.out.println("PostgreSQL:");
        System.out.println("1. Start PostgreSQL server");
        System.out.println("2. Create database: CREATE DATABASE testdb;");
        System.out.println("3. Uncomment the PostgreSQL example code");
        System.out.println("4. Update connection details as needed");
        System.out.println("");
        System.out.println("MySQL:");
        System.out.println("1. Start MySQL server");
        System.out.println("2. Create database: CREATE DATABASE testdb;");
        System.out.println("3. Uncomment the MySQL example code");
        System.out.println("4. Update connection details as needed");
        System.out.println("");
        System.out.println("SQLite:");
        System.out.println("1. No server needed - uses local file");
        System.out.println("2. Uncomment the SQLite example code");
        System.out.println("3. Update database file path if needed");
    }
}