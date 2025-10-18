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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.ColumnNamingStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Unit tests for DynamicJdbcExporter using GaussDB.
 *
 * These tests connect to a real GaussDB instance using connection details from environment variables:
 * - GAUSSDB_URL (default: jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb)
 * - GAUSSDB_USERNAME (required)
 * - GAUSSDB_PASSWORD (required)
 *
 * Tests will be skipped if environment variables are not set.
 *
 * GaussDB is based on PostgreSQL, so most PostgreSQL features should work,
 * but with some vendor-specific extensions and differences.
 */
class DynamicJdbcExporterGaussDBTest {

    private Connection connection;
    private boolean connectionAvailable = false;

    @BeforeEach
    void setUp() {
        // Get connection details from environment variables
        String url = System.getenv().getOrDefault("GAUSSDB_URL",
            "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb?currentSchema=sit_suncbs_coredb");
        String username = System.getenv("GAUSSDB_USERNAME");
        String password = System.getenv("GAUSSDB_PASSWORD");

        // Skip tests if credentials not provided
        if (username == null || password == null) {
            System.out.println("⚠️  Skipping GaussDB tests: GAUSSDB_USERNAME and GAUSSDB_PASSWORD environment variables not set");
            connectionAvailable = false;
            return;
        }

        try {
            // Attempt to connect to GaussDB
            connection = DriverManager.getConnection(url, username, password);

            // Test if we have necessary permissions by trying to create a test schema
            try {
                createGaussDBTestTables();
                connectionAvailable = true;
                System.out.println("✅ Connected to GaussDB at: " + url);
            } catch (SQLException e) {
                // If we can't create tables, skip tests
                System.out.println("⚠️  Skipping GaussDB tests: Insufficient permissions - " + e.getMessage());
                System.out.println("    Hint: User needs CREATE TABLE permissions in the database or a specific schema.");
                connectionAvailable = false;
                if (connection != null) {
                    connection.close();
                    connection = null;
                }
            }
        } catch (SQLException e) {
            System.out.println("⚠️  Skipping GaussDB tests: Could not connect to database - " + e.getMessage());
            connectionAvailable = false;
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null && !connection.isClosed()) {
            // Clean up test tables
            try (Statement stmt = connection.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS gaussdb_employees CASCADE");
                stmt.execute("DROP TABLE IF EXISTS gaussdb_products CASCADE");
                stmt.execute("DROP TABLE IF EXISTS gaussdb_nullable_data CASCADE");
                stmt.execute("DROP TABLE IF EXISTS gaussdb_numeric_types CASCADE");
                stmt.execute("DROP TABLE IF EXISTS gaussdb_date_time_types CASCADE");
            } catch (SQLException e) {
                // Ignore cleanup errors
            }
            connection.close();
        }
    }

    @Test
    void testGaussDBBasicTypes(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, name, email, age, salary, created_at FROM gaussdb_employees ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_employees.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 employee records");

        // Verify first record
        Map<String, Object> firstRecord = records.get(0);
        assertEquals(1L, firstRecord.get("id"));
        assertEquals("Zhang Wei", firstRecord.get("name"));
        assertEquals("zhang.wei@example.com", firstRecord.get("email"));
        assertEquals(28, firstRecord.get("age"));
        assertEquals(new BigDecimal("85000.50"), firstRecord.get("salary"));
        assertNotNull(firstRecord.get("created_at"));
    }

    @Test
    void testGaussDBNumericTypes(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, small_val, medium_val, large_val, float_val, double_val, decimal_val FROM gaussdb_numeric_types ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_numeric.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 numeric records");

        // Verify numeric types
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("small_val"));
        assertNotNull(firstRecord.get("medium_val"));
        assertNotNull(firstRecord.get("large_val"));
        assertNotNull(firstRecord.get("float_val"));
        assertNotNull(firstRecord.get("double_val"));
        assertNotNull(firstRecord.get("decimal_val"));
    }

    @Test
    void testGaussDBDateTimeTypes(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, date_val, time_val, timestamp_val FROM gaussdb_date_time_types ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_datetime.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 datetime records");

        // Verify datetime types
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("date_val"));
        assertNotNull(firstRecord.get("time_val"));
        assertNotNull(firstRecord.get("timestamp_val"));
    }

    @Test
    void testGaussDBNullHandling(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, required_value, optional_value FROM gaussdb_nullable_data ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_nulls.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 records");

        // Verify null handling
        Map<String, Object> firstRecord = records.get(0);
        assertNull(firstRecord.get("optional_value"), "First record should have null optional_value");
        assertNotNull(firstRecord.get("required_value"), "First record should have non-null required_value");

        Map<String, Object> secondRecord = records.get(1);
        assertNotNull(secondRecord.get("optional_value"), "Second record should have non-null optional_value");
    }

    @Test
    void testGaussDBWithConfiguration(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, name, email FROM gaussdb_employees ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_configured.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(2)
            .withFetchSize(10)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE);

        // When
        DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 employee records");
    }

    @Test
    void testGaussDBTextTypes(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT id, name, email FROM gaussdb_products ORDER BY id";
        File outputFile = tempDir.resolve("gaussdb_text.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 product records");

        // Verify text data
        Map<String, Object> firstRecord = records.get(0);
        assertTrue(firstRecord.get("name") instanceof String);
        assertTrue(firstRecord.get("email") instanceof String);
    }

    @Test
    void testGaussDBEmptyResultSet(@TempDir Path tempDir) throws SQLException, IOException {
        assumeTrue(connectionAvailable, "GaussDB connection not available");

        // Given
        String sql = "SELECT * FROM gaussdb_employees WHERE id > 10000";
        File outputFile = tempDir.resolve("gaussdb_empty.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created even for empty results");

        // Verify we can read it back (should be empty)
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(0, records.size(), "Should have 0 records");
    }

    // Helper methods

    private void createGaussDBTestTables() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            // Drop tables if they exist (for cleanup from previous failed runs)
            stmt.execute("DROP TABLE IF EXISTS gaussdb_employees CASCADE");
            stmt.execute("DROP TABLE IF EXISTS gaussdb_products CASCADE");
            stmt.execute("DROP TABLE IF EXISTS gaussdb_nullable_data CASCADE");
            stmt.execute("DROP TABLE IF EXISTS gaussdb_numeric_types CASCADE");
            stmt.execute("DROP TABLE IF EXISTS gaussdb_date_time_types CASCADE");

            // Employees table with basic types
            stmt.execute("""
                CREATE TABLE gaussdb_employees (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(100) NOT NULL,
                    email VARCHAR(255) NOT NULL,
                    age INTEGER,
                    salary DECIMAL(10, 2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """);

            stmt.execute("""
                INSERT INTO gaussdb_employees (id, name, email, age, salary, created_at)
                VALUES
                    (1, 'Zhang Wei', 'zhang.wei@example.com', 28, 85000.50, '2024-01-15 09:30:00'),
                    (2, 'Li Na', 'li.na@example.com', 35, 92000.00, '2024-02-20 14:15:00'),
                    (3, 'Wang Fang', 'wang.fang@example.com', 42, 105000.75, '2024-03-10 11:45:00')
                """);

            // Products table
            stmt.execute("""
                CREATE TABLE gaussdb_products (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(200) NOT NULL,
                    email VARCHAR(255),
                    price DECIMAL(12, 2)
                )
                """);

            stmt.execute("""
                INSERT INTO gaussdb_products (id, name, email, price)
                VALUES
                    (1, 'Product A', 'contact@producta.com', 199.99),
                    (2, 'Product B', 'contact@productb.com', 299.50)
                """);

            // Nullable data table
            stmt.execute("""
                CREATE TABLE gaussdb_nullable_data (
                    id INTEGER PRIMARY KEY,
                    required_value VARCHAR(50) NOT NULL,
                    optional_value VARCHAR(50)
                )
                """);

            stmt.execute("""
                INSERT INTO gaussdb_nullable_data (id, required_value, optional_value)
                VALUES
                    (1, 'Required 1', NULL),
                    (2, 'Required 2', 'Optional 2'),
                    (3, 'Required 3', NULL)
                """);

            // Numeric types table
            stmt.execute("""
                CREATE TABLE gaussdb_numeric_types (
                    id INTEGER PRIMARY KEY,
                    small_val SMALLINT,
                    medium_val INTEGER,
                    large_val BIGINT,
                    float_val REAL,
                    double_val DOUBLE PRECISION,
                    decimal_val DECIMAL(15, 4)
                )
                """);

            stmt.execute("""
                INSERT INTO gaussdb_numeric_types
                VALUES
                    (1, 100, 50000, 9876543210, 3.14159, 2.718281828, 12345.6789),
                    (2, -200, -100000, -1234567890, -1.414, -3.141592653, -9876.5432)
                """);

            // Date/time types table
            stmt.execute("""
                CREATE TABLE gaussdb_date_time_types (
                    id INTEGER PRIMARY KEY,
                    date_val DATE,
                    time_val TIME,
                    timestamp_val TIMESTAMP
                )
                """);

            stmt.execute("""
                INSERT INTO gaussdb_date_time_types
                VALUES
                    (1, '2024-10-18', '14:30:00', '2024-10-18 14:30:00'),
                    (2, '2025-01-01', '09:00:00', '2025-01-01 09:00:00')
                """);
        }
    }

    /**
     * Helper method to read Parquet file back for verification
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }
}
