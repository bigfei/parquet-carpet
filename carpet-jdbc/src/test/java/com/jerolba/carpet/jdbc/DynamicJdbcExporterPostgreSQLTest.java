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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.ColumnNamingStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

/**
 * Unit tests for DynamicJdbcExporter using PostgreSQL.
 * These tests require Docker and PostgreSQL container.
 */
@Testcontainers
class DynamicJdbcExporterPostgreSQLTest {

    @Container
    @SuppressWarnings("resource") // Testcontainers handles lifecycle automatically
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(
        DockerImageName.parse("postgres:15-alpine"))
        .withDatabaseName("testdb")
        .withUsername("testuser")
        .withPassword("testpass");

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        // Connect to PostgreSQL container
        connection = DriverManager.getConnection(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );

        // Create test tables with PostgreSQL-specific features
        createPostgreSQLTestTables();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testPostgreSQLBasicTypes(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, email, age, salary, created_at FROM employees";
        File outputFile = tempDir.resolve("postgresql_employees.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
        assertEquals(3, totalRows, "Should export 3 rows");

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 employee records");

        // Verify first record
        Map<String, Object> firstRecord = records.get(0);
        assertEquals(1L, firstRecord.get("id"));
        assertEquals("John Doe", firstRecord.get("name"));
        assertEquals("john.doe@example.com", firstRecord.get("email"));
        assertEquals(30, firstRecord.get("age"));
        assertEquals(new BigDecimal("75000.50"), firstRecord.get("salary"));
        assertNotNull(firstRecord.get("created_at"));
    }

    @Test
    void testPostgreSQLJsonType(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, metadata FROM products";
        File outputFile = tempDir.resolve("postgresql_products.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
        assertEquals(2, totalRows, "Should export 2 rows");

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 product records");

        // Verify JSON data is properly handled as String
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("metadata"));
        assertTrue(firstRecord.get("metadata") instanceof String,
            "JSON should be converted to String for Parquet");
    }

    @Test
    void testPostgreSQLArrayTypes(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, tags, scores FROM array_test";
        File outputFile = tempDir.resolve("postgresql_arrays.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
        assertEquals(2, totalRows, "Should export 2 rows");

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 records");

        // Arrays should be converted to String representations
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("tags"));
        assertNotNull(firstRecord.get("scores"));
        assertTrue(firstRecord.get("tags") instanceof String, "Arrays should be converted to String");
    }

    @Test
    void testPostgreSQLNullHandling(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM nullable_data ORDER BY id";
        File outputFile = tempDir.resolve("postgresql_nulls.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
        assertEquals(3, totalRows, "Should export 3 rows");

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
    void testPostgreSQLLargeDataset(@TempDir Path tempDir) throws SQLException, IOException {
        // Given - create a larger dataset
        Statement stmt = connection.createStatement();
        stmt.execute("INSERT INTO large_data SELECT generate_series(1, 1000), 'Name ' || generate_series(1, 1000), random() * 100000");

        String sql = "SELECT id, name, value FROM large_data";
        File outputFile = tempDir.resolve("postgresql_large.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withFetchSize(100);

        // When
        long totalRows = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
        assertEquals(1000, totalRows, "Should export 1000 rows");

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(1000, records.size(), "Should have 1000 records");

        // File size should be reasonable
        assertTrue(outputFile.length() > 0, "File should have content");
    }

    @Test
    void testPostgreSQLSchemaAnalysis() throws SQLException {
        // Given
        String sql = "SELECT id, name, email, age, salary, created_at FROM employees LIMIT 1";
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // When
            List<DynamicJdbcExporter.ColumnInfo> columns = DynamicJdbcExporter.analyzeResultSet(metaData);

            // Then
            assertEquals(6, columns.size(), "Should have 6 columns");

            DynamicJdbcExporter.ColumnInfo idColumn = columns.get(0);
            assertEquals("id", idColumn.label());
            assertEquals("id", idColumn.name());
            assertEquals(java.sql.Types.BIGINT, idColumn.type());
            // PostgreSQL returns "bigserial" for auto-incrementing BIGINT columns
            assertTrue(idColumn.typeName().toLowerCase().equals("bigint") ||
                      idColumn.typeName().toLowerCase().equals("bigserial"),
                      "Expected 'bigint' or 'bigserial' but got: " + idColumn.typeName());
            assertFalse(idColumn.nullable(), "Primary key should not be nullable in PostgreSQL");

            DynamicJdbcExporter.ColumnInfo nameColumn = columns.get(1);
            assertEquals("name", nameColumn.label());
            assertEquals(java.sql.Types.VARCHAR, nameColumn.type());
            assertFalse(nameColumn.nullable(), "Name should NOT be nullable (defined as NOT NULL)");
        }
    }

    @Test
    void testPostgreSQLConfigurableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT \"employeeId\", \"employeeName\", \"departmentName\" FROM employee_departments";
        File outputFile = tempDir.resolve("postgresql_employee_departments.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(1)  // Small batch size for testing
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME)
            .withConvertCamelCase(false);

        // When
        long totalRows = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
        assertEquals(3, totalRows, "Should export 3 rows");

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 records");

        // Verify column naming (should preserve camelCase with our config)
        Map<String, Object> firstRecord = records.get(0);
        assertTrue(firstRecord.containsKey("employeeId"), "Should contain employeeId (not employee_id)");
        assertTrue(firstRecord.containsKey("employeeName"), "Should contain employeeName (not employee_name)");
    }

    /**
     * Helper method to read Parquet file back for verification
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        try {
            return reader.toList();
        } finally {
            // Note: CarpetReader doesn't implement Closeable directly
            // The iterator handles resource cleanup
        }
    }

    /**
     * Create PostgreSQL-specific test tables with various data types
     */
    private void createPostgreSQLTestTables() throws SQLException {
        Statement stmt = connection.createStatement();

        // Clean up any existing tables
        try {
            stmt.execute("DROP TABLE IF EXISTS employees CASCADE");
            stmt.execute("DROP TABLE IF EXISTS products CASCADE");
            stmt.execute("DROP TABLE IF EXISTS array_test CASCADE");
            stmt.execute("DROP TABLE IF EXISTS nullable_data CASCADE");
            stmt.execute("DROP TABLE IF EXISTS large_data CASCADE");
            stmt.execute("DROP TABLE IF EXISTS employee_departments CASCADE");
        } catch (SQLException e) {
            // Ignore errors if tables don't exist
        }

        // Basic employees table with PostgreSQL-specific timestamp
        stmt.execute("""
            CREATE TABLE employees (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age INTEGER CHECK (age >= 18),
                salary DECIMAL(12, 2) DEFAULT 0.00,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """);

        stmt.execute("""
            INSERT INTO employees (id, name, email, age, salary, created_at) VALUES
            (1, 'John Doe', 'john.doe@example.com', 30, 75000.50, '2024-01-01 10:00:00 UTC'),
            (2, 'Jane Smith', 'jane.smith@example.com', 28, 90000.00, '2024-01-02 11:00:00 UTC'),
            (3, 'Bob Johnson', 'bob.johnson@example.com', 35, 65000.00, '2024-01-03 12:00:00 UTC')
        """);

        // Products table with JSONB support
        stmt.execute("""
            CREATE TABLE products (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(200) NOT NULL,
                metadata JSONB,
                created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
            )
        """);

        stmt.execute("""
            INSERT INTO products (id, name, metadata) VALUES
            (1, 'Laptop', '{"brand": "Dell", "ram": "16GB", "storage": "512GB SSD"}'),
            (2, 'Monitor', '{"brand": "LG", "size": "27 inch", "resolution": "4K"}')
        """);

        // Array types test table
        stmt.execute("""
            CREATE TABLE array_test (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100),
                tags TEXT[],
                scores INTEGER[]
            )
        """);

        stmt.execute("""
            INSERT INTO array_test (id, name, tags, scores) VALUES
            (1, 'Product A', ARRAY['electronics', 'computer'], ARRAY[9, 8, 10]),
            (2, 'Product B', ARRAY['accessories', 'peripheral'], ARRAY[7, 9, 8])
        """);

        // Nullable data table
        stmt.execute("""
            CREATE TABLE nullable_data (
                id BIGSERIAL PRIMARY KEY,
                required_value VARCHAR(100) NOT NULL,
                optional_value VARCHAR(100)
            )
        """);

        stmt.execute("""
            INSERT INTO nullable_data (id, required_value, optional_value) VALUES
            (1, 'required_value_1', NULL),
            (2, 'required_value_2', 'optional_value_2'),
            (3, 'required_value_3', 'optional_value_3')
        """);

        // Large dataset table
        stmt.execute("""
            CREATE TABLE large_data (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100),
                value DOUBLE PRECISION
            )
        """);

        // Table with camelCase column names
        stmt.execute("""
            CREATE TABLE employee_departments (
                "employeeId" BIGINT,
                "employeeName" VARCHAR(100),
                "departmentName" VARCHAR(50)
            )
        """);

        stmt.execute("""
            INSERT INTO employee_departments ("employeeId", "employeeName", "departmentName") VALUES
            (1, 'John Doe', 'Engineering'),
            (2, 'Jane Smith', 'Engineering'),
            (3, 'Bob Johnson', 'Marketing')
        """);
    }
}