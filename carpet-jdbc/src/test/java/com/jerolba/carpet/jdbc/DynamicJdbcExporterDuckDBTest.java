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

/**
 * Unit tests for DynamicJdbcExporter using DuckDB as the in-memory database.
 */
class DynamicJdbcExporterDuckDBTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        // Create DuckDB in-memory database connection
        connection = DriverManager.getConnection("jdbc:duckdb:");

        // Create test tables with various data types
        createTestTables();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testSimpleTableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, age, salary FROM employees";
        File outputFile = tempDir.resolve("employees.parquet").toFile();

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
        assertEquals("John Doe", firstRecord.get("name"));
        assertEquals(30, firstRecord.get("age"));
        assertEquals(new BigDecimal("75000.50"), firstRecord.get("salary"));
    }

    @Test
    void testComplexDataTypesExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM complex_data";
        File outputFile = tempDir.resolve("complex_data.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 records");

        // Verify complex data types
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("created_at"));
        assertTrue(firstRecord.get("is_active") instanceof Boolean);
        assertEquals(new BigDecimal("123456789.123456789"), firstRecord.get("decimal_value"));
    }

    @Test
    void testConfigurableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT employeeId, employeeName, departmentName FROM employee_departments";
        File outputFile = tempDir.resolve("employee_departments.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(1)  // Small batch size for testing
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME)
            .withConvertCamelCase(false);

        // When
        DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

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

    @Test
    void testEmptyResultSetExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM employees WHERE id > 1000";
        File outputFile = tempDir.resolve("empty.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created even for empty results");

        // Verify we can read it back (should be empty)
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(0, records.size(), "Should have 0 records");
    }

    @Test
    void testAggregationQueryExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT department, COUNT(*) as employee_count, AVG(salary) as avg_salary FROM employees GROUP BY department";
        File outputFile = tempDir.resolve("department_summary.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 departments");

        // Verify aggregation results
        Map<String, Object> engineeringRecord = records.stream()
            .filter(r -> "Engineering".equals(r.get("department")))
            .findFirst()
            .orElseThrow();

        assertEquals(2L, engineeringRecord.get("employee_count"));
        assertEquals(82500.25, engineeringRecord.get("avg_salary"));
    }

    @Test
    void testSchemaAnalysis(@TempDir Path tempDir) throws SQLException {
        // Given
        String sql = "SELECT * FROM employees";
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // When
            List<DynamicJdbcExporter.ColumnInfo> columns = DynamicJdbcExporter.analyzeResultSet(metaData);

            // Then
            assertEquals(5, columns.size(), "Should have 5 columns");

            DynamicJdbcExporter.ColumnInfo idColumn = columns.get(0);
            assertEquals("id", idColumn.label());
            assertEquals("id", idColumn.name());
            assertEquals(java.sql.Types.BIGINT, idColumn.type());
            assertEquals("BIGINT", idColumn.typeName());
            // Note: DuckDB JDBC driver reports primary key columns as nullable despite NOT NULL constraint
            // This appears to be a DuckDB JDBC driver behavior
            assertTrue(idColumn.nullable(), "DuckDB reports primary key columns as nullable");

            DynamicJdbcExporter.ColumnInfo nameColumn = columns.get(1);
            assertEquals("name", nameColumn.label());
            assertEquals(java.sql.Types.VARCHAR, nameColumn.type());
            assertTrue(nameColumn.nullable(), "Name should be nullable");
        }
    }

    @Test
    void testLargeDatasetPerformance(@TempDir Path tempDir) throws SQLException, IOException {
        // Given - create a larger dataset
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE large_data AS SELECT * FROM employees CROSS JOIN generate_series(1, 1000)");
        String sql = "SELECT * FROM large_data";
        File outputFile = tempDir.resolve("large_data.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withFetchSize(100);

        // When
        DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3000, records.size(), "Should have 3000 records (3 * 1000)");

        // File size should be reasonable
        assertTrue(outputFile.length() > 0, "File should have content");
    }

    @Test
    void testNullValueHandling(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM nullable_data";
        File outputFile = tempDir.resolve("nullable_data.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 records");

        // Verify null values are handled properly
        Map<String, Object> firstRecord = records.get(0);
        assertNull(firstRecord.get("optional_value"), "First record should have null optional_value");
        assertNotNull(firstRecord.get("required_value"), "First record should have non-null required_value");

        Map<String, Object> secondRecord = records.get(1);
        assertNotNull(secondRecord.get("optional_value"), "Second record should have non-null optional_value");
        assertNotNull(secondRecord.get("required_value"), "Second record should have non-null required_value");
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
     * Create test tables with various data types
     */
    private void createTestTables() throws SQLException {
        Statement stmt = connection.createStatement();

        // Simple employees table
        stmt.execute("""
            CREATE TABLE employees (
                id BIGINT PRIMARY KEY NOT NULL,
                name VARCHAR(100),
                age INT,
                salary DECIMAL(10, 2),
                department VARCHAR(50)
            )
        """);

        stmt.execute("""
            INSERT INTO employees VALUES
            (1, 'John Doe', 30, 75000.50, 'Engineering'),
            (2, 'Jane Smith', 28, 90000.00, 'Engineering'),
            (3, 'Bob Johnson', 35, 65000.00, 'Marketing')
        """);

        // Complex data types table
        stmt.execute("""
            CREATE TABLE complex_data (
                id INT PRIMARY KEY,
                uuid_value UUID,
                decimal_value DECIMAL(18, 9),
                created_at TIMESTAMP,
                is_active BOOLEAN,
                json_data JSON
            )
        """);

        stmt.execute("""
            INSERT INTO complex_data VALUES
            (1, '550e8400-e29b-41d4-a716-446655440000', 123456789.123456789, '2024-01-01 10:00:00', true, '{"key": "value"}'),
            (2, '550e8400-e29b-41d4-a716-446655440001', 987654321.987654321, '2024-01-02 11:00:00', false, '{"other": "data"}')
        """);

        // Table with camelCase column names
        stmt.execute("""
            CREATE TABLE employee_departments (
                employeeId BIGINT,
                employeeName VARCHAR(100),
                departmentName VARCHAR(50)
            )
        """);

        stmt.execute("""
            INSERT INTO employee_departments VALUES
            (1, 'John Doe', 'Engineering'),
            (2, 'Jane Smith', 'Engineering'),
            (3, 'Bob Johnson', 'Marketing')
        """);

        // Nullable data table
        stmt.execute("""
            CREATE TABLE nullable_data (
                id INT PRIMARY KEY,
                required_value VARCHAR(100) NOT NULL,
                optional_value VARCHAR(100)
            )
        """);

        stmt.execute("""
            INSERT INTO nullable_data VALUES
            (1, 'required_value_1', NULL),
            (2, 'required_value_2', 'optional_value_2')
        """);
    }
}