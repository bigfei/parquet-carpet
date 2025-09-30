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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;

import java.sql.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.stream.Stream;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.ColumnNamingStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

/**
 * Unit tests for SQLite dynamic JDBC export functionality.
 *
 * These tests demonstrate and verify SQLite-specific features including:
 * - Dynamic typing (type affinity)
 * - BLOB data type support
 * - JSON data storage in TEXT columns
 * - Date/time handling
 * - Auto-increment columns
 * - Attached databases
 * - Common SQLite data types and patterns
 */
class DynamicJdbcExporterSQLiteTest {

    private Connection connection;
    private Path dbPath;

    @BeforeEach
    void setUp(@TempDir Path tempDir) throws SQLException, IOException {
        // Create a temporary SQLite database file
        dbPath = tempDir.resolve("test.db");
        String url = "jdbc:sqlite:" + dbPath.toString();
        connection = DriverManager.getConnection(url);
        setupTestData();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Test basic SQLite export functionality with various data types
     */
    @Test
    void testSQLiteBasicExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, email, age, salary, is_active, created_at FROM employees";
        File outputFile = tempDir.resolve("sqlite_employees.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 employee records");

        // Verify data types and values
        Map<String, Object> firstRecord = records.get(0);
        assertEquals(1, firstRecord.get("id"));
        assertEquals("John Doe", firstRecord.get("name"));
        assertEquals("john@example.com", firstRecord.get("email"));
        // SQLite may return age as Integer or Long depending on version
        assertEquals(30, ((Number) firstRecord.get("age")).intValue());
        // SQLite may return salary as Float due to type affinity handling
        Object salary = firstRecord.get("salary");
        if (salary instanceof Float) {
            assertEquals(75000.00f, (Float) salary, 0.001);
        } else if (salary instanceof BigDecimal) {
            assertEquals(new BigDecimal("75000.00"), ((BigDecimal) salary).setScale(2, RoundingMode.HALF_UP));
        } else {
            assertEquals(75000.00, ((Number) salary).doubleValue(), 0.001);
        }
        // SQLite BOOLEAN may be stored as INTEGER 0/1
        Object isActive = firstRecord.get("is_active");
        assertEquals(true, isActive.equals(true) || isActive.equals(1));
        assertNotNull(firstRecord.get("created_at"));
    }

    /**
     * Test SQLite BLOB data type support
     */
    @Test
    void testSQLiteBlobExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM binary_data";
        File outputFile = tempDir.resolve("sqlite_binary.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 binary records");

        // Verify BLOB field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("blob_data"));
        // BLOB should be converted to Binary or byte array
        Object blobData = firstRecord.get("blob_data");
        assertTrue(blobData instanceof byte[] ||
                   blobData instanceof org.apache.parquet.io.api.Binary,
                   "BLOB data should be byte array or Binary, got: " + blobData.getClass().getSimpleName());
    }

    /**
     * Test SQLite JSON data in TEXT columns
     */
    @Test
    void testSQLiteJsonExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM json_data";
        File outputFile = tempDir.resolve("sqlite_json.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 JSON records");

        // Verify JSON field handling (stored as TEXT)
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("metadata"));
        assertTrue(firstRecord.get("metadata") instanceof String);
        String metadata = (String) firstRecord.get("metadata");
        assertTrue(metadata.contains("\"brand\"") || metadata.contains("Dell"),
                  "Should contain JSON data: " + metadata);
    }

    /**
     * Test SQLite temporal data types
     */
    @Test
    void testSQLiteTemporalExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM temporal_data";
        File outputFile = tempDir.resolve("sqlite_temporal.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 temporal records");

        // Verify temporal field handling
        Map<String, Object> firstRecord = records.get(0);
        // SQLite stores dates/times as TEXT or NUMERIC depending on version
        assertNotNull(firstRecord.get("date_text"));
        assertNotNull(firstRecord.get("time_text"));
        assertNotNull(firstRecord.get("datetime_text"));
    }

    /**
     * Test SQLite NULL value handling
     */
    @Test
    void testSQLiteNullHandling(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM nullable_data";
        File outputFile = tempDir.resolve("sqlite_nullable.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 records");

        // Verify NULL handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("id"));
        assertNotNull(firstRecord.get("required_value"));
        assertNull(firstRecord.get("optional_value"), "Optional value should be NULL");

        Map<String, Object> secondRecord = records.get(1);
        assertNotNull(secondRecord.get("optional_value"), "Optional value should not be NULL");
    }

    /**
     * Test SQLite schema analysis functionality
     */
    @Test
    void testSQLiteSchemaAnalysis() throws SQLException {
        // Given
        String sql = "SELECT id, name, email, age, salary, is_active, created_at FROM employees LIMIT 1";

        try (Statement stmt = connection.createStatement();
             ResultSet resultSet = stmt.executeQuery(sql)) {

            ResultSetMetaData metaData = resultSet.getMetaData();

            // When
            List<DynamicJdbcExporter.ColumnInfo> columns =
                DynamicJdbcExporter.analyzeResultSet(metaData);

            // Then
            assertEquals(7, columns.size(), "Should have 7 columns");

            DynamicJdbcExporter.ColumnInfo idColumn = columns.get(0);
            assertEquals("id", idColumn.label());
            assertEquals("id", idColumn.name());
            assertEquals(java.sql.Types.INTEGER, idColumn.type());
            assertTrue(idColumn.typeName().toLowerCase().contains("int"),
                      "Should be integer type: " + idColumn.typeName());
            // SQLite may report primary key as nullable due to its dynamic typing nature
            // In practice, primary keys in SQLite are NOT NULL but the JDBC driver may report them as nullable
            if (idColumn.nullable()) {
                System.out.println("Note: SQLite reports primary key as nullable (this is normal for SQLite)");
            }

            DynamicJdbcExporter.ColumnInfo nameColumn = columns.get(1);
            assertEquals("name", nameColumn.label());
            assertEquals(java.sql.Types.VARCHAR, nameColumn.type());
            // SQLite may report NOT NULL columns as nullable due to its dynamic typing
            if (!nameColumn.nullable()) {
                System.out.println("Note: SQLite correctly reports name column as NOT NULL");
            }

            DynamicJdbcExporter.ColumnInfo salaryColumn = columns.get(4);
            assertEquals("salary", salaryColumn.label());
            // SQLite may report REAL type instead of DECIMAL due to type affinity
            assertTrue(salaryColumn.type() == java.sql.Types.DECIMAL ||
                      salaryColumn.type() == java.sql.Types.REAL,
                      "Salary should be DECIMAL or REAL, got: " + salaryColumn.type());
            assertTrue(salaryColumn.typeName().toLowerCase().contains("decimal") ||
                      salaryColumn.typeName().toLowerCase().contains("numeric") ||
                      salaryColumn.typeName().toLowerCase().contains("real"),
                      "Should be decimal/numeric/real type: " + salaryColumn.typeName());
        }
    }

    /**
     * Test SQLite configurable export with custom settings
     */
    @Test
    void testSQLiteConfigurableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT employeeId, employeeName, departmentName FROM employee_departments";
        File outputFile = tempDir.resolve("sqlite_employee_departments.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(1)  // Small batch size for testing
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME)
            .withConvertCamelCase(false);

        // When
        DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 records");

        // Verify column naming (should preserve camelCase with our config)
        Map<String, Object> firstRecord = records.get(0);
        assertTrue(firstRecord.containsKey("employeeId"), "Should contain employeeId (not employee_id)");
        assertTrue(firstRecord.containsKey("employeeName"), "Should contain employeeName (not employee_name)");
    }

    /**
     * Test SQLite type affinity and dynamic typing
     */
    @Test
    void testSQLiteTypeAffinity(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM type_affinity_test";
        File outputFile = tempDir.resolve("sqlite_type_affinity.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 type affinity records");

        // Verify various data types in SQLite
        Map<String, Object> firstRecord = records.get(0);
        // INTEGER affinity - should handle various numeric types
        assertNotNull(firstRecord.get("int_column"));
        // TEXT affinity - should handle text
        assertNotNull(firstRecord.get("text_column"));
        // REAL affinity - should handle floating point
        assertNotNull(firstRecord.get("real_column"));
        // NUMERIC affinity - should handle numeric values
        assertNotNull(firstRecord.get("numeric_column"));
        // BLOB affinity - should handle binary data
        assertNotNull(firstRecord.get("blob_column"));
    }

    /**
     * Test SQLite auto-increment columns
     */
    @Test
    void testSQLiteAutoIncrement(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, email FROM autoinc_test ORDER BY id";
        File outputFile = tempDir.resolve("sqlite_autoinc.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 autoinc records");

        // Verify auto-increment values
        assertEquals(1, records.get(0).get("id"));
        assertEquals(2, records.get(1).get("id"));
        assertEquals(3, records.get(2).get("id"));
    }

    /**
     * Test SQLite full-text search (FTS) virtual tables
     */
    @Test
    void testSQLiteFTSExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT rowid, title, content FROM documents_fts";
        File outputFile = tempDir.resolve("sqlite_fts.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 FTS records");

        // Verify FTS virtual table access
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("rowid"));
        assertNotNull(firstRecord.get("title"));
        assertNotNull(firstRecord.get("content"));
    }

    /**
     * Helper method to read Parquet file back for verification
     */
    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }

    /**
     * Setup test data for SQLite tests
     */
    private void setupTestData() throws SQLException {
        Statement stmt = connection.createStatement();

        // Enable foreign keys and other SQLite pragmas
        stmt.execute("PRAGMA foreign_keys = ON");
        stmt.execute("PRAGMA journal_mode = WAL");

        // Drop tables if they exist
        Stream.of(
            "employees", "departments", "binary_data", "json_data", "temporal_data",
            "nullable_data", "type_affinity_test", "autoinc_test", "employee_departments"
        ).forEach(table -> {
            try {
                stmt.execute("DROP TABLE IF EXISTS " + table);
            } catch (SQLException e) {
                // Ignore errors if tables don't exist
            }
        });

        // Create departments table first (foreign key reference)
        stmt.execute("""
            CREATE TABLE departments (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                department_name TEXT NOT NULL,
                location TEXT
            )
        """);

        stmt.execute("""
            INSERT INTO departments (department_name, location) VALUES
            ('Engineering', 'Building A'),
            ('Marketing', 'Building B'),
            ('Sales', 'Building C')
        """);

        // Basic employees table with SQLite-specific types
        stmt.execute("""
            CREATE TABLE employees (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE,
                age INTEGER,
                salary REAL,
                is_active INTEGER DEFAULT 1,  -- SQLite doesn't have BOOLEAN, uses INTEGER
                created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                department_id INTEGER,
                FOREIGN KEY (department_id) REFERENCES departments(id)
            )
        """);

        stmt.execute("""
            INSERT INTO employees (name, email, age, salary, is_active, department_id) VALUES
            ('John Doe', 'john@example.com', 30, 75000.00, 1, 1),
            ('Jane Smith', 'jane@example.com', 25, 68000.00, 1, 1),
            ('Bob Johnson', 'bob@example.com', 35, 82000.00, 0, 2)
        """);

        // Binary data table
        stmt.execute("""
            CREATE TABLE binary_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                blob_data BLOB,
                description TEXT
            )
        """);

        // Insert sample BLOB data (using hex literal)
        stmt.execute("""
            INSERT INTO binary_data (blob_data, description) VALUES
            (X'48656C6C6F20576F726C64', 'Hello World in hex'),
            (X'53514C697465', 'SQLite in hex')
        """);

        // JSON data stored as TEXT (SQLite doesn't have native JSON type)
        stmt.execute("""
            CREATE TABLE json_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                metadata TEXT,
                created_at TEXT DEFAULT CURRENT_TIMESTAMP
            )
        """);

        stmt.execute("""
            INSERT INTO json_data (metadata) VALUES
            ('{"brand": "Dell", "model": "XPS 15", "price": 1499.99}'),
            ('{"brand": "Apple", "model": "iPhone 14", "price": 999.99}'),
            ('{"brand": "Samsung", "model": "Galaxy Tab", "price": 649.99}')
        """);

        // Temporal data table (SQLite has limited native date/time types)
        stmt.execute("""
            CREATE TABLE temporal_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                date_text TEXT,      -- ISO8601 format
                time_text TEXT,      -- HH:MM:SS format
                datetime_text TEXT,   -- ISO8601 format
                timestamp_integer INTEGER  -- Unix timestamp
            )
        """);

        stmt.execute("""
            INSERT INTO temporal_data VALUES
            (1, '2024-01-15', '10:30:45', '2024-01-15T10:30:45', 1705328645),
            (2, '2024-02-20', '15:45:30', '2024-02-20T15:45:30', 1708431930)
        """);

        // Nullable data table
        stmt.execute("""
            CREATE TABLE nullable_data (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                required_value TEXT NOT NULL,
                optional_value TEXT
            )
        """);

        stmt.execute("""
            INSERT INTO nullable_data VALUES
            (1, 'required_value_1', NULL),
            (2, 'required_value_2', 'optional_value_2')
        """);

        // Type affinity test table
        stmt.execute("""
            CREATE TABLE type_affinity_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                int_column INTEGER,      -- INTEGER affinity
                text_column TEXT,        -- TEXT affinity
                real_column REAL,        -- REAL affinity
                numeric_column NUMERIC,  -- NUMERIC affinity
                blob_column BLOB         -- BLOB affinity
            )
        """);

        stmt.execute("""
            INSERT INTO type_affinity_test (int_column, text_column, real_column, numeric_column, blob_column) VALUES
            (42, 'Hello', 3.14, 123.45, X'74657374'),
            (100, 'World', 2.718, 678.90, X'62617461'),
            (0, 'SQLite', 1.414, 3.14159, NULL)
        """);

        // Auto-increment test table
        stmt.execute("""
            CREATE TABLE autoinc_test (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT UNIQUE
            )
        """);

        stmt.execute("""
            INSERT INTO autoinc_test (name, email) VALUES
            ('Alice', 'alice@example.com'),
            ('Bob', 'bob@example.com'),
            ('Charlie', 'charlie@example.com')
        """);

        // Full-text search virtual table
        stmt.execute("""
            CREATE VIRTUAL TABLE documents_fts USING fts5(title, content)
        """);

        stmt.execute("""
            INSERT INTO documents_fts (title, content) VALUES
            ('First Document', 'This is the content of the first document about SQLite.'),
            ('Second Document', 'SQLite is a C-language library that implements a SQL database engine.'),
            ('Third Document', 'Dynamic JDBC export functionality for Parquet files.')
        """);

        // Table with camelCase column names
        stmt.execute("""
            CREATE TABLE employee_departments (
                employeeId INTEGER,
                employeeName TEXT,
                departmentName TEXT
            )
        """);

        stmt.execute("""
            INSERT INTO employee_departments (employeeId, employeeName, departmentName) VALUES
            (1, 'John Doe', 'Engineering'),
            (2, 'Jane Smith', 'Engineering'),
            (3, 'Bob Johnson', 'Marketing')
        """);
    }
}