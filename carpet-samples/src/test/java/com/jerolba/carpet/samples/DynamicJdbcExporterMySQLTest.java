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

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.io.TempDir;

import java.sql.*;
import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.*;
import java.util.stream.Stream;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.ColumnNamingStrategy;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.testcontainers.containers.MySQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerImageName;

/**
 * Unit tests for MySQL dynamic JDBC export functionality.
 *
 * These tests demonstrate and verify MySQL-specific features including:
 * - JSON data type support
 * - ENUM data type support
 * - SET data type support
 * - TINYINT, MEDIUMINT, BIGINT types
 * - TIMESTAMP and DATETIME types
 * - YEAR data type
 * - BLOB and TEXT types
 * - AUTO_INCREMENT columns
 * - Character set and collation handling
 */
@Testcontainers
class DynamicJdbcExporterMySQLTest {

    @Container
    private static final MySQLContainer<?> mysql = new MySQLContainer<>(
        DockerImageName.parse("mysql:8.0"))
        .withDatabaseName("testdb")
        .withUsername("test")
        .withPassword("test")
        .withLogConsumer(outputFrame -> {
            System.out.print("🐳 MySQL: " + outputFrame.getUtf8String());
        });

    private Connection connection;

    @BeforeAll
    static void beforeAll() {
        System.out.println("🐳 Starting MySQL container...");
        System.out.println("📦 Docker image: " + mysql.getDockerImageName());
        System.out.println("🏷️  Container name: is" + mysql.getContainerName());
        System.out.println("⏳ Waiting for container to be ready...");
    }

    @BeforeEach
    void setUp() throws SQLException {
        System.out.println("🚀 Setting up MySQL test...");
        System.out.println("📍 MySQL container status: " + (mysql.isRunning() ? "RUNNING" : "STOPPED"));
        System.out.println("🔗 JDBC URL: " + mysql.getJdbcUrl());
        System.out.println("👤 Username: " + mysql.getUsername());

        connection = DriverManager.getConnection(
            mysql.getJdbcUrl(),
            mysql.getUsername(),
            mysql.getPassword()
        );
        System.out.println("✅ Database connection established");
        setupTestData();
        System.out.println("✅ Test data setup completed");
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Test basic MySQL export functionality with various data types
     */
    @Test
    void testMySQLBasicExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, email, age, salary, is_active, created_at FROM employees";
        File outputFile = tempDir.resolve("mysql_employees.parquet").toFile();

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
        // MySQL TINYINT comes as Byte, but value is the same
        assertEquals(30, ((Number) firstRecord.get("age")).intValue());
        assertEquals(new BigDecimal("75000.00"), firstRecord.get("salary"));
        assertEquals(true, firstRecord.get("is_active"));
        assertNotNull(firstRecord.get("created_at"));
    }

    /**
     * Test MySQL JSON data type support
     */
    @Test
    void testMySQLJsonExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, product_name, metadata FROM products";
        File outputFile = tempDir.resolve("mysql_products.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 product records");

        // Verify JSON field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("metadata"));
        // JSON should be converted to String representation
        assertTrue(firstRecord.get("metadata") instanceof String);
    }

    /**
     * Test MySQL ENUM data type support
     */
    @Test
    void testMySQLEnumExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, task_name, status, priority FROM tasks";
        File outputFile = tempDir.resolve("mysql_tasks.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 task records");

        // Verify ENUM field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("status"));
        assertNotNull(firstRecord.get("priority"));
        // ENUM values should be converted to String
        assertTrue(firstRecord.get("status") instanceof String);
        assertTrue(firstRecord.get("priority") instanceof String);
    }

    /**
     * Test MySQL SET data type support
     */
    @Test
    void testMySQLSetExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, username, permissions FROM users";
        File outputFile = tempDir.resolve("mysql_users.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 user records");

        // Verify SET field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("permissions"));
        // SET values should be converted to String
        assertTrue(firstRecord.get("permissions") instanceof String);
    }

    /**
     * Test MySQL temporal data types (TIMESTAMP, DATETIME, DATE, TIME, YEAR)
     */
    @Test
    void testMySQLTemporalTypesExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM temporal_data";
        File outputFile = tempDir.resolve("mysql_temporal.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 temporal records");

        // Verify temporal field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("timestamp_col"));
        assertNotNull(firstRecord.get("datetime_col"));
        assertNotNull(firstRecord.get("date_col"));
        assertNotNull(firstRecord.get("time_col"));
        assertNotNull(firstRecord.get("year_col"));
    }

    /**
     * Test MySQL BLOB and TEXT data types
     */
    @Test
    void testMySQLBlobTextExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM binary_data";
        File outputFile = tempDir.resolve("mysql_binary.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 binary records");

        // Verify BLOB/TEXT field handling
        Map<String, Object> firstRecord = records.get(0);
        assertNotNull(firstRecord.get("blob_data"));
        assertNotNull(firstRecord.get("text_data"));
        assertNotNull(firstRecord.get("longtext_data"));
    }

    /**
     * Test MySQL schema analysis functionality
     */
    @Test
    void testMySQLSchemaAnalysis() throws SQLException {
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
            assertFalse(idColumn.nullable(), "Primary key should not be nullable");

            DynamicJdbcExporter.ColumnInfo nameColumn = columns.get(1);
            assertEquals("name", nameColumn.label());
            assertEquals(java.sql.Types.VARCHAR, nameColumn.type());
            assertFalse(nameColumn.nullable(), "Name should NOT be nullable (defined as NOT NULL)");

            DynamicJdbcExporter.ColumnInfo salaryColumn = columns.get(4);
            assertEquals("salary", salaryColumn.label());
            assertEquals(java.sql.Types.DECIMAL, salaryColumn.type());
            assertTrue(salaryColumn.typeName().toLowerCase().contains("decimal"),
                      "Should be decimal type: " + salaryColumn.typeName());
        }
    }

    /**
     * Test MySQL configurable export with custom settings
     */
    @Test
    void testMySQLConfigurableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT employeeId, employeeName, departmentName FROM employee_departments";
        File outputFile = tempDir.resolve("mysql_employee_departments.parquet").toFile();

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
     * Test NULL value handling in MySQL
     */
    @Test
    void testMySQLNullHandling(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM nullable_data";
        File outputFile = tempDir.resolve("mysql_nullable.parquet").toFile();

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
     * Test MySQL character set and collation
     */
    @Test
    void testMySQLCharacterSet(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM charset_test";
        File outputFile = tempDir.resolve("mysql_charset.parquet").toFile();

        // When
        DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertTrue(outputFile.exists(), "Parquet file should be created");

        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 charset records");

        // Verify character data is preserved correctly
        Map<String, Object> firstRecord = records.get(0);
        assertEquals("Hello World", firstRecord.get("utf8_text"));
        assertEquals("Café", firstRecord.get("utf8mb4_text"));
        assertEquals("测试", firstRecord.get("chinese_text"));
    }

    /**
     * Helper method to read Parquet file back for verification
     */
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map> reader = new CarpetReader<>(file, Map.class);
        @SuppressWarnings("unchecked")
        List<Map<String, Object>> records = (List<Map<String, Object>>) (List<?>) reader.toList();
        return records;
    }

    /**
     * Setup test data for MySQL tests
     */
    private void setupTestData() throws SQLException {
        Statement stmt = connection.createStatement();

        // Drop tables if they exist
        Stream.of(
            "employees", "products", "tasks", "users", "temporal_data",
            "binary_data", "nullable_data", "charset_test", "employee_departments"
        ).forEach(table -> {
            try {
                stmt.execute("DROP TABLE IF EXISTS " + table);
            } catch (SQLException e) {
                // Ignore errors if tables don't exist
            }
        });

        // Basic employees table with MySQL-specific types
        stmt.execute("""
            CREATE TABLE employees (
                id INT PRIMARY KEY AUTO_INCREMENT,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255) UNIQUE,
                age TINYINT UNSIGNED,
                salary DECIMAL(10,2) DEFAULT 0.00,
                is_active BOOLEAN DEFAULT TRUE,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO employees (name, email, age, salary, is_active) VALUES
            ('John Doe', 'john@example.com', 30, 75000.00, TRUE),
            ('Jane Smith', 'jane@example.com', 25, 68000.00, TRUE),
            ('Bob Johnson', 'bob@example.com', 35, 82000.00, FALSE)
        """);

        // Products table with JSON support
        stmt.execute("""
            CREATE TABLE products (
                id INT PRIMARY KEY AUTO_INCREMENT,
                product_name VARCHAR(200) NOT NULL,
                metadata JSON,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO products (product_name, metadata) VALUES
            ('Laptop', '{"brand": "Dell", "model": "XPS 15", "price": 1499.99, "specs": {"cpu": "i7", "ram": 16}}'),
            ('Phone', '{"brand": "Apple", "model": "iPhone 14", "price": 999.99, "color": "blue"}'),
            ('Tablet', '{"brand": "Samsung", "model": "Galaxy Tab", "price": 649.99, "storage": "128GB"}')
        """);

        // Tasks table with ENUM support
        stmt.execute("""
            CREATE TABLE tasks (
                id INT PRIMARY KEY AUTO_INCREMENT,
                task_name VARCHAR(200) NOT NULL,
                status ENUM('pending', 'in_progress', 'completed', 'cancelled') DEFAULT 'pending',
                priority ENUM('low', 'medium', 'high', 'urgent') DEFAULT 'medium',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO tasks (task_name, status, priority) VALUES
            ('Fix login bug', 'in_progress', 'high'),
            ('Update documentation', 'pending', 'medium'),
            ('Setup CI/CD', 'completed', 'low')
        """);

        // Users table with SET support
        stmt.execute("""
            CREATE TABLE users (
                id INT PRIMARY KEY AUTO_INCREMENT,
                username VARCHAR(50) UNIQUE NOT NULL,
                permissions SET('read', 'write', 'delete', 'admin') DEFAULT 'read',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO users (username, permissions) VALUES
            ('user1', 'read'),
            ('user2', 'read,write'),
            ('admin', 'read,write,delete,admin')
        """);

        // Temporal data types table
        stmt.execute("""
            CREATE TABLE temporal_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                timestamp_col TIMESTAMP,
                datetime_col DATETIME,
                date_col DATE,
                time_col TIME,
                year_col YEAR
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO temporal_data VALUES
            (1, '2024-01-15 10:30:45', '2024-01-15 10:30:45', '2024-01-15', '10:30:45', 2024),
            (2, '2024-02-20 15:45:30', '2024-02-20 15:45:30', '2024-02-20', '15:45:30', 2023)
        """);

        // BLOB and TEXT data types table
        stmt.execute("""
            CREATE TABLE binary_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                blob_data BLOB,
                text_data TEXT,
                longtext_data LONGTEXT
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO binary_data (blob_data, text_data, longtext_data) VALUES
            ('binary data here', 'Regular text content', 'This is a very long text content that demonstrates the LONGTEXT data type capability in MySQL databases.'),
            ('more binary', 'Short text', 'Another long text entry with UTF-8 support: café, naïve, résumé, 测试, 中文, 日本語')
        """);

        // Nullable data table
        stmt.execute("""
            CREATE TABLE nullable_data (
                id INT PRIMARY KEY AUTO_INCREMENT,
                required_value VARCHAR(100) NOT NULL,
                optional_value VARCHAR(100)
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO nullable_data VALUES
            (1, 'required_value_1', NULL),
            (2, 'required_value_2', 'optional_value_2')
        """);

        // Character set test table
        stmt.execute("""
            CREATE TABLE charset_test (
                id INT PRIMARY KEY AUTO_INCREMENT,
                utf8_text VARCHAR(100) CHARACTER SET utf8,
                utf8mb4_text VARCHAR(100) CHARACTER SET utf8mb4,
                chinese_text VARCHAR(100) CHARACTER SET utf8mb4
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO charset_test (utf8_text, utf8mb4_text, chinese_text) VALUES
            ('Hello World', 'Café', '测试'),
            ('ASCII only', 'Emoji: 😊', '中文支持')
        """);

        // Table with camelCase column names (MySQL preserves case in backticks)
        stmt.execute("""
            CREATE TABLE employee_departments (
                `employeeId` INT,
                `employeeName` VARCHAR(100),
                `departmentName` VARCHAR(50)
            )
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        """);

        stmt.execute("""
            INSERT INTO employee_departments (`employeeId`, `employeeName`, `departmentName`) VALUES
            (1, 'John Doe', 'Engineering'),
            (2, 'Jane Smith', 'Engineering'),
            (3, 'Bob Johnson', 'Marketing')
        """);
    }
}