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
 * Generic tests for DynamicJdbcExporter using DuckDB as the in-memory database.
 * This class contains general functionality tests that are not specific to any SQL dialect.
 *
 * For dialect-specific tests, see:
 * - DynamicJdbcExporterDuckDBTest
 * - DynamicJdbcExporterPostgreSQLTest
 * - DynamicJdbcExporterMySQLTest
 * - DynamicJdbcExporterSQLiteTest
 */
class DynamicJdbcExporterGenericTest {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        // Create DuckDB in-memory database connection for generic tests
        connection = DriverManager.getConnection("jdbc:duckdb:");
        createGenericTestTables();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    void testGenericBasicTypes(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT id, name, description, price, created_at FROM products";
        File outputFile = tempDir.resolve("products.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertEquals(3, totalRows, "Should export 3 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 product records");

        // Verify first record
        Map<String, Object> firstRecord = records.get(0);
        assertEquals(1L, firstRecord.get("id"));
        assertEquals("Product A", firstRecord.get("name"));
        assertEquals("Description of Product A", firstRecord.get("description"));
        assertEquals(new BigDecimal("19.99"), firstRecord.get("price"));
        assertNotNull(firstRecord.get("created_at"));
    }

    @Test
    void testGenericConfigurableExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT productId, productName, category FROM product_categories";
        File outputFile = tempDir.resolve("product_categories.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(2)  // Small batch size for testing
            .withCompressionCodec(CompressionCodecName.GZIP)
            .withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME)
            .withConvertCamelCase(false);

        // When
        long totalRows = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        // Then
        assertEquals(3, totalRows, "Should export 3 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 records");

        // Verify column naming (should preserve camelCase with our config)
        Map<String, Object> firstRecord = records.get(0);
        assertTrue(firstRecord.containsKey("productId"), "Should contain productId (not product_id)");
        assertTrue(firstRecord.containsKey("productName"), "Should contain productName (not product_name)");
    }

    @Test
    void testGenericEmptyResultSetExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM products WHERE id > 1000";
        File outputFile = tempDir.resolve("empty.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertEquals(0, totalRows, "Should export 0 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created even for empty results");

        // Verify we can read it back (should be empty)
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(0, records.size(), "Should have 0 records");
    }

    @Test
    void testGenericAggregationQueryExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT category, COUNT(*) as product_count, AVG(price) as avg_price FROM products GROUP BY category";
        File outputFile = tempDir.resolve("category_summary.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertEquals(2, totalRows, "Should export 2 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(2, records.size(), "Should have 2 categories");

        // Verify aggregation results
        Map<String, Object> electronicsRecord = records.stream()
            .filter(r -> "Electronics".equals(r.get("category")))
            .findFirst()
            .orElseThrow();

        assertEquals(2L, electronicsRecord.get("product_count"));
        assertEquals(159.99, electronicsRecord.get("avg_price"));
    }

    @Test
    void testGenericSchemaAnalysis() throws SQLException {
        // Given
        String sql = "SELECT * FROM products LIMIT 1";
        try (ResultSet resultSet = connection.createStatement().executeQuery(sql)) {
            ResultSetMetaData metaData = resultSet.getMetaData();

            // When
            List<DynamicJdbcExporter.ColumnInfo> columns = DynamicJdbcExporter.analyzeResultSet(metaData);

            // Then
            assertEquals(7, columns.size(), "Should have 7 columns");

            DynamicJdbcExporter.ColumnInfo idColumn = columns.get(0);
            assertEquals("id", idColumn.label());
            assertEquals("id", idColumn.name());
            assertEquals(java.sql.Types.BIGINT, idColumn.type());
            assertEquals("BIGINT", idColumn.typeName());

            DynamicJdbcExporter.ColumnInfo nameColumn = columns.get(1);
            assertEquals("name", nameColumn.label());
            assertEquals(java.sql.Types.VARCHAR, nameColumn.type());
            assertTrue(nameColumn.nullable(), "Name should be nullable");
        }
    }

    @Test
    void testGenericLargeDatasetPerformance(@TempDir Path tempDir) throws SQLException, IOException {
        // Given - create a larger dataset
        Statement stmt = connection.createStatement();
        stmt.execute("CREATE TABLE large_products AS SELECT * FROM products CROSS JOIN generate_series(1, 500)");
        String sql = "SELECT * FROM large_products";
        File outputFile = tempDir.resolve("large_products.parquet").toFile();

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withFetchSize(100);

        // When
        long totalRows = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        // Then
        assertEquals(1500, totalRows, "Should export 1500 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(1500, records.size(), "Should have 1500 records (3 * 500)");

        // File size should be reasonable
        assertTrue(outputFile.length() > 0, "File should have content");
    }

    @Test
    void testGenericNullValueHandling(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT * FROM nullable_products ORDER BY id";
        File outputFile = tempDir.resolve("nullable_products.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertEquals(3, totalRows, "Should export 3 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 records");

        // Verify null values are handled properly
        Map<String, Object> firstRecord = records.get(0);
        assertNull(firstRecord.get("optional_description"), "First record should have null optional_description");
        assertNotNull(firstRecord.get("required_name"), "First record should have non-null required_name");

        Map<String, Object> secondRecord = records.get(1);
        assertNotNull(secondRecord.get("optional_description"), "Second record should have non-null optional_description");
        assertNotNull(secondRecord.get("required_name"), "Second record should have non-null required_name");
    }

    @Test
    void testGenericJoinQueryExport(@TempDir Path tempDir) throws SQLException, IOException {
        // Given
        String sql = "SELECT p.id, p.name, p.price, c.category_name FROM products p JOIN categories c ON p.category_id = c.id";
        File outputFile = tempDir.resolve("products_with_categories.parquet").toFile();

        // When
        long totalRows = DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);

        // Then
        assertEquals(3, totalRows, "Should export 3 rows");
        assertTrue(outputFile.exists(), "Parquet file should be created");

        // Verify we can read it back
        List<Map<String, Object>> records = readParquetFile(outputFile);
        assertEquals(3, records.size(), "Should have 3 product records");

        // Verify join results
        Map<String, Object> firstRecord = records.get(0);
        assertEquals(1L, firstRecord.get("id"));
        assertEquals("Product A", firstRecord.get("name"));
        assertEquals(new BigDecimal("19.99"), firstRecord.get("price"));
        assertNotNull(firstRecord.get("category_name"));
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
     * Create generic test tables with common data types
     */
    private void createGenericTestTables() throws SQLException {
        Statement stmt = connection.createStatement();

        // Basic products table
        stmt.execute("""
            CREATE TABLE products (
                id BIGINT PRIMARY KEY,
                name VARCHAR(100),
                description VARCHAR(500),
                price DECIMAL(10, 2),
                category VARCHAR(50),
                category_id BIGINT,
                created_at TIMESTAMP
            )
        """);

        stmt.execute("""
            INSERT INTO products VALUES
            (1, 'Product A', 'Description of Product A', 19.99, 'Electronics', 1, '2024-01-01 10:00:00'),
            (2, 'Product B', 'Description of Product B', 299.99, 'Electronics', 1, '2024-01-02 11:00:00'),
            (3, 'Product C', 'Description of Product C', 49.99, 'Books', 2, '2024-01-03 12:00:00')
        """);

        // Categories table
        stmt.execute("""
            CREATE TABLE categories (
                id BIGINT PRIMARY KEY,
                category_name VARCHAR(50)
            )
        """);

        stmt.execute("""
            INSERT INTO categories VALUES
            (1, 'Electronics'),
            (2, 'Books')
        """);

        // Table with camelCase column names
        stmt.execute("""
            CREATE TABLE product_categories (
                productId BIGINT,
                productName VARCHAR(100),
                category VARCHAR(50)
            )
        """);

        stmt.execute("""
            INSERT INTO product_categories VALUES
            (1, 'Product A', 'Electronics'),
            (2, 'Product B', 'Electronics'),
            (3, 'Product C', 'Books')
        """);

        // Nullable data table
        stmt.execute("""
            CREATE TABLE nullable_products (
                id BIGINT PRIMARY KEY,
                required_name VARCHAR(100) NOT NULL,
                optional_description VARCHAR(500),
                price DECIMAL(10, 2)
            )
        """);

        stmt.execute("""
            INSERT INTO nullable_products VALUES
            (1, 'Product 1', NULL, 19.99),
            (2, 'Product 2', 'Optional description', 29.99),
            (3, 'Product 3', NULL, 39.99)
        """);
    }
}