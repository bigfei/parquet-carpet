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

    @Test
    void testIndividualOptimizationImpact(@TempDir Path tempDir) throws SQLException, IOException {
        System.out.println("\n=== Individual Optimization Impact Analysis (1,000,000 rows) ===\n");

        String sql = "SELECT * FROM performance_test";

        // Comprehensive warmup phase
        System.out.println("🔥 Warming up PostgreSQL database...");
        System.out.println("   This ensures shared_buffers, OS cache, and query plans are hot");

        // Run ANALYZE to update statistics
        try (var stmt = connection.createStatement()) {
            stmt.execute("ANALYZE performance_test");
            System.out.println("   ✓ ANALYZE completed - statistics updated");
        }

        // Warmup runs - execute query multiple times to warm up all caches
        DynamicExportConfig warmupConfig = new DynamicExportConfig();
        warmupConfig.setBatchSize(10000);
        warmupConfig.setFetchSize(10000);

        System.out.println("   Running warmup iterations...");
        for (int i = 1; i <= 3; i++) {
            File warmupFile = tempDir.resolve("warmup_" + i + ".parquet").toFile();
            long warmupStart = System.currentTimeMillis();
            DynamicJdbcExporter.exportWithConfig(connection, sql, warmupFile, warmupConfig);
            long warmupDuration = System.currentTimeMillis() - warmupStart;
            System.out.printf("   Iteration %d: %,d ms (%,d rows/sec)%n",
                i, warmupDuration, (1000000L * 1000) / warmupDuration);
        }

        System.out.println("   ✓ Database fully warmed up - starting actual performance tests\n");

        // Small delay to let things settle
        try {
            Thread.sleep(1000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Baseline: Small batch + small fetch WITHOUT metadata caching (simulates old behavior)
        System.out.println("Test 1: Batch=1000, Fetch=1000, NO Metadata Caching (OLD BASELINE)");
        File outputFile0 = tempDir.resolve("perf_baseline_no_cache.parquet").toFile();
        DynamicExportConfig config0 = new DynamicExportConfig();
        config0.setBatchSize(1000);
        config0.setFetchSize(1000);
        config0.setUseMetadataCaching(false);  // Disable optimization

        long startTime0 = System.currentTimeMillis();
        long rows0 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile0, config0);
        long duration0 = System.currentTimeMillis() - startTime0;
        double rowsPerSec0 = (rows0 * 1000.0) / duration0;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration0, duration0 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec0);
        System.out.printf("  File size: %.2f MB%n%n", outputFile0.length() / (1024.0 * 1024.0));

        // Test metadata caching alone with same batch/fetch sizes
        System.out.println("Test 2: Batch=1000, Fetch=1000, WITH Metadata Caching (METADATA ONLY)");
        File outputFile1 = tempDir.resolve("perf_with_cache.parquet").toFile();
        DynamicExportConfig config1 = new DynamicExportConfig();
        config1.setBatchSize(1000);
        config1.setFetchSize(1000);
        config1.setUseMetadataCaching(true);  // Enable optimization

        long startTime1 = System.currentTimeMillis();
        long rows1 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile1, config1);
        long duration1 = System.currentTimeMillis() - startTime1;
        double rowsPerSec1 = (rows1 * 1000.0) / duration1;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration1, duration1 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec1);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec1 / rowsPerSec0,
            (long)(((rowsPerSec1 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile1.length() / (1024.0 * 1024.0));

        // Test batch size ONLY (no metadata caching)
        System.out.println("Test 3: Batch=10000, Fetch=1000, NO Metadata Caching (BATCH ONLY)");
        File outputFile2 = tempDir.resolve("perf_batch_no_cache.parquet").toFile();
        DynamicExportConfig config2 = new DynamicExportConfig();
        config2.setBatchSize(10000);
        config2.setFetchSize(1000);
        config2.setUseMetadataCaching(false);  // Test batch alone

        long startTime2 = System.currentTimeMillis();
        long rows2 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile2, config2);
        long duration2 = System.currentTimeMillis() - startTime2;
        double rowsPerSec2 = (rows2 * 1000.0) / duration2;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration2, duration2 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec2);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec2 / rowsPerSec0,
            (long)(((rowsPerSec2 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile2.length() / (1024.0 * 1024.0));

        // Test fetch size ONLY (no metadata caching)
        System.out.println("Test 4: Batch=1000, Fetch=10000, NO Metadata Caching (FETCH ONLY)");
        File outputFile3 = tempDir.resolve("perf_fetch_no_cache.parquet").toFile();
        DynamicExportConfig config3 = new DynamicExportConfig();
        config3.setBatchSize(1000);
        config3.setFetchSize(10000);
        config3.setUseMetadataCaching(false);  // Test fetch alone

        long startTime3 = System.currentTimeMillis();
        long rows3 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile3, config3);
        long duration3 = System.currentTimeMillis() - startTime3;
        double rowsPerSec3 = (rows3 * 1000.0) / duration3;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration3, duration3 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec3);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec3 / rowsPerSec0,
            (long)(((rowsPerSec3 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile3.length() / (1024.0 * 1024.0));

        // Test batch + fetch WITHOUT metadata caching
        System.out.println("Test 5: Batch=10000, Fetch=10000, NO Metadata Caching (BATCH+FETCH, NO CACHE)");
        File outputFile4 = tempDir.resolve("perf_batch_fetch_no_cache.parquet").toFile();
        DynamicExportConfig config4 = new DynamicExportConfig();
        config4.setBatchSize(10000);
        config4.setFetchSize(10000);
        config4.setUseMetadataCaching(false);  // Test batch+fetch without cache

        long startTime4 = System.currentTimeMillis();
        long rows4 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile4, config4);
        long duration4 = System.currentTimeMillis() - startTime4;
        double rowsPerSec4 = (rows4 * 1000.0) / duration4;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration4, duration4 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec4);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec4 / rowsPerSec0,
            (long)(((rowsPerSec4 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile4.length() / (1024.0 * 1024.0));

        // Test batch + metadata caching
        System.out.println("Test 6: Batch=10000, Fetch=1000, WITH Metadata Caching (BATCH + METADATA)");
        File outputFile5 = tempDir.resolve("perf_batch_with_cache.parquet").toFile();
        DynamicExportConfig config5 = new DynamicExportConfig();
        config5.setBatchSize(10000);
        config5.setFetchSize(1000);
        config5.setUseMetadataCaching(true);

        long startTime5 = System.currentTimeMillis();
        long rows5 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile5, config5);
        long duration5 = System.currentTimeMillis() - startTime5;
        double rowsPerSec5 = (rows5 * 1000.0) / duration5;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration5, duration5 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec5);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec5 / rowsPerSec0,
            (long)(((rowsPerSec5 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile5.length() / (1024.0 * 1024.0));

        // Test fetch + metadata caching
        System.out.println("Test 7: Batch=1000, Fetch=10000, WITH Metadata Caching (FETCH + METADATA)");
        File outputFile6 = tempDir.resolve("perf_fetch_with_cache.parquet").toFile();
        DynamicExportConfig config6 = new DynamicExportConfig();
        config6.setBatchSize(1000);
        config6.setFetchSize(10000);
        config6.setUseMetadataCaching(true);

        long startTime6 = System.currentTimeMillis();
        long rows6 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile6, config6);
        long duration6 = System.currentTimeMillis() - startTime6;
        double rowsPerSec6 = (rows6 * 1000.0) / duration6;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration6, duration6 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec6);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec6 / rowsPerSec0,
            (long)(((rowsPerSec6 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile6.length() / (1024.0 * 1024.0));

        // Test ALL optimizations together
        System.out.println("Test 8: Batch=10000, Fetch=10000, WITH Metadata Caching (ALL OPTIMIZATIONS)");
        File outputFile7 = tempDir.resolve("perf_all_optimizations.parquet").toFile();
        DynamicExportConfig config7 = new DynamicExportConfig();
        config7.setBatchSize(10000);
        config7.setFetchSize(10000);
        config7.setUseMetadataCaching(true);  // All optimizations enabled

        long startTime7 = System.currentTimeMillis();
        long rows7 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile7, config7);
        long duration7 = System.currentTimeMillis() - startTime7;
        double rowsPerSec7 = (rows7 * 1000.0) / duration7;

        System.out.printf("  Duration: %,d ms (%.1f sec)%n", duration7, duration7 / 1000.0);
        System.out.printf("  Throughput: %,d rows/sec%n", (long)rowsPerSec7);
        System.out.printf("  Improvement: %.2fx faster (+%d%%)%n", rowsPerSec7 / rowsPerSec0,
            (long)(((rowsPerSec7 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("  File size: %.2f MB%n%n", outputFile7.length() / (1024.0 * 1024.0));

        // Summary
        System.out.println("=====================================================================");
        System.out.println("=== COMPREHENSIVE OPTIMIZATION IMPACT BREAKDOWN ===");
        System.out.println("=====================================================================");
        System.out.printf("Test 1 - Baseline (no cache, 1k/1k):        %,10d rows/sec  [1.00x]%n", (long)rowsPerSec0);
        System.out.printf("Test 2 - Metadata ONLY (cache, 1k/1k):      %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec1, rowsPerSec1 / rowsPerSec0,
            (long)(((rowsPerSec1 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 3 - Batch ONLY (no cache, 10k/1k):     %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec2, rowsPerSec2 / rowsPerSec0,
            (long)(((rowsPerSec2 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 4 - Fetch ONLY (no cache, 1k/10k):     %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec3, rowsPerSec3 / rowsPerSec0,
            (long)(((rowsPerSec3 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 5 - Batch+Fetch (no cache, 10k/10k):   %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec4, rowsPerSec4 / rowsPerSec0,
            (long)(((rowsPerSec4 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 6 - Metadata+Batch (cache, 10k/1k):    %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec5, rowsPerSec5 / rowsPerSec0,
            (long)(((rowsPerSec5 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 7 - Metadata+Fetch (cache, 1k/10k):    %,10d rows/sec  [%.2fx, +%d%%]%n",
            (long)rowsPerSec6, rowsPerSec6 / rowsPerSec0,
            (long)(((rowsPerSec6 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.printf("Test 8 - ALL (cache, 10k/10k):              %,10d rows/sec  [%.2fx, +%d%%] ⭐%n",
            (long)rowsPerSec7, rowsPerSec7 / rowsPerSec0,
            (long)(((rowsPerSec7 - rowsPerSec0) / rowsPerSec0) * 100));
        System.out.println("=====================================================================");

        // Calculate individual contributions
        System.out.println("\n=== ISOLATED CONTRIBUTION ANALYSIS ===");
        double metadataAlone = rowsPerSec1 - rowsPerSec0;
        double batchAlone = rowsPerSec2 - rowsPerSec0;
        double fetchAlone = rowsPerSec3 - rowsPerSec0;
        double batchFetchNoCache = rowsPerSec4 - rowsPerSec0;
        double metadataBatch = rowsPerSec5 - rowsPerSec0;
        double metadataFetch = rowsPerSec6 - rowsPerSec0;
        double allThree = rowsPerSec7 - rowsPerSec0;

        System.out.printf("Metadata Caching alone:          +%,10d rows/sec (+%.0f%%)%n",
            (long)metadataAlone, (metadataAlone / rowsPerSec0) * 100);
        System.out.printf("Batch Size alone:                +%,10d rows/sec (+%.0f%%)%n",
            (long)batchAlone, (batchAlone / rowsPerSec0) * 100);
        System.out.printf("Fetch Size alone:                +%,10d rows/sec (+%.0f%%)%n",
            (long)fetchAlone, (fetchAlone / rowsPerSec0) * 100);
        System.out.printf("Batch+Fetch (no cache):          +%,10d rows/sec (+%.0f%%)%n",
            (long)batchFetchNoCache, (batchFetchNoCache / rowsPerSec0) * 100);
        System.out.printf("Metadata+Batch:                  +%,10d rows/sec (+%.0f%%)%n",
            (long)metadataBatch, (metadataBatch / rowsPerSec0) * 100);
        System.out.printf("Metadata+Fetch:                  +%,10d rows/sec (+%.0f%%)%n",
            (long)metadataFetch, (metadataFetch / rowsPerSec0) * 100);
        System.out.printf("ALL THREE:                       +%,10d rows/sec (+%.0f%%) ⭐%n",
            (long)allThree, (allThree / rowsPerSec0) * 100);
        System.out.println();

        // Verify correctness
        assertEquals(1000000, rows0, "Test 1 - Baseline should export 1M rows");
        assertEquals(1000000, rows1, "Test 2 - Metadata only should export 1M rows");
        assertEquals(1000000, rows2, "Test 3 - Batch only should export 1M rows");
        assertEquals(1000000, rows3, "Test 4 - Fetch only should export 1M rows");
        assertEquals(1000000, rows4, "Test 5 - Batch+Fetch should export 1M rows");
        assertEquals(1000000, rows5, "Test 6 - Metadata+Batch should export 1M rows");
        assertEquals(1000000, rows6, "Test 7 - Metadata+Fetch should export 1M rows");
        assertEquals(1000000, rows7, "Test 8 - All optimizations should export 1M rows");
    }

    @Test
    void testPerformanceComparison(@TempDir Path tempDir) throws SQLException, IOException {
        System.out.println("\n=== Performance Comparison Test (1,000,000 rows) ===\n");

        String sql = "SELECT * FROM performance_test";

        // Test 1: Small batch size (old default = 1000)
        System.out.println("Test 1: Batch Size 1,000 (OLD default)");
        File outputFile1 = tempDir.resolve("perf_batch_1000.parquet").toFile();
        DynamicExportConfig config1 = new DynamicExportConfig();
        config1.setBatchSize(1000);
        config1.setFetchSize(1000);

        long startTime1 = System.currentTimeMillis();
        long rows1 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile1, config1);
        long duration1 = System.currentTimeMillis() - startTime1;
        double rowsPerSec1 = (rows1 * 1000.0) / duration1;

        System.out.printf("  Duration: %d ms%n", duration1);
        System.out.printf("  Throughput: %.0f rows/sec%n", rowsPerSec1);
        System.out.printf("  File size: %.2f MB%n%n", outputFile1.length() / (1024.0 * 1024.0));

        // Test 2: Large batch size (new default = 10000)
        System.out.println("Test 2: Batch Size 10,000 (NEW default)");
        File outputFile2 = tempDir.resolve("perf_batch_10000.parquet").toFile();
        DynamicExportConfig config2 = new DynamicExportConfig();
        config2.setBatchSize(10000);
        config2.setFetchSize(10000);

        long startTime2 = System.currentTimeMillis();
        long rows2 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile2, config2);
        long duration2 = System.currentTimeMillis() - startTime2;
        double rowsPerSec2 = (rows2 * 1000.0) / duration2;

        System.out.printf("  Duration: %d ms%n", duration2);
        System.out.printf("  Throughput: %.0f rows/sec%n", rowsPerSec2);
        System.out.printf("  File size: %.2f MB%n%n", outputFile2.length() / (1024.0 * 1024.0));

        // Test 3: Very large batch size (50000)
        System.out.println("Test 3: Batch Size 50,000 (AGGRESSIVE)");
        File outputFile3 = tempDir.resolve("perf_batch_50000.parquet").toFile();
        DynamicExportConfig config3 = new DynamicExportConfig();
        config3.setBatchSize(50000);
        config3.setFetchSize(50000);

        long startTime3 = System.currentTimeMillis();
        long rows3 = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile3, config3);
        long duration3 = System.currentTimeMillis() - startTime3;
        double rowsPerSec3 = (rows3 * 1000.0) / duration3;

        System.out.printf("  Duration: %d ms%n", duration3);
        System.out.printf("  Throughput: %.0f rows/sec%n", rowsPerSec3);
        System.out.printf("  File size: %.2f MB%n%n", outputFile3.length() / (1024.0 * 1024.0));

        // Summary
        System.out.println("=== Performance Summary ===");
        System.out.printf("Batch 1000:  %.0f rows/sec (baseline)%n", rowsPerSec1);
        System.out.printf("Batch 10000: %.0f rows/sec (%.1fx faster)%n",
            rowsPerSec2, rowsPerSec2 / rowsPerSec1);
        System.out.printf("Batch 50000: %.0f rows/sec (%.1fx faster)%n",
            rowsPerSec3, rowsPerSec3 / rowsPerSec1);
        System.out.println();

        // Assertions
        assertEquals(1000000, rows1, "Should export 1,000,000 rows");
        assertEquals(1000000, rows2, "Should export 1,000,000 rows");
        assertEquals(1000000, rows3, "Should export 1,000,000 rows");
        assertTrue(outputFile1.exists() && outputFile2.exists() && outputFile3.exists(),
            "All output files should exist");

        // Performance assertion: batch 10000 should be faster or comparable
        // Note: This is relaxed to avoid flakiness due to system load variations
        // The main goal is to verify the feature works, not enforce strict performance
        assertTrue(rowsPerSec2 > rowsPerSec1 * 0.7,
            String.format("Batch 10000 should not be significantly slower (actual: %.1fx)",
                rowsPerSec2 / rowsPerSec1));
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
            stmt.execute("DROP TABLE IF EXISTS performance_test CASCADE");
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

        // Performance test table with larger dataset
        stmt.execute("""
            CREATE TABLE performance_test (
                id BIGSERIAL PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(255),
                age INTEGER,
                salary DECIMAL(12, 2),
                department VARCHAR(50),
                hire_date DATE,
                is_active BOOLEAN,
                created_at TIMESTAMP
            )
        """);

        // Insert 1,000,000 rows for performance testing
        stmt.execute("""
            INSERT INTO performance_test
            SELECT
                generate_series(1, 1000000) as id,
                'Employee ' || generate_series(1, 1000000) as name,
                'employee' || generate_series(1, 1000000) || '@company.com' as email,
                20 + (random() * 45)::int as age,
                40000 + (random() * 100000)::decimal(12, 2) as salary,
                CASE (random() * 4)::int
                    WHEN 0 THEN 'Engineering'
                    WHEN 1 THEN 'Sales'
                    WHEN 2 THEN 'Marketing'
                    ELSE 'Support'
                END as department,
                CURRENT_DATE - (random() * 3650)::int as hire_date,
                random() > 0.1 as is_active,
                CURRENT_TIMESTAMP - (random() * 365)::int * INTERVAL '1 day' as created_at
        """);
    }
}