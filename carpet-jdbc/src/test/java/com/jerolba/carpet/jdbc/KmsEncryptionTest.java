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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.jerolba.carpet.CarpetReader;

/**
 * Tests for AWS KMS envelope encryption functionality.
 *
 * These tests require:
 * 1. AWS credentials configured (environment variables, credentials file, or IAM role)
 * 2. KMS key ID specified in test.properties file
 * 3. KMS key with GenerateDataKey and Decrypt permissions
 *
 * Setup:
 * 1. Create test.properties in src/test/resources/:
 *    aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
 *    aws.kms.region=us-east-1
 *
 * 2. Configure AWS credentials (one of):
 *    - Environment: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 *    - File: ~/.aws/credentials
 *    - IAM Role (if running on EC2/ECS)
 *
 * Tests are skipped if KMS key is not configured.
 */
class KmsEncryptionTest {

    private Connection connection;
    private String kmsKeyId;
    private String awsRegion;
    private String awsProfile;
    private Path outputDir;

    @BeforeEach
    void setUp() throws SQLException, IOException {
        // Create output directory for encrypted files
        // Tests run from carpet-jdbc directory, so "encrypted" is relative to carpet-jdbc/
        outputDir = Path.of("encrypted").toAbsolutePath();
        Files.createDirectories(outputDir);

        // Load KMS configuration from properties file
        Properties props = loadTestProperties();
        kmsKeyId = props.getProperty("aws.kms.keyId");
        awsRegion = props.getProperty("aws.kms.region", "us-east-1");
        awsProfile = props.getProperty("aws.profile");

        // Skip tests if KMS key not configured
        assumeTrue(kmsKeyId != null && !kmsKeyId.isEmpty(),
            "KMS key ID not configured. Set aws.kms.keyId in src/test/resources/test.properties");

        // Create DuckDB in-memory database for test data
        connection = DriverManager.getConnection("jdbc:duckdb:");
        createTestTables();
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    /**
     * Test single-file encryption with unique DEK per file.
     * Each file gets its own KMS GenerateDataKey call.
     */
    @Test
    void testSingleFileEncryption() throws Exception {
        // Given
        String sql = "SELECT * FROM customers";
        File outputFile = outputDir.resolve("customers_encrypted.parquet").toFile();
        File metadataFile = new File(outputFile.getAbsolutePath() + ".metadata");

        KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
            .withKmsKeyId(kmsKeyId)
            .withAwsRegion(awsRegion)
            .withAwsProfile(awsProfile)
            .withEncryptionContextEntry("test", "single-file")
            .withEncryptionContextEntry("table", "customers");

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withKmsEncryption(kmsConfig);

        // When - Export with encryption
        long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
            connection, sql, outputFile, config);

        // Then - Verify encryption artifacts
        assertTrue(outputFile.exists(), "Encrypted file should exist");
        assertTrue(metadataFile.exists(), "Metadata file should exist");
        assertTrue(outputFile.length() > 0, "Encrypted file should not be empty");
        assertTrue(metadataFile.length() > 0, "Metadata file should not be empty");
        assertEquals(3, rowCount, "Should export 3 customer rows");

        // Verify we can decrypt and read the data
        @SuppressWarnings("rawtypes")
        List<Map> decryptedData = decryptAndReadParquet(outputFile, kmsConfig);
        assertEquals(3, decryptedData.size(), "Should decrypt 3 rows");

        @SuppressWarnings("rawtypes")
        Map firstRow = decryptedData.get(0);
        assertNotNull(firstRow.get("id"));
        assertNotNull(firstRow.get("name"));
        assertNotNull(firstRow.get("email"));
    }

    /**
     * Test batch encryption with shared DEK.
     * All files share the same DEK - only ONE KMS GenerateDataKey call for entire batch.
     */
    @Test
    void testBatchEncryptionWithSharedKey() throws Exception {
        // Given
        Map<String, String> tableQueries = new LinkedHashMap<>();
        tableQueries.put("customers", "SELECT * FROM customers");
        tableQueries.put("orders", "SELECT * FROM orders");
        tableQueries.put("products", "SELECT * FROM products");

        KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
            .withKmsKeyId(kmsKeyId)
            .withAwsRegion(awsRegion)
            .withAwsProfile(awsProfile)
            .withEncryptionContextEntry("test", "batch-encryption")
            .withEncryptionContextEntry("batch_id", "test-batch-001");

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withKmsEncryption(kmsConfig);

        // When - Export batch with shared DEK (1 KMS call for all 3 tables)
        Map<String, Long> results = DynamicJdbcExporter.exportBatchWithKmsEncryption(
            connection, tableQueries, outputDir.toFile(), config);

        // Then - Verify all files encrypted
        assertEquals(3, results.size(), "Should export 3 tables");
        assertEquals(3L, results.get("customers"), "Should export 3 customers");
        assertEquals(4L, results.get("orders"), "Should export 4 orders");
        assertEquals(4L, results.get("products"), "Should export 4 products");

        // Verify files exist
        File customersFile = outputDir.resolve("customers.parquet").toFile();
        File ordersFile = outputDir.resolve("orders.parquet").toFile();
        File productsFile = outputDir.resolve("products.parquet").toFile();

        assertTrue(customersFile.exists(), "Customers file should exist");
        assertTrue(ordersFile.exists(), "Orders file should exist");
        assertTrue(productsFile.exists(), "Products file should exist");

        // Verify metadata files exist
        assertTrue(new File(customersFile.getAbsolutePath() + ".metadata").exists());
        assertTrue(new File(ordersFile.getAbsolutePath() + ".metadata").exists());
        assertTrue(new File(productsFile.getAbsolutePath() + ".metadata").exists());

        // Verify we can decrypt all files
        @SuppressWarnings("rawtypes")
        List<Map> customersData = decryptAndReadParquet(customersFile, kmsConfig);
        @SuppressWarnings("rawtypes")
        List<Map> ordersData = decryptAndReadParquet(ordersFile, kmsConfig);
        @SuppressWarnings("rawtypes")
        List<Map> productsData = decryptAndReadParquet(productsFile, kmsConfig);

        assertEquals(3, customersData.size(), "Should decrypt 3 customer rows");
        assertEquals(4, ordersData.size(), "Should decrypt 4 order rows");
        assertEquals(4, productsData.size(), "Should decrypt 4 product rows");
    }

    /**
     * Test manual shared context management.
     * Demonstrates explicit control over DEK lifecycle.
     */
    @Test
    void testManualSharedContextEncryption() throws Exception {
        // Given
        KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
            .withKmsKeyId(kmsKeyId)
            .withAwsRegion(awsRegion)
            .withAwsProfile(awsProfile)
            .withEncryptionContextEntry("test", "manual-context");

        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(100)
            .withKmsEncryption(kmsConfig);

        // When - Use shared context manually (1 KMS call, 2 files)
        try (KmsSharedKeyEncryptionContext sharedContext =
                new KmsSharedKeyEncryptionContext(kmsConfig)) {

            long customersCount = DynamicJdbcExporter.exportWithSharedKmsEncryption(
                connection,
                "SELECT * FROM customers",
                outputDir.resolve("customers_shared.parquet").toFile(),
                config,
                sharedContext
            );

            long ordersCount = DynamicJdbcExporter.exportWithSharedKmsEncryption(
                connection,
                "SELECT * FROM orders",
                outputDir.resolve("orders_shared.parquet").toFile(),
                config,
                sharedContext
            );

            assertEquals(3, customersCount, "Should export 3 customers");
            assertEquals(4, ordersCount, "Should export 4 orders");
        }

        // Then - Verify both files encrypted and decryptable
        File customersFile = outputDir.resolve("customers_shared.parquet").toFile();
        File ordersFile = outputDir.resolve("orders_shared.parquet").toFile();

        assertTrue(customersFile.exists());
        assertTrue(ordersFile.exists());

        @SuppressWarnings("rawtypes")
        List<Map> customersData = decryptAndReadParquet(customersFile, kmsConfig);
        @SuppressWarnings("rawtypes")
        List<Map> ordersData = decryptAndReadParquet(ordersFile, kmsConfig);

        assertEquals(3, customersData.size());
        assertEquals(4, ordersData.size());
    }

    /**
     * Test encryption with different encryption contexts.
     * Useful for multi-tenant scenarios.
     */
    @Test
    void testEncryptionWithDifferentContexts() throws Exception {
        // Given - Two different tenants with different encryption contexts
        String[] tenants = {"tenant-a", "tenant-b"};

        for (String tenant : tenants) {
            KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
                .withKmsKeyId(kmsKeyId)
                .withAwsRegion(awsRegion)
                .withAwsProfile(awsProfile)
                .withEncryptionContextEntry("tenant", tenant)
                .withEncryptionContextEntry("test", "multi-tenant");

            DynamicExportConfig config = new DynamicExportConfig()
                .withKmsEncryption(kmsConfig);

            // When - Export for this tenant
            File outputFile = outputDir.resolve(tenant + "_customers.parquet").toFile();
            long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
                connection,
                "SELECT * FROM customers",
                outputFile,
                config
            );

            // Then - Verify encrypted
            assertEquals(3, rowCount);
            assertTrue(outputFile.exists());

            // Verify decryption works with correct context
            @SuppressWarnings("rawtypes")
            List<Map> data = decryptAndReadParquet(outputFile, kmsConfig);
            assertEquals(3, data.size());
        }
    }

    /**
     * Helper: Decrypt and read Parquet file using KMS.
     */
    @SuppressWarnings("rawtypes")
    private List<Map> decryptAndReadParquet(File encryptedFile, KmsEncryptionConfig kmsConfig)
            throws IOException {
        File metadataFile = new File(encryptedFile.getAbsolutePath() + ".metadata");

        // Generate decrypted filename: aaa.parquet -> aaa.decrypted.parquet
        String encryptedPath = encryptedFile.getAbsolutePath();
        String decryptedPath = encryptedPath.replace(".parquet", ".decrypted.parquet");
        File decryptedFile = new File(decryptedPath);

        // Decrypt the file
        KmsEnvelopeEncryptionOutputStream.decryptFile(
            encryptedFile,
            metadataFile,
            decryptedFile,
            kmsConfig
        );

        // Read decrypted Parquet file
        return new CarpetReader<>(decryptedFile, Map.class).toList();
    }

    /**
     * Load test properties containing KMS configuration.
     */
    private Properties loadTestProperties() throws IOException {
        Properties props = new Properties();

        // Try to load from test.properties
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("test.properties")) {
            if (is != null) {
                props.load(is);
            }
        }

        // Also check for local override file (not in version control)
        File localProps = new File("src/test/resources/test-local.properties");
        if (localProps.exists()) {
            try (FileInputStream fis = new FileInputStream(localProps)) {
                props.load(fis);
            }
        }

        return props;
    }

    /**
     * Create test tables with sample data.
     */
    private void createTestTables() throws SQLException {
        // Customers table
        connection.createStatement().execute(
            "CREATE TABLE customers (" +
            "  id INTEGER PRIMARY KEY, " +
            "  name VARCHAR(100), " +
            "  email VARCHAR(100), " +
            "  created_at TIMESTAMP" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO customers VALUES " +
            "(1, 'Alice Johnson', 'alice@example.com', TIMESTAMP '2024-01-15 10:30:00'), " +
            "(2, 'Bob Smith', 'bob@example.com', TIMESTAMP '2024-01-16 14:20:00'), " +
            "(3, 'Charlie Brown', 'charlie@example.com', TIMESTAMP '2024-01-17 09:15:00')");

        // Orders table
        connection.createStatement().execute(
            "CREATE TABLE orders (" +
            "  order_id INTEGER PRIMARY KEY, " +
            "  customer_id INTEGER, " +
            "  order_date DATE, " +
            "  total_amount DECIMAL(10,2), " +
            "  status VARCHAR(20)" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO orders VALUES " +
            "(101, 1, DATE '2024-01-20', 150.75, 'completed'), " +
            "(102, 2, DATE '2024-01-21', 299.99, 'completed'), " +
            "(103, 1, DATE '2024-01-22', 89.50, 'pending'), " +
            "(104, 3, DATE '2024-01-23', 450.00, 'completed')");

        // Products table
        connection.createStatement().execute(
            "CREATE TABLE products (" +
            "  product_id INTEGER PRIMARY KEY, " +
            "  name VARCHAR(100), " +
            "  category VARCHAR(50), " +
            "  price DECIMAL(10,2)" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO products VALUES " +
            "(201, 'Laptop', 'Electronics', 999.99), " +
            "(202, 'Mouse', 'Electronics', 29.99), " +
            "(203, 'Desk Chair', 'Furniture', 249.99), " +
            "(204, 'Monitor', 'Electronics', 399.99)");
    }
}
