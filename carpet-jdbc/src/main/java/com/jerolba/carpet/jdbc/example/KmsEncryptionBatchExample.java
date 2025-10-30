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
package com.jerolba.carpet.jdbc.example;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.LinkedHashMap;
import java.util.Map;

import com.jerolba.carpet.jdbc.DynamicExportConfig;
import com.jerolba.carpet.jdbc.DynamicJdbcExporter;
import com.jerolba.carpet.jdbc.KmsEncryptionConfig;

/**
 * Example demonstrating batch KMS encryption for multiple table exports.
 *
 * This example shows how to export multiple tables efficiently by sharing a
 * single Data Encryption Key (DEK) across all exports. This reduces KMS API
 * calls from N (one per file) to just ONE for the entire batch.
 *
 * Performance comparison:
 * - Without batch: 10 tables × 1 KMS call each = 10 KMS API calls
 * - With batch: 10 tables with 1 shared DEK = 1 KMS API call
 *
 * Each file still maintains security with unique IVs (initialization vectors).
 */
public class KmsEncryptionBatchExample {

    public static void main(String[] args) throws SQLException, IOException {
        // AWS KMS Key ARN or Alias
        // Format: arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
        // Or alias: alias/my-key-alias
        String kmsKeyId = System.getenv("KMS_KEY_ID");
        if (kmsKeyId == null || kmsKeyId.isEmpty()) {
            System.err.println("Please set KMS_KEY_ID environment variable");
            System.err.println("Example: export KMS_KEY_ID=arn:aws:kms:us-east-1:123456789012:key/...");
            System.exit(1);
        }

        // Example 1: Export multiple tables from a single database
        batchExportMultipleTables(kmsKeyId);

        // Example 2: Export the same data with different encryption contexts
        batchExportWithDifferentContexts(kmsKeyId);
    }

    /**
     * Export multiple tables using a single KMS call.
     */
    private static void batchExportMultipleTables(String kmsKeyId) throws SQLException, IOException {
        System.out.println("\n=== Example 1: Batch Export Multiple Tables ===\n");

        String jdbcUrl = "jdbc:h2:mem:testdb";

        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            // Create test tables
            createTestTables(connection);

            // Prepare queries for multiple tables
            Map<String, String> tableQueries = new LinkedHashMap<>();
            tableQueries.put("customers", "SELECT * FROM customers");
            tableQueries.put("orders", "SELECT * FROM orders");
            tableQueries.put("products", "SELECT * FROM products");
            tableQueries.put("order_items", "SELECT * FROM order_items");

            // Configure KMS encryption
            KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
                .withKmsKeyId(kmsKeyId)
                .withAwsRegion("us-east-1")
                .withEncryptionContextEntry("database", "sales_db")
                .withEncryptionContextEntry("environment", "production");

            // Configure export
            DynamicExportConfig exportConfig = new DynamicExportConfig()
                .withBatchSize(1000)
                .withKmsEncryption(kmsConfig);

            // Export all tables with ONE KMS call
            File outputDir = new File("encrypted_batch_export");
            outputDir.mkdirs();

            long startTime = System.currentTimeMillis();

            Map<String, Long> results = DynamicJdbcExporter.exportBatchWithKmsEncryption(
                connection,
                tableQueries,
                outputDir,
                exportConfig
            );

            long duration = System.currentTimeMillis() - startTime;

            // Print results
            System.out.println("\n=== Export Summary ===");
            System.out.println("Duration: " + duration + " ms");
            System.out.println("KMS API calls: 1 (shared DEK)");
            System.out.println("Tables exported: " + results.size());
            results.forEach((table, rows) ->
                System.out.println("  - " + table + ": " + rows + " rows")
            );
            System.out.println("\nOutput directory: " + outputDir.getAbsolutePath());
            System.out.println("\nPerformance benefit:");
            System.out.println("  Without batch: " + results.size() + " KMS calls (1 per table)");
            System.out.println("  With batch: 1 KMS call (shared DEK)");
            System.out.println("  Reduction: " + (results.size() - 1) + " fewer KMS API calls");
        }
    }

    /**
     * Export the same query multiple times with different encryption contexts.
     * Useful for multi-tenant scenarios or region-specific exports.
     */
    private static void batchExportWithDifferentContexts(String kmsKeyId) throws SQLException, IOException {
        System.out.println("\n\n=== Example 2: Batch Export with Different Contexts ===\n");

        String jdbcUrl = "jdbc:h2:mem:testdb2";

        try (Connection connection = DriverManager.getConnection(jdbcUrl)) {
            // Create test data
            connection.createStatement().execute(
                "CREATE TABLE transactions (id INT, amount DECIMAL(10,2), region VARCHAR(50))");
            connection.createStatement().execute(
                "INSERT INTO transactions VALUES (1, 100.50, 'US'), (2, 200.75, 'EU')");

            // Export for different regions/tenants
            String[] tenants = {"tenant-a", "tenant-b", "tenant-c"};

            for (String tenant : tenants) {
                // Each tenant gets its own encryption context
                KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
                    .withKmsKeyId(kmsKeyId)
                    .withAwsRegion("us-east-1")
                    .withEncryptionContextEntry("tenant", tenant)
                    .withEncryptionContextEntry("export_type", "daily_snapshot");

                DynamicExportConfig exportConfig = new DynamicExportConfig()
                    .withKmsEncryption(kmsConfig);

                // Create tenant-specific directory
                File tenantDir = new File("encrypted_tenant_exports/" + tenant);
                tenantDir.mkdirs();

                // Export all tables for this tenant
                Map<String, String> queries = new LinkedHashMap<>();
                queries.put("transactions", "SELECT * FROM transactions");
                queries.put("summary", "SELECT region, SUM(amount) as total FROM transactions GROUP BY region");

                System.out.println("Exporting for " + tenant + "...");

                Map<String, Long> results = DynamicJdbcExporter.exportBatchWithKmsEncryption(
                    connection,
                    queries,
                    tenantDir,
                    exportConfig
                );

                System.out.println("  ✓ " + tenant + ": " + results.size() + " files exported");
            }

            System.out.println("\n=== Multi-Tenant Export Complete ===");
            System.out.println("Total tenants: " + tenants.length);
            System.out.println("Files per tenant: 2");
            System.out.println("Total KMS calls: " + tenants.length + " (1 per tenant)");
            System.out.println("Without batch: " + (tenants.length * 2) + " KMS calls");
            System.out.println("Savings: " + (tenants.length) + " fewer KMS calls");
        }
    }

    /**
     * Create test tables with sample data.
     */
    private static void createTestTables(Connection connection) throws SQLException {
        // Customers table
        connection.createStatement().execute(
            "CREATE TABLE customers (" +
            "  id INT PRIMARY KEY, " +
            "  name VARCHAR(100), " +
            "  email VARCHAR(100), " +
            "  created_at TIMESTAMP" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO customers VALUES " +
            "(1, 'Alice Johnson', 'alice@example.com', '2024-01-15 10:30:00'), " +
            "(2, 'Bob Smith', 'bob@example.com', '2024-01-16 14:20:00'), " +
            "(3, 'Charlie Brown', 'charlie@example.com', '2024-01-17 09:15:00')");

        // Orders table
        connection.createStatement().execute(
            "CREATE TABLE orders (" +
            "  order_id INT PRIMARY KEY, " +
            "  customer_id INT, " +
            "  order_date DATE, " +
            "  total_amount DECIMAL(10,2), " +
            "  status VARCHAR(20)" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO orders VALUES " +
            "(101, 1, '2024-01-20', 150.75, 'completed'), " +
            "(102, 2, '2024-01-21', 299.99, 'completed'), " +
            "(103, 1, '2024-01-22', 89.50, 'pending'), " +
            "(104, 3, '2024-01-23', 450.00, 'completed')");

        // Products table
        connection.createStatement().execute(
            "CREATE TABLE products (" +
            "  product_id INT PRIMARY KEY, " +
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

        // Order items table
        connection.createStatement().execute(
            "CREATE TABLE order_items (" +
            "  order_id INT, " +
            "  product_id INT, " +
            "  quantity INT, " +
            "  unit_price DECIMAL(10,2)" +
            ")");
        connection.createStatement().execute(
            "INSERT INTO order_items VALUES " +
            "(101, 202, 2, 29.99), " +
            "(101, 203, 1, 249.99), " +
            "(102, 201, 1, 999.99), " +
            "(103, 202, 3, 29.99), " +
            "(104, 201, 1, 999.99), " +
            "(104, 204, 1, 399.99)");
    }
}
