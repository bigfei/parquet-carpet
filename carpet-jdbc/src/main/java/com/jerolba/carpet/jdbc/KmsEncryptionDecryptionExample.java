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

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;

/**
 * Complete example demonstrating encryption and decryption with AWS KMS.
 */
public class KmsEncryptionDecryptionExample {

    public static void main(String[] args) throws Exception {
        // Example 1: Encrypt data during export
        encryptExample();

        // Example 2: Decrypt previously encrypted file
        decryptExample();

        // Example 3: Complete round-trip
        roundTripExample();
    }

    /**
     * Example: Encrypt data during JDBC export
     */
    public static void encryptExample() throws Exception {
        Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb",
            "user",
            "password"
        );

        // Configure KMS encryption
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("application", "data-export")
            .withEncryptionContextEntry("environment", "production");

        // Configure export with encryption
        DynamicExportConfig exportConfig = new DynamicExportConfig()
            .withBatchSize(5000)
            .withKmsEncryption(kmsConfig);

        // Export with encryption
        File encryptedFile = new File("/tmp/encrypted-export.parquet");
        long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM users WHERE active = true",
            encryptedFile,
            exportConfig
        );

        System.out.println("Exported " + rowCount + " rows");
        System.out.println("Encrypted file: " + encryptedFile.getAbsolutePath());
        System.out.println("Metadata file: " + encryptedFile.getAbsolutePath() + ".metadata");

        connection.close();
    }

    /**
     * Example: Decrypt a previously encrypted file
     */
    public static void decryptExample() throws Exception {
        // Files to decrypt
        File encryptedFile = new File("/tmp/encrypted-export.parquet");
        File metadataFile = new File("/tmp/encrypted-export.parquet.metadata");
        File decryptedFile = new File("/tmp/decrypted-export.parquet");

        // Configure KMS (must match encryption context used during encryption)
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")  // Can be omitted, will use key from metadata
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("application", "data-export")
            .withEncryptionContextEntry("environment", "production");

        // Decrypt the file
        KmsEnvelopeEncryptionOutputStream.decryptFile(
            encryptedFile,
            metadataFile,
            decryptedFile,
            kmsConfig
        );

        System.out.println("Decrypted file: " + decryptedFile.getAbsolutePath());

        // Now you can read the decrypted Parquet file normally
        // var reader = new CarpetReader<>(decryptedFile, MyRecord.class);
        // List<MyRecord> data = reader.toList();
    }

    /**
     * Example: Complete round-trip (encrypt, then decrypt, then read)
     */
    public static void roundTripExample() throws Exception {
        Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb",
            "user",
            "password"
        );

        // Step 1: Export with encryption
        System.out.println("=== Step 1: Encrypting data ===");

        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("dataset", "customers")
            .withEncryptionContextEntry("export_date", "2025-10-28");

        DynamicExportConfig exportConfig = new DynamicExportConfig()
            .withBatchSize(10000)
            .withKmsEncryption(kmsConfig);

        File encryptedFile = new File("/tmp/customers-encrypted.parquet");
        long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM customers",
            encryptedFile,
            exportConfig
        );

        System.out.println("Encrypted " + rowCount + " rows");
        System.out.println("Encrypted file size: " + encryptedFile.length() + " bytes");

        // Step 2: Decrypt the file
        System.out.println("\n=== Step 2: Decrypting data ===");

        File metadataFile = new File(encryptedFile.getAbsolutePath() + ".metadata");
        File decryptedFile = new File("/tmp/customers-decrypted.parquet");

        KmsEnvelopeEncryptionOutputStream.decryptFile(
            encryptedFile,
            metadataFile,
            decryptedFile,
            kmsConfig
        );

        System.out.println("Decrypted file size: " + decryptedFile.length() + " bytes");

        // Step 3: Read decrypted Parquet file
        System.out.println("\n=== Step 3: Reading decrypted data ===");
        // This would require a proper record class
        // var reader = new CarpetReader<>(decryptedFile, Customer.class);
        // List<Customer> customers = reader.toList();
        // System.out.println("Read " + customers.size() + " records");

        // Cleanup
        connection.close();
        System.out.println("\n=== Round-trip complete ===");
    }

    /**
     * Example: Decrypt without knowing the KMS key ID (reads from metadata)
     */
    public static void decryptWithMinimalConfig() throws Exception {
        File encryptedFile = new File("/tmp/encrypted-export.parquet");
        File metadataFile = new File("/tmp/encrypted-export.parquet.metadata");
        File decryptedFile = new File("/tmp/decrypted-output.parquet");

        // Minimal config - only need region and encryption context
        // KMS key ID will be read from metadata file
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("application", "data-export")
            .withEncryptionContextEntry("environment", "production");

        KmsEnvelopeEncryptionOutputStream.decryptFile(
            encryptedFile,
            metadataFile,
            decryptedFile,
            kmsConfig
        );

        System.out.println("Decrypted successfully!");
    }

    /**
     * Example: Handling different encryption contexts
     */
    public static void multipleDatasets() throws Exception {
        Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb",
            "user",
            "password"
        );

        // Dataset 1: Customer data
        KmsEncryptionConfig customersConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("dataset", "customers")
            .withEncryptionContextEntry("classification", "PII");

        DynamicExportConfig config1 = new DynamicExportConfig()
            .withKmsEncryption(customersConfig);

        DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM customers",
            new File("/secure/customers.parquet"),
            config1
        );

        // Dataset 2: Orders data
        KmsEncryptionConfig ordersConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("dataset", "orders")
            .withEncryptionContextEntry("classification", "confidential");

        DynamicExportConfig config2 = new DynamicExportConfig()
            .withKmsEncryption(ordersConfig);

        DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM orders",
            new File("/secure/orders.parquet"),
            config2
        );

        connection.close();

        System.out.println("Both datasets encrypted with different contexts");
    }

    /**
     * Example: Error handling during decryption
     */
    public static void decryptWithErrorHandling() {
        File encryptedFile = new File("/tmp/encrypted-export.parquet");
        File metadataFile = new File("/tmp/encrypted-export.parquet.metadata");
        File decryptedFile = new File("/tmp/decrypted-output.parquet");

        try {
            KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
                .withAwsRegion("us-east-1")
                .withEncryptionContextEntry("application", "data-export")
                .withEncryptionContextEntry("environment", "production");

            KmsEnvelopeEncryptionOutputStream.decryptFile(
                encryptedFile,
                metadataFile,
                decryptedFile,
                kmsConfig
            );

            System.out.println("Decryption successful!");

        } catch (java.io.FileNotFoundException e) {
            System.err.println("File not found: " + e.getMessage());
            System.err.println("Make sure both encrypted file and metadata file exist");

        } catch (java.io.IOException e) {
            if (e.getMessage().contains("KMS")) {
                System.err.println("KMS error: " + e.getMessage());
                System.err.println("Possible causes:");
                System.err.println("  - Wrong encryption context");
                System.err.println("  - Insufficient KMS permissions");
                System.err.println("  - Wrong AWS region");
            } else if (e.getMessage().contains("metadata")) {
                System.err.println("Metadata file error: " + e.getMessage());
                System.err.println("The metadata file may be corrupted or invalid");
            } else {
                System.err.println("Decryption error: " + e.getMessage());
            }
            e.printStackTrace();
        }
    }
}
