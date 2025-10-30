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
 * Example demonstrating AWS KMS envelope encryption for Parquet exports.
 *
 * Prerequisites:
 * 1. Add AWS SDK KMS dependency to your project:
 *    implementation "software.amazon.awssdk:kms:2.20.0"
 *
 * 2. Configure AWS credentials (one of):
 *    - Environment variables: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY
 *    - AWS credentials file: ~/.aws/credentials
 *    - IAM role (for EC2/ECS/Lambda)
 *
 * 3. Ensure KMS key policy grants encrypt permission
 */
public class KmsEncryptionExample {

    public static void main(String[] args) throws Exception {
        // Database connection setup
        Connection connection = DriverManager.getConnection(
            "jdbc:postgresql://localhost:5432/mydb",
            "user",
            "password"
        );

        // Configure KMS encryption
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/my-parquet-key")  // or use full ARN
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("application", "data-export")
            .withEncryptionContextEntry("environment", "production");

        // Configure export with KMS encryption
        DynamicExportConfig exportConfig = new DynamicExportConfig()
            .withBatchSize(5000)
            .withFetchSize(5000)
            .withKmsEncryption(kmsConfig);

        // Export with encryption
        File outputFile = new File("/tmp/encrypted-export.parquet");
        long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM users WHERE active = true",
            outputFile,
            exportConfig
        );

        System.out.println("Exported " + rowCount + " rows");
        System.out.println("Encrypted file: " + outputFile.getAbsolutePath());
        System.out.println("Metadata file: " + outputFile.getAbsolutePath() + ".metadata");

        connection.close();
    }

    /**
     * Example with multiple encryption context entries for audit trails
     */
    public static void exportWithAuditContext(Connection connection) throws Exception {
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
            .withAwsRegion("us-east-1")
            .withEncryptionContextEntry("user", "admin@example.com")
            .withEncryptionContextEntry("table", "sensitive_data")
            .withEncryptionContextEntry("export_date", "2025-10-28")
            .withEncryptionContextEntry("purpose", "compliance-report");

        DynamicExportConfig config = new DynamicExportConfig()
            .withKmsEncryption(kmsConfig);

        DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM sensitive_data",
            new File("/secure/exports/sensitive_data.parquet"),
            config
        );
    }

    /**
     * Example exporting to S3 with encryption (requires S3 OutputFile implementation)
     */
    public static void exportToS3WithEncryption(Connection connection) throws Exception {
        // Note: This is pseudocode. You would need to implement S3OutputFile
        // or use a temporary local file and upload to S3 after encryption

        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
            .withKmsKeyId("alias/s3-parquet-key")
            .withAwsRegion("us-west-2");

        // Export to local temp file with encryption
        File tempFile = File.createTempFile("export-", ".parquet");
        DynamicExportConfig config = new DynamicExportConfig()
            .withKmsEncryption(kmsConfig);

        DynamicJdbcExporter.exportWithKmsEncryption(
            connection,
            "SELECT * FROM orders",
            tempFile,
            config
        );

        // Then upload both files to S3
        // uploadToS3(tempFile, "s3://my-bucket/data/orders.parquet");
        // uploadToS3(new File(tempFile + ".metadata"), "s3://my-bucket/data/orders.parquet.metadata");

        tempFile.delete();
        new File(tempFile.getAbsolutePath() + ".metadata").delete();
    }
}
