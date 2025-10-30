# AWS KMS Envelope Encryption for Parquet Exports

This feature provides transparent encryption of Parquet files during JDBC exports using AWS KMS (Key Management Service) envelope encryption.

## Overview

Envelope encryption is a security best practice that combines the performance of symmetric encryption with the security of asymmetric encryption:

1. **Data Encryption Key (DEK)**: A random AES-256 key is generated for each file
2. **Encryption**: The Parquet file is encrypted with the DEK using AES-256-GCM
3. **Key Encryption**: The DEK is encrypted with your AWS KMS Customer Master Key (CMK)
4. **Storage**: The encrypted DEK is stored in a companion `.metadata` file alongside the encrypted Parquet file

## Prerequisites

### 1. Add AWS SDK Dependency

Add the AWS SDK KMS dependency to your `build.gradle`:

```gradle
dependencies {
    implementation "software.amazon.awssdk:kms:2.20.0"
}
```

Or for Maven (`pom.xml`):

```xml
<dependency>
    <groupId>software.amazon.awssdk</groupId>
    <artifactId>kms</artifactId>
    <version>2.20.0</version>
</dependency>
```

### 2. Configure AWS Credentials

Configure AWS credentials using one of these methods:

**Environment Variables:**
```bash
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
```

**AWS Credentials File** (`~/.aws/credentials`):
```ini
[default]
aws_access_key_id = your_access_key
aws_secret_access_key = your_secret_key
```

**IAM Role** (for EC2, ECS, Lambda):
The AWS SDK will automatically use the IAM role attached to your compute instance.

### 3. Create and Configure KMS Key

Create a KMS key with appropriate permissions:

```bash
aws kms create-key --description "Parquet file encryption key"
aws kms create-alias --alias-name alias/parquet-encryption --target-key-id <key-id>
```

Ensure your IAM user/role has these permissions:
- `kms:Encrypt` - Required for encrypting DEKs
- `kms:Decrypt` - Required for decrypting files (if needed)
- `kms:DescribeKey` - Optional, for key validation

## Usage

### Basic Example

```java
import com.jerolba.carpet.jdbc.*;
import java.io.File;
import java.sql.Connection;

// Configure KMS encryption
KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
    .withKmsKeyId("alias/parquet-encryption")
    .withAwsRegion("us-east-1");

// Configure export with encryption
DynamicExportConfig exportConfig = new DynamicExportConfig()
    .withBatchSize(5000)
    .withKmsEncryption(kmsConfig);

// Export with encryption
long rowCount = DynamicJdbcExporter.exportWithKmsEncryption(
    connection,
    "SELECT * FROM sensitive_table",
    new File("/secure/exports/data.parquet"),
    exportConfig
);
```

### With Encryption Context

Encryption context provides additional authenticated data (AAD) for audit trails:

```java
KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
    .withKmsKeyId("alias/parquet-encryption")
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("application", "data-export")
    .withEncryptionContextEntry("environment", "production")
    .withEncryptionContextEntry("user", "admin@example.com")
    .withEncryptionContextEntry("table", "customers")
    .withEncryptionContextEntry("export_date", "2025-10-28");

DynamicExportConfig config = new DynamicExportConfig()
    .withKmsEncryption(kmsConfig);

DynamicJdbcExporter.exportWithKmsEncryption(
    connection,
    "SELECT * FROM customers",
    new File("/exports/customers.parquet"),
    config
);
```

**Benefits of Encryption Context:**
- Logged in AWS CloudTrail for audit compliance
- Can be used in KMS key policies for fine-grained access control
- Must be provided during decryption (prevents unauthorized decryption)

### Full Configuration Example

```java
KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
    .withAwsRegion("us-east-1")
    .withAlgorithm("AES/GCM/NoPadding")  // Default
    .withKeySize(256)                     // Default (128 or 256)
    .withEncryptionContextEntry("purpose", "compliance-report");

DynamicExportConfig exportConfig = new DynamicExportConfig()
    .withBatchSize(10000)
    .withFetchSize(10000)
    .withCompressionCodec(CompressionCodecName.SNAPPY)
    .withKmsEncryption(kmsConfig);

DynamicJdbcExporter.exportWithKmsEncryption(
    connection,
    "SELECT * FROM orders WHERE date > CURRENT_DATE - INTERVAL '30 days'",
    new File("/exports/orders_encrypted.parquet"),
    exportConfig
);
```

## Metadata File Format

The `.metadata` file is created alongside each encrypted Parquet file in JSON format:

```json
{
  "version": "1.0",
  "algorithm": "AES/GCM/NoPadding",
  "keySize": 256,
  "kmsKeyId": "alias/parquet-encryption",
  "encryptedDataKey": "AQIDAHj...base64-encoded-encrypted-key...==",
  "iv": "dGVzdCBpdiB0ZXN0",
  "encryptionContext": {
    "application": "data-export",
    "environment": "production"
  },
  "timestamp": "1730131200000"
}
```

## Decryption

To decrypt an encrypted Parquet file, you need:

1. The encrypted Parquet file
2. The `.metadata` file
3. Access to the same KMS key used for encryption
4. The same encryption context (if one was used)

### Decryption Example

```java
import com.jerolba.carpet.jdbc.*;
import java.io.File;

// Configure KMS with same settings used during encryption
KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("application", "data-export")
    .withEncryptionContextEntry("environment", "production");

// Decrypt the file
File encryptedFile = new File("/exports/data.parquet");
File metadataFile = new File("/exports/data.parquet.metadata");
File decryptedFile = new File("/tmp/data-decrypted.parquet");

KmsEnvelopeEncryptionOutputStream.decryptFile(
    encryptedFile,
    metadataFile,
    decryptedFile,
    kmsConfig
);

// Now read the decrypted Parquet file normally
var reader = new CarpetReader<>(decryptedFile, MyRecord.class);
List<MyRecord> data = reader.toList();
```

### Minimal Configuration Decryption

You don't need to specify the KMS key ID - it will be read from the metadata file:

```java
// Only need region and encryption context
KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig()
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("application", "data-export");

KmsEnvelopeEncryptionOutputStream.decryptFile(
    encryptedFile,
    metadataFile,
    decryptedFile,
    kmsConfig
);
```

### Error Handling

```java
try {
    KmsEnvelopeEncryptionOutputStream.decryptFile(
        encryptedFile,
        metadataFile,
        decryptedFile,
        kmsConfig
    );
    System.out.println("Decryption successful!");

} catch (FileNotFoundException e) {
    System.err.println("File not found: " + e.getMessage());

} catch (IOException e) {
    if (e.getMessage().contains("KMS")) {
        System.err.println("KMS error - check encryption context and permissions");
    } else if (e.getMessage().contains("metadata")) {
        System.err.println("Invalid or corrupted metadata file");
    }
    e.printStackTrace();
}
```

### Common Decryption Errors

**Access Denied**
- Ensure IAM user/role has `kms:Decrypt` permission
- Verify the KMS key policy allows decryption

**InvalidCiphertextException**
- Encryption context must match exactly
- Verify all key-value pairs are identical to those used during encryption

**Metadata file errors**
- Ensure `.metadata` file wasn't modified
- Check file permissions and readability

## Security Best Practices

### 1. Use Encryption Context
Always use encryption context for audit trails and access control:
```java
.withEncryptionContextEntry("application", "my-app")
.withEncryptionContextEntry("user", currentUser)
```

### 2. Restrict KMS Key Access
Create restrictive KMS key policies:
```json
{
  "Effect": "Allow",
  "Principal": {
    "AWS": "arn:aws:iam::123456789012:role/DataExportRole"
  },
  "Action": ["kms:Encrypt", "kms:Decrypt"],
  "Resource": "*",
  "Condition": {
    "StringEquals": {
      "kms:EncryptionContext:application": "data-export",
      "kms:EncryptionContext:environment": "production"
    }
  }
}
```

### 3. Enable CloudTrail Logging
Monitor KMS operations in CloudTrail for compliance:
```bash
aws cloudtrail lookup-events \
  --lookup-attributes AttributeKey=ResourceName,AttributeValue=alias/parquet-encryption
```

### 4. Rotate KMS Keys Regularly
Enable automatic key rotation:
```bash
aws kms enable-key-rotation --key-id <key-id>
```

### 5. Secure Metadata Files
Protect `.metadata` files with same access controls as encrypted data:
- Store in secure locations
- Use S3 bucket policies if storing in S3
- Never commit to version control

## Performance Considerations

- **Encryption Overhead**: AES-256-GCM adds ~5-10% performance overhead
- **KMS API Calls**: One KMS encrypt call per file (not per row)
- **Memory Usage**: No significant increase, encryption is streaming
- **File Size**: Encrypted files are slightly larger due to GCM tags

## Troubleshooting

### "KmsClient cannot be resolved"
**Solution**: Add AWS SDK KMS dependency to your project

### "Access Denied" errors
**Solution**: Check IAM permissions for `kms:Encrypt` on the specified key

### "InvalidCiphertextException" during decryption
**Solution**: Ensure encryption context matches during encryption and decryption

### Files are not encrypted
**Solution**: Verify `config.withKmsEncryption(kmsConfig)` is called before export

## Integration with S3

For S3 uploads, export locally first then upload both files:

```java
File localFile = new File("/tmp/export.parquet");
DynamicJdbcExporter.exportWithKmsEncryption(connection, query, localFile, config);

// Upload both files to S3
s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-bucket")
    .key("data/export.parquet")
    .build(), localFile.toPath());

s3Client.putObject(PutObjectRequest.builder()
    .bucket("my-bucket")
    .key("data/export.parquet.metadata")
    .build(), new File(localFile + ".metadata").toPath());
```

## Comparison with S3 Encryption

| Feature | KMS Envelope Encryption | S3 Server-Side Encryption |
|---------|------------------------|--------------------------|
| **Encryption Location** | Client-side (before upload) | Server-side (during storage) |
| **Key Control** | Full control over KMS keys | AWS-managed or customer keys |
| **Audit Trail** | CloudTrail logs all operations | Limited to S3 access logs |
| **Portability** | Works with any storage | S3-specific |
| **Performance** | Slight client overhead | No client overhead |
| **Use Case** | Maximum security control | Simplified encryption |

## License

Apache License 2.0
