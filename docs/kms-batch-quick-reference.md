# KMS Batch Encryption - Quick Reference

## Problem Solved
Making one KMS API call per Parquet file is expensive and slow for batch exports.

## Solution
Share a single Data Encryption Key (DEK) across multiple files in a batch - **1 KMS call instead of N**.

---

## Basic Usage (Recommended)

```java
// Prepare queries
Map<String, String> tableQueries = new LinkedHashMap<>();
tableQueries.put("customers", "SELECT * FROM customers");
tableQueries.put("orders", "SELECT * FROM orders");
tableQueries.put("products", "SELECT * FROM products");

// Configure encryption
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/...")
    .withAwsRegion("us-east-1");

DynamicExportConfig config = new DynamicExportConfig()
    .withBatchSize(1000)
    .withKmsEncryption(kmsConfig);

// Export with ONE KMS call
Map<String, Long> results = DynamicJdbcExporter.exportBatchWithKmsEncryption(
    connection,
    tableQueries,
    new File("output"),
    config
);
```

**Result**: 3 files created with 1 KMS API call total.

---

## Manual Control

```java
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/...")
    .withAwsRegion("us-east-1");

// Create shared context (1 KMS call happens here)
try (KmsSharedKeyEncryptionContext context =
        new KmsSharedKeyEncryptionContext(kmsConfig)) {

    // Export multiple files (no additional KMS calls)
    DynamicJdbcExporter.exportWithSharedKmsEncryption(
        connection, "SELECT * FROM customers",
        new File("customers.parquet"), config, context
    );

    DynamicJdbcExporter.exportWithSharedKmsEncryption(
        connection, "SELECT * FROM orders",
        new File("orders.parquet"), config, context
    );
}
```

---

## Before & After

### Before (Inefficient)
```java
for (String table : tables) {
    // Makes 1 KMS call per iteration
    DynamicJdbcExporter.exportWithKmsEncryption(
        connection,
        "SELECT * FROM " + table,
        new File(table + ".parquet"),
        config
    );
}
```
**10 tables = 10 KMS calls**

### After (Optimized)
```java
Map<String, String> queries = new LinkedHashMap<>();
for (String table : tables) {
    queries.put(table, "SELECT * FROM " + table);
}

// Makes 1 KMS call total
DynamicJdbcExporter.exportBatchWithKmsEncryption(
    connection, queries, new File("output"), config
);
```
**10 tables = 1 KMS call (90% reduction)**

---

## Performance Comparison

| Tables | Without Batch | With Batch | Savings |
|--------|---------------|------------|---------|
| 10     | 10 KMS calls  | 1 KMS call | 90%     |
| 50     | 50 KMS calls  | 1 KMS call | 98%     |
| 100    | 100 KMS calls | 1 KMS call | 99%     |

---

## Security

✅ Each file gets unique IV (initialization vector)
✅ Same AES-256-GCM encryption strength
✅ Files can be decrypted independently
✅ CloudTrail audit logging preserved

**What's shared**: Only the DEK during the export session
**What's unique**: IV per file, metadata per file

---

## When to Use

### Use Batch Mode ✅
- Exporting multiple tables from same database
- Scheduled batch jobs (nightly exports, etc.)
- Multi-tenant scenarios (multiple files per tenant)
- High-frequency exports where KMS costs matter

### Use Single-File Mode ⚠️
- One-off single file exports
- Files need different encryption contexts
- Maximum isolation required between files
- Files exported at different times (not a batch)

---

## API Quick Reference

### High-Level (Recommended)
```java
Map<String, Long> exportBatchWithKmsEncryption(
    Connection connection,
    Map<String, String> tableQueries,  // filename -> SQL
    File outputDirectory,
    DynamicExportConfig config
)
```

### Low-Level (Manual Control)
```java
long exportWithSharedKmsEncryption(
    Connection connection,
    String sqlQuery,
    File outputFile,
    DynamicExportConfig config,
    KmsSharedKeyEncryptionContext sharedContext
)
```

### Context Management
```java
KmsSharedKeyEncryptionContext context =
    new KmsSharedKeyEncryptionContext(KmsEncryptionConfig config);

context.createEncryptingOutputStream(File outputFile);
context.close();  // Or use try-with-resources
```

---

## Example Code

See complete runnable example:
```
carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/example/KmsEncryptionBatchExample.java
```

Run with:
```bash
export KMS_KEY_ID="arn:aws:kms:us-east-1:123456789012:key/..."
./gradlew :carpet-jdbc:run -PmainClass=com.jerolba.carpet.jdbc.example.KmsEncryptionBatchExample
```

---

## Documentation

- [Full Batch Encryption Guide](kms-batch-encryption.md)
- [Implementation Details](kms-batch-optimization-summary.md)
- [Basic KMS Encryption](kms-encryption.md)
- [Decryption Guide](kms-decryption-implementation.md)

---

## Troubleshooting

**Q: Can I mix batch and single-file exports?**
A: Yes! Use batch for bulk exports, single-file for one-offs.

**Q: What if I need different encryption contexts?**
A: Create separate batches per encryption context (e.g., one batch per tenant).

**Q: Are the encrypted files compatible?**
A: Yes! Files encrypted with batch mode can be decrypted exactly the same way as single-file mode.

**Q: What if the batch fails mid-way?**
A: Files already written are valid and can be decrypted. The context cleans up automatically with try-with-resources.

---

## Cost Calculation

AWS KMS pricing: ~$0.03 per 10,000 requests (varies by region)

**Example: 1000 tables/month**

Without batch:
- 1000 tables × 1 KMS call = 1000 requests
- Cost: $0.003/month

With batch:
- 1 batch × 1 KMS call = 1 request
- Cost: $0.000003/month
- **Savings: $0.002997/month (99.7% reduction)**

Scale up to production volumes for real savings!
