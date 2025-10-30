# KMS Batch Encryption for JDBC Exports

## Overview

When exporting multiple tables or queries to Parquet files with KMS encryption, you can use **batch encryption** to dramatically reduce AWS KMS API calls. Instead of making one KMS call per file, batch mode makes **only ONE KMS call** for the entire batch by sharing a Data Encryption Key (DEK) across all exports.

## Performance Benefits

### Cost Savings
AWS KMS charges per API call. Batch encryption can reduce costs by 90%+ for multi-table exports:

- **Without batch**: 10 tables × 1 KMS call each = **10 KMS API calls**
- **With batch**: 10 tables with shared DEK = **1 KMS API call**
- **Savings**: 9 fewer KMS calls (90% reduction)

### Speed Improvement
Each KMS API call adds network latency (~50-200ms). For batch exports:

- **Without batch**: 10 × 100ms = 1000ms in KMS overhead
- **With batch**: 1 × 100ms = 100ms in KMS overhead
- **Improvement**: 900ms faster

## Security Considerations

Batch encryption maintains the same security level as single-file encryption:

- ✅ Each file still gets a **unique initialization vector (IV)**, which is critical for AES-GCM security
- ✅ The shared DEK is only used within a single batch export session
- ✅ Different batches use different DEKs
- ✅ Each file's metadata still contains the encrypted DEK specific to that file

The only thing shared is the plaintext DEK during the export process. Each file can be decrypted independently using its own metadata.

## Usage

### Single Batch Export

Export multiple tables using a single KMS call:

```java
// Prepare queries for multiple tables
Map<String, String> tableQueries = new LinkedHashMap<>();
tableQueries.put("customers", "SELECT * FROM customers");
tableQueries.put("orders", "SELECT * FROM orders");
tableQueries.put("products", "SELECT * FROM products");

// Configure KMS encryption
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/...")
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("database", "sales_db")
    .withEncryptionContextEntry("environment", "production");

// Configure export
DynamicExportConfig exportConfig = new DynamicExportConfig()
    .withBatchSize(1000)
    .withKmsEncryption(kmsConfig);

// Export all tables with ONE KMS call
File outputDir = new File("encrypted_exports");
Map<String, Long> results = DynamicJdbcExporter.exportBatchWithKmsEncryption(
    connection,
    tableQueries,
    outputDir,
    exportConfig
);

// Results contain row counts per table
results.forEach((table, rows) ->
    System.out.println(table + ": " + rows + " rows")
);
```

### Manual Control with Shared Context

For more control, you can create the shared context explicitly:

```java
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/...")
    .withAwsRegion("us-east-1");

DynamicExportConfig exportConfig = new DynamicExportConfig()
    .withKmsEncryption(kmsConfig);

// Create shared context - this makes ONE KMS call
try (KmsSharedKeyEncryptionContext sharedContext =
        new KmsSharedKeyEncryptionContext(kmsConfig)) {

    // Export multiple tables reusing the same DEK
    DynamicJdbcExporter.exportWithSharedKmsEncryption(
        connection,
        "SELECT * FROM customers",
        new File("customers.parquet"),
        exportConfig,
        sharedContext
    );

    DynamicJdbcExporter.exportWithSharedKmsEncryption(
        connection,
        "SELECT * FROM orders",
        new File("orders.parquet"),
        exportConfig,
        sharedContext
    );

    // ... more exports ...

} // Shared context closes automatically
```

## Multi-Tenant Scenarios

Batch encryption is ideal for multi-tenant exports where each tenant needs different encryption contexts:

```java
String[] tenants = {"tenant-a", "tenant-b", "tenant-c"};

for (String tenant : tenants) {
    // Each tenant gets its own encryption context
    KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
        .withKmsKeyId(kmsKeyId)
        .withAwsRegion("us-east-1")
        .withEncryptionContextEntry("tenant", tenant)
        .withEncryptionContextEntry("export_type", "daily");

    DynamicExportConfig exportConfig = new DynamicExportConfig()
        .withKmsEncryption(kmsConfig);

    // Export all tables for this tenant (1 KMS call per tenant)
    Map<String, String> queries = getTenantQueries(tenant);
    DynamicJdbcExporter.exportBatchWithKmsEncryption(
        connection,
        queries,
        new File("exports/" + tenant),
        exportConfig
    );
}
```

Result: **3 KMS calls total** (1 per tenant) instead of N×3 calls (where N = number of tables per tenant).

## Best Practices

### When to Use Batch Mode
- ✅ Exporting multiple tables from the same database
- ✅ Scheduled batch jobs that export many files at once
- ✅ Multi-tenant scenarios with multiple files per tenant
- ✅ High-frequency exports where KMS costs are significant

### When to Use Single-File Mode
- ✅ Exporting one-off queries
- ✅ Different encryption contexts required per file
- ✅ Files exported at different times (not a batch)
- ✅ Maximum isolation between files is required

### Encryption Context Strategy
For batch exports, use encryption context to track the batch:

```java
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId(kmsKeyId)
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("batch_id", UUID.randomUUID().toString())
    .withEncryptionContextEntry("export_date", LocalDate.now().toString())
    .withEncryptionContextEntry("database", "production");
```

This helps with:
- CloudTrail auditing (all files in a batch have the same context)
- Access control policies (can grant/deny access by batch_id)
- Troubleshooting (correlate files from the same export operation)

## Implementation Details

### How It Works

1. **Create Shared Context**: `KmsSharedKeyEncryptionContext` generates one random AES-256 key (DEK)
2. **Encrypt DEK with KMS**: ONE call to `kms:Encrypt` to protect the DEK
3. **Export Files**: Each file:
   - Uses the shared DEK for AES-GCM encryption
   - Gets a unique random IV (96 bits)
   - Stores the encrypted DEK in its `.metadata` file
4. **Close Context**: KMS client is closed, DEK is cleared from memory

### File Structure

Each encrypted file in the batch has:
```
customers.parquet           # Encrypted data (AES-256-GCM with unique IV)
customers.parquet.metadata  # Contains encrypted DEK, IV, algorithm
orders.parquet              # Encrypted data (AES-256-GCM with unique IV)
orders.parquet.metadata     # Contains same encrypted DEK, different IV
```

### Security Notes

- The plaintext DEK exists only in memory during the export session
- Each file can be decrypted independently (no need for the other files)
- The encrypted DEK in metadata is protected by AWS KMS permissions
- Different encryption contexts can still be used (just create separate batches)

## API Reference

### DynamicJdbcExporter Methods

#### exportBatchWithKmsEncryption()
```java
public static Map<String, Long> exportBatchWithKmsEncryption(
    Connection connection,
    Map<String, String> tableQueries,  // filename -> SQL query
    File outputDirectory,
    DynamicExportConfig config
) throws SQLException, IOException
```
High-level method for batch export with automatic DEK sharing.

#### exportWithSharedKmsEncryption()
```java
public static long exportWithSharedKmsEncryption(
    Connection connection,
    String sqlQuery,
    File outputFile,
    DynamicExportConfig config,
    KmsSharedKeyEncryptionContext sharedContext
) throws SQLException, IOException
```
Low-level method for manual control. You create and manage the shared context.

### KmsSharedKeyEncryptionContext

```java
public class KmsSharedKeyEncryptionContext implements AutoCloseable {
    public KmsSharedKeyEncryptionContext(KmsEncryptionConfig config);
    public KmsEnvelopeEncryptionOutputStream createEncryptingOutputStream(File file);
    public void close();
}
```

Thread-safe context for sharing a DEK across multiple file exports. Use try-with-resources to ensure proper cleanup.

## Examples

See complete runnable examples in:
- `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/example/KmsEncryptionBatchExample.java`

## Migration Guide

If you're currently using `exportWithKmsEncryption()` in a loop:

### Before (Multiple KMS Calls)
```java
for (String table : tables) {
    DynamicJdbcExporter.exportWithKmsEncryption(
        connection,
        "SELECT * FROM " + table,
        new File(table + ".parquet"),
        config
    ); // Each call makes a KMS API call
}
```

### After (Single KMS Call)
```java
Map<String, String> queries = new LinkedHashMap<>();
for (String table : tables) {
    queries.put(table, "SELECT * FROM " + table);
}

DynamicJdbcExporter.exportBatchWithKmsEncryption(
    connection,
    queries,
    new File("output"),
    config
); // ONE KMS call for all tables
```

## Related Documentation

- [KMS Encryption Overview](kms-encryption.md)
- [KMS Encryption Implementation](kms-encryption-implementation.md)
- [KMS Decryption](kms-decryption-implementation.md)
