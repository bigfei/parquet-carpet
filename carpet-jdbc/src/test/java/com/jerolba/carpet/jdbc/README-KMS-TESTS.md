# Running KMS Encryption Tests

## Quick Start

### 1. Set up AWS KMS

Follow the [AWS KMS Setup Guide](../../docs/aws-kms-setup.md) to:
- Create a KMS key
- Configure IAM permissions
- Set up AWS credentials

### 2. Configure Test Properties

Create `src/test/resources/test.properties`:

```properties
aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
aws.kms.region=us-east-1
```

Or copy the sample file:
```bash
cp src/test/resources/test.properties.sample src/test/resources/test.properties
# Edit test.properties with your KMS key ID
```

### 3. Configure AWS Credentials

Choose one method:

**Option A: Environment Variables**
```bash
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_DEFAULT_REGION=us-east-1
```

**Option B: AWS Credentials File**
```bash
# ~/.aws/credentials
[default]
aws_access_key_id = your-access-key
aws_secret_access_key = your-secret-key

# ~/.aws/config
[default]
region = us-east-1
```

**Option C: IAM Role** (if running on EC2/ECS/Lambda)
```bash
# No configuration needed - automatically uses instance role
```

### 4. Run Tests

```bash
# Run all KMS encryption tests
./gradlew :carpet-jdbc:test --tests KmsEncryptionTest

# Run specific test
./gradlew :carpet-jdbc:test --tests KmsEncryptionTest.testSingleFileEncryption
./gradlew :carpet-jdbc:test --tests KmsEncryptionTest.testBatchEncryptionWithSharedKey
./gradlew :carpet-jdbc:test --tests KmsEncryptionTest.testManualSharedContextEncryption
```

## Test Coverage

### `testSingleFileEncryption`
Tests encrypting a single file with unique DEK:
- Generates new DEK per file
- Makes 1 KMS GenerateDataKey call
- Verifies encrypted file and metadata created
- Verifies file can be decrypted

### `testBatchEncryptionWithSharedKey`
Tests batch encryption with shared DEK:
- Exports 3 tables (customers, orders, products)
- Makes only 1 KMS GenerateDataKey call for all 3 files
- Each file gets unique IV for security
- Verifies all files can be decrypted independently

### `testManualSharedContextEncryption`
Tests manual control over shared DEK context:
- Explicitly creates `KmsSharedKeyEncryptionContext`
- Exports 2 files using shared context
- Demonstrates try-with-resources pattern
- Verifies proper resource cleanup

### `testEncryptionWithDifferentContexts`
Tests multi-tenant encryption patterns:
- Creates files for different tenants
- Each tenant uses different encryption context
- Demonstrates isolation between tenants
- Verifies context-specific decryption

## What Tests Verify

✅ **Single-file encryption** works correctly
✅ **Batch encryption** reduces KMS calls (1 call for N files)
✅ **Shared DEK context** management and cleanup
✅ **Different encryption contexts** for multi-tenant scenarios
✅ **Encryption metadata** files created correctly
✅ **Decryption** works for all encryption modes
✅ **End-to-end roundtrip** (encrypt → decrypt → read data)

## Skipping Tests

If KMS key is not configured, tests are automatically skipped:

```
KmsEncryptionTest > testSingleFileEncryption() SKIPPED
  Assumption failed: KMS key ID not configured
```

To skip tests manually:
```bash
./gradlew :carpet-jdbc:test -x KmsEncryptionTest
```

## Troubleshooting

### "KMS key ID not configured"
**Solution**: Create `test.properties` file with your KMS key ID

### "Unable to locate credentials"
**Solution**: Configure AWS credentials (see step 3 above)

### "User is not authorized to perform: kms:GenerateDataKey"
**Solution**: Add KMS permissions to your IAM user/role:
```json
{
  "Effect": "Allow",
  "Action": ["kms:GenerateDataKey", "kms:Decrypt"],
  "Resource": "arn:aws:kms:*:*:key/*"
}
```

### "The ciphertext refers to a customer master key that does not exist"
**Solution**: Check the KMS key ID in `test.properties` is correct

### Tests pass but decryption fails
**Solution**: Ensure the same encryption context is used for decryption

## Test Data

Tests use DuckDB in-memory database with sample data:

**Customers** (3 rows):
- Alice Johnson, Bob Smith, Charlie Brown

**Orders** (4 rows):
- Order amounts: $150.75, $299.99, $89.50, $450.00

**Products** (4 rows):
- Laptop, Mouse, Desk Chair, Monitor

## Performance Notes

Tests measure KMS API call efficiency:

- **Single-file mode**: N files = N KMS calls
- **Batch mode**: N files = 1 KMS call (90%+ reduction)

Watch console output for KMS call notifications:
```
Generated shared DEK using KMS GenerateDataKey (plaintext DEK never sent over network)
=== Batch KMS Encryption (1 KMS call for 3 tables) ===
```

## CI/CD Integration

### Skip in CI without KMS Access
```yaml
# GitHub Actions example
- name: Run tests
  run: ./gradlew test
  env:
    AWS_ACCESS_KEY_ID: ${{ secrets.AWS_ACCESS_KEY_ID }}
    AWS_SECRET_ACCESS_KEY: ${{ secrets.AWS_SECRET_ACCESS_KEY }}
    AWS_REGION: us-east-1
  # Tests auto-skip if AWS_ACCESS_KEY_ID not set
```

### Run with Secrets
Store KMS key and AWS credentials as CI/CD secrets:
- `AWS_ACCESS_KEY_ID`
- `AWS_SECRET_ACCESS_KEY`
- `KMS_KEY_ID`

Generate `test.properties` dynamically:
```bash
cat > src/test/resources/test.properties <<EOF
aws.kms.keyId=$KMS_KEY_ID
aws.kms.region=us-east-1
EOF
```

## Cost Impact

Each test run costs (approximate):
- 4 KMS GenerateDataKey calls × $0.000003 = $0.000012
- 8 KMS Decrypt calls × $0.000003 = $0.000024
- **Total per test run: ~$0.000036** (negligible)

Running 1000 times: ~$0.036

## Security Notes

⚠️ **Never commit `test.properties` with real KMS key IDs**

Add to `.gitignore`:
```gitignore
src/test/resources/test.properties
src/test/resources/test-local.properties
```

✅ Only commit `test.properties.sample` (template without credentials)

## See Also

- [AWS KMS Setup Guide](../../docs/aws-kms-setup.md) - Complete KMS configuration
- [KMS Batch Encryption](../../docs/kms-batch-encryption.md) - Batch mode documentation
- [KMS Security Improvement](../../docs/kms-security-improvement.md) - Why GenerateDataKey is secure
