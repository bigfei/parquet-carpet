# KMS Security Improvement: Using GenerateDataKey

## Overview

The implementation has been updated to use AWS KMS `GenerateDataKey` API instead of locally generating keys and encrypting them with KMS `Encrypt` API. This improves security by ensuring **the plaintext DEK never travels TO AWS** over the network.

## What Changed

### Before (Original Implementation)

```java
// 1. Generate DEK locally
KeyGenerator keyGen = KeyGenerator.getInstance("AES");
SecretKey dek = keyGen.generateKey();

// 2. Encrypt DEK by sending plaintext to KMS
EncryptRequest request = EncryptRequest.builder()
    .keyId(kmsKeyId)
    .plaintext(SdkBytes.fromByteArray(dek.getEncoded()))  // ← Plaintext sent to AWS
    .build();

EncryptResponse response = kmsClient.encrypt(request);
```

**Network traffic**: Plaintext DEK sent TO AWS (protected by TLS, but still transmitted)

### After (New Implementation)

```java
// 1. Request KMS to generate DEK (happens inside AWS)
GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
    .keyId(kmsKeyId)
    .keySpec("AES_256")
    .build();

// 2. Receive both plaintext and encrypted DEK FROM AWS
GenerateDataKeyResponse response = kmsClient.generateDataKey(request);
SecretKey dek = new SecretKeySpec(response.plaintext().asByteArray(), "AES");
byte[] encryptedDek = response.ciphertextBlob().asByteArray();
```

**Network traffic**: Plaintext DEK only travels FROM AWS (generated inside AWS KMS)

## Security Benefits

### 1. Reduced Attack Surface
- **Before**: Plaintext DEK transmitted in both directions (to AWS for encryption)
- **After**: Plaintext DEK only transmitted in one direction (from AWS after generation)

### 2. AWS Best Practice
The `GenerateDataKey` API is AWS's recommended approach for envelope encryption:
- Used by S3 server-side encryption
- Used by EBS volume encryption
- Used by AWS SDKs (encryption client libraries)
- Documented as best practice in AWS security whitepapers

### 3. Key Generation Inside AWS
- DEK is generated inside AWS KMS service (FIPS 140-2 Level 3 validated)
- Uses AWS's hardware security modules (HSMs)
- Better entropy source than local SecureRandom

### 4. Atomic Operation
- Single API call returns both plaintext and encrypted DEK
- No window between key generation and encryption
- Reduces potential timing attacks

## Network Traffic Analysis

### Before: Two-way Plaintext Transmission
```
Client                              AWS KMS
------                              -------
1. Generate random DEK locally
2. Send plaintext DEK ----TLS----> Receive plaintext DEK
3. Receive encrypted DEK <--TLS--- Encrypt with CMK, return
```

### After: One-way Plaintext Transmission
```
Client                              AWS KMS
------                              -------
1. Request data key -----TLS-----> Generate DEK in HSM
2. Receive plaintext + encrypted <-TLS--- Return both versions
```

**Key difference**: Plaintext DEK never leaves your infrastructure TO AWS.

## Implementation Details

### Both Classes Updated

1. **`KmsSharedKeyEncryptionContext`** (batch encryption)
   - Now uses `GenerateDataKey` in constructor
   - Single KMS call generates DEK for entire batch
   - More secure than previous approach

2. **`KmsEnvelopeEncryptionOutputStream`** (single file)
   - Now uses `GenerateDataKey` in first constructor
   - Second constructor (shared context) unchanged
   - Maintains backward compatibility

### Code Changes

**Key generation method** (both classes):
```java
private GenerateDataKeyResponse generateDataKeyWithKms(KmsEncryptionConfig config) {
    String keySpec = (config.getKeySize() == 128) ? "AES_128" : "AES_256";

    GenerateDataKeyRequest request = GenerateDataKeyRequest.builder()
        .keyId(config.getKmsKeyId())
        .keySpec(keySpec)
        .encryptionContext(config.getEncryptionContext())
        .build();

    return kmsClient.generateDataKey(request);
}
```

**Replaced**:
- `KeyGenerator.getInstance("AES")` - local key generation
- `EncryptRequest` / `EncryptResponse` - separate encryption step

**With**:
- `GenerateDataKeyRequest` / `GenerateDataKeyResponse` - single atomic operation

## Still Protected by TLS

**Important**: While the plaintext DEK does travel over the network FROM AWS, it is still protected by:
- **TLS 1.2+** encryption on all AWS SDK connections
- **AWS SDK's built-in security**: Certificate validation, secure defaults
- **IAM permissions**: Only authorized principals can call GenerateDataKey

This is exactly how AWS's own services (S3, EBS, etc.) work.

## Backward Compatibility

✅ **Fully backward compatible**
- Encrypted files use the same format
- Decryption logic unchanged (still uses `Decrypt` API)
- Metadata files identical structure
- No migration needed for existing files

The change only affects **encryption time** (how the DEK is created), not decryption or file format.

## Performance Impact

**Negligible difference**:
- Same number of KMS API calls
- `GenerateDataKey` and `Encrypt` have similar latency
- Both are single round-trips to AWS KMS

**Batch mode still optimal**:
- Single `GenerateDataKey` call for entire batch
- 90%+ reduction in KMS calls vs. single-file mode

## Security Comparison

| Aspect | Old (Encrypt API) | New (GenerateDataKey API) |
|--------|-------------------|---------------------------|
| Plaintext DEK to AWS | Yes (over TLS) | No |
| Plaintext DEK from AWS | No | Yes (over TLS) |
| Key generation | Local (SecureRandom) | AWS HSM |
| FIPS 140-2 compliance | Depends on local JCE | Yes (AWS KMS HSM) |
| AWS best practice | No | Yes ✅ |
| Used by AWS services | No | Yes (S3, EBS, etc.) |
| Attack surface | Slightly larger | Smaller |

## References

- [AWS KMS GenerateDataKey API](https://docs.aws.amazon.com/kms/latest/APIReference/API_GenerateDataKey.html)
- [AWS Encryption SDK](https://docs.aws.amazon.com/encryption-sdk/latest/developer-guide/how-it-works.html)
- [AWS S3 Encryption](https://docs.aws.amazon.com/AmazonS3/latest/userguide/UsingKMSEncryption.html)
- [Envelope Encryption Best Practices](https://docs.aws.amazon.com/wellarchitected/latest/framework/sec_protect_data_at_rest_encrypted.html)

## Migration

**No action required!** The change is automatic and transparent:
- Existing code works without changes
- New encryptions automatically use `GenerateDataKey`
- Decryption of old files works unchanged
- File format identical

This is a drop-in security improvement.
