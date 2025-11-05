# AWS KMS Setup Guide for Envelope Encryption

This guide explains how to set up AWS KMS for encrypting Parquet files using envelope encryption.

## Overview

The envelope encryption implementation requires:
1. **AWS KMS Customer Master Key (CMK)** - Master key that encrypts data encryption keys
2. **IAM Permissions** - Access to generate and decrypt data keys
3. **AWS Credentials** - Authentication to AWS services

## Step 1: Create a KMS Key

### Option A: Using AWS Console

1. Go to **AWS Console** → **KMS** → **Customer managed keys**
2. Click **Create key**
3. Configure:
   - **Key type**: Symmetric
   - **Key usage**: Encrypt and decrypt
   - **Regionality**: Single-Region key (or Multi-Region if needed)
4. Add **Key alias** (e.g., `parquet-encryption`)
5. Define **key administrators** (users who can manage the key)
6. Define **key users** (users/roles who can use the key for encryption/decryption)
7. Review and click **Finish**
8. **Copy the Key ARN** - you'll need this for configuration

### Option B: Using AWS CLI

```bash
# Create the key
aws kms create-key \
  --description "Parquet file envelope encryption" \
  --key-usage ENCRYPT_DECRYPT \
  --origin AWS_KMS

# The output contains the KeyId - save this!
# Example output:
# {
#   "KeyMetadata": {
#     "KeyId": "12345678-1234-1234-1234-123456789012",
#     "Arn": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
#   }
# }

# Create an alias for easier reference
aws kms create-alias \
  --alias-name alias/parquet-encryption \
  --target-key-id 12345678-1234-1234-1234-123456789012
```

### Option C: Using CloudFormation/Terraform

**CloudFormation:**
```yaml
Resources:
  ParquetEncryptionKey:
    Type: AWS::KMS::Key
    Properties:
      Description: KMS key for Parquet file envelope encryption
      KeyPolicy:
        Version: '2012-10-17'
        Statement:
          - Sid: Enable IAM User Permissions
            Effect: Allow
            Principal:
              AWS: !Sub 'arn:aws:iam::${AWS::AccountId}:root'
            Action: 'kms:*'
            Resource: '*'
          - Sid: Allow key usage
            Effect: Allow
            Principal:
              AWS: !GetAtt ApplicationRole.Arn
            Action:
              - 'kms:GenerateDataKey'
              - 'kms:Decrypt'
            Resource: '*'

  ParquetEncryptionKeyAlias:
    Type: AWS::KMS::Alias
    Properties:
      AliasName: alias/parquet-encryption
      TargetKeyId: !Ref ParquetEncryptionKey
```

**Terraform:**
```hcl
resource "aws_kms_key" "parquet_encryption" {
  description             = "KMS key for Parquet file envelope encryption"
  deletion_window_in_days = 10
  enable_key_rotation     = true
}

resource "aws_kms_alias" "parquet_encryption" {
  name          = "alias/parquet-encryption"
  target_key_id = aws_kms_key.parquet_encryption.key_id
}

output "kms_key_id" {
  value = aws_kms_key.parquet_encryption.key_id
}

output "kms_key_arn" {
  value = aws_kms_key.parquet_encryption.arn
}
```

## Step 2: Configure IAM Permissions

The IAM user or role running the encryption code needs these permissions:

### Required Permissions

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowKMSDataKeyOperations",
      "Effect": "Allow",
      "Action": [
        "kms:GenerateDataKey",
        "kms:Decrypt"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012"
    }
  ]
}
```

### Permissions Explanation

- **`kms:GenerateDataKey`** - Required for **encryption**
  - Generates a new data encryption key (DEK)
  - Returns both plaintext and encrypted versions
  - Called once per file (single-file mode) or once per batch (batch mode)

- **`kms:Decrypt`** - Required for **decryption**
  - Decrypts the encrypted DEK stored in `.metadata` files
  - Called once per file being decrypted

### Optional: Encryption Context Conditions

For additional security, you can require specific encryption contexts:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowKMSWithContext",
      "Effect": "Allow",
      "Action": [
        "kms:GenerateDataKey",
        "kms:Decrypt"
      ],
      "Resource": "arn:aws:kms:us-east-1:123456789012:key/*",
      "Condition": {
        "StringEquals": {
          "kms:EncryptionContext:application": "parquet-export",
          "kms:EncryptionContext:environment": "production"
        }
      }
    }
  ]
}
```

Then in your code:
```java
KmsEncryptionConfig config = KmsEncryptionConfig.builder()
    .withKmsKeyId(kmsKeyId)
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("application", "parquet-export")
    .withEncryptionContextEntry("environment", "production");
```

### Attach Policy to IAM User/Role

**Option A: Inline policy**
```bash
aws iam put-user-policy \
  --user-name my-user \
  --policy-name ParquetKMSAccess \
  --policy-document file://kms-policy.json
```

**Option B: Managed policy**
```bash
# Create policy
aws iam create-policy \
  --policy-name ParquetKMSAccess \
  --policy-document file://kms-policy.json

# Attach to user
aws iam attach-user-policy \
  --user-name my-user \
  --policy-arn arn:aws:iam::123456789012:policy/ParquetKMSAccess

# Or attach to role (for EC2/ECS/Lambda)
aws iam attach-role-policy \
  --role-name my-application-role \
  --policy-arn arn:aws:iam::123456789012:policy/ParquetKMSAccess
```

## Step 3: Configure AWS Credentials

The application needs AWS credentials to authenticate KMS API calls.

### Option A: Environment Variables (Recommended for Development)

```bash
export AWS_ACCESS_KEY_ID=AKIAIOSFODNN7EXAMPLE
export AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
export AWS_DEFAULT_REGION=us-east-1
```

### Option B: AWS Credentials File (Recommended for Local Development)

Create or edit `~/.aws/credentials`:
```ini
[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY
```

Create or edit `~/.aws/config`:
```ini
[default]
region = us-east-1
```

### Option C: IAM Role (Recommended for Production)

For applications running on:
- **EC2**: Attach an IAM role to the instance
- **ECS**: Assign a task role with KMS permissions
- **Lambda**: Configure the execution role with KMS permissions
- **EKS**: Use IAM Roles for Service Accounts (IRSA)

**Example: EC2 Instance Role**
```bash
# Create role
aws iam create-role \
  --role-name EC2ParquetEncryptionRole \
  --assume-role-policy-document '{
    "Version": "2012-10-17",
    "Statement": [{
      "Effect": "Allow",
      "Principal": {"Service": "ec2.amazonaws.com"},
      "Action": "sts:AssumeRole"
    }]
  }'

# Attach KMS policy
aws iam attach-role-policy \
  --role-name EC2ParquetEncryptionRole \
  --policy-arn arn:aws:iam::123456789012:policy/ParquetKMSAccess

# Create instance profile
aws iam create-instance-profile \
  --instance-profile-name EC2ParquetEncryptionProfile

# Add role to instance profile
aws iam add-role-to-instance-profile \
  --instance-profile-name EC2ParquetEncryptionProfile \
  --role-name EC2ParquetEncryptionRole

# Attach to EC2 instance
aws ec2 associate-iam-instance-profile \
  --instance-id i-1234567890abcdef0 \
  --iam-instance-profile Name=EC2ParquetEncryptionProfile
```

No explicit credentials needed in code - AWS SDK automatically uses the instance role.

### Option D: AWS SSO (for Organizations)

```bash
# Configure SSO
aws configure sso

# Login
aws sso login --profile my-profile

# Use profile
export AWS_PROFILE=my-profile
```

## Step 4: Verify Setup

### Test AWS Credentials
```bash
# Check identity
aws sts get-caller-identity

# Should return:
# {
#   "UserId": "AIDAI...",
#   "Account": "123456789012",
#   "Arn": "arn:aws:iam::123456789012:user/my-user"
# }
```

### Test KMS Access
```bash
# Test GenerateDataKey permission
aws kms generate-data-key \
  --key-id alias/parquet-encryption \
  --key-spec AES_256

# Should return success with Plaintext and CiphertextBlob

# Test Decrypt permission
aws kms decrypt \
  --key-id alias/parquet-encryption \
  --ciphertext-blob fileb://encrypted-key.bin

# Should return success with Plaintext
```

### Test with Java Code

```java
import com.jerolba.carpet.jdbc.*;

public class KmsTest {
    public static void main(String[] args) throws Exception {
        // Configure KMS
        KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
            .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/...")
            .withAwsRegion("us-east-1");

        // Test by creating a shared context (this calls GenerateDataKey)
        try (KmsSharedKeyEncryptionContext context =
                new KmsSharedKeyEncryptionContext(kmsConfig)) {
            System.out.println("✓ KMS setup working!");
        } catch (Exception e) {
            System.err.println("✗ KMS setup failed: " + e.getMessage());
            throw e;
        }
    }
}
```

## Step 5: Application Configuration

### In Properties File
```properties
# KMS Configuration
aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
aws.kms.region=us-east-1
```

### In Code
```java
// Load from properties
Properties props = new Properties();
props.load(new FileInputStream("config.properties"));

KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId(props.getProperty("aws.kms.keyId"))
    .withAwsRegion(props.getProperty("aws.kms.region"));
```

## Security Best Practices

### 1. Enable Key Rotation
```bash
aws kms enable-key-rotation --key-id 12345678-1234-1234-1234-123456789012
```

### 2. Use Encryption Context
```java
KmsEncryptionConfig config = KmsEncryptionConfig.builder()
    .withKmsKeyId(kmsKeyId)
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("application", "parquet-export")
    .withEncryptionContextEntry("environment", "production")
    .withEncryptionContextEntry("team", "data-engineering");
```

Benefits:
- Additional authentication (AAD - Additional Authenticated Data)
- Logged in CloudTrail for auditing
- Can be used in IAM conditions for fine-grained access control

### 3. Monitor KMS Usage
Enable CloudTrail logging for KMS:
```bash
aws cloudtrail create-trail \
  --name kms-audit-trail \
  --s3-bucket-name my-audit-bucket

aws cloudtrail start-logging --name kms-audit-trail
```

Monitor:
- Failed GenerateDataKey attempts (unauthorized access)
- Decrypt operations (who's accessing encrypted data)
- Encryption context values in CloudTrail logs

### 4. Use Least Privilege
Grant only necessary permissions:
- Development: GenerateDataKey + Decrypt
- Production encryption: GenerateDataKey only
- Production decryption: Decrypt only

### 5. Consider Key Aliases
Use aliases instead of key IDs for flexibility:
```java
KmsEncryptionConfig config = KmsEncryptionConfig.builder()
    .withKmsKeyId("alias/parquet-encryption-prod")  // ← Alias, not key ID
    .withAwsRegion("us-east-1");
```

Benefits:
- Can rotate to a new key without code changes
- More readable than UUIDs
- Can have different aliases per environment

## Cost Considerations

AWS KMS pricing (as of 2024):
- **$1/month** per CMK
- **$0.03 per 10,000 requests** (GenerateDataKey + Decrypt)

### Cost Example: Batch Encryption

**Without batch mode** (10,000 files):
- 10,000 GenerateDataKey calls = $0.03
- 10,000 Decrypt calls (when reading) = $0.03
- **Total: $0.06 per batch**

**With batch mode** (10,000 files):
- 1 GenerateDataKey call = $0.000003
- 10,000 Decrypt calls (when reading) = $0.03
- **Total: $0.030003 per batch**
- **Savings: 50% on encryption operations**

For high-volume scenarios, batch mode significantly reduces costs.

## Troubleshooting

### Error: "User is not authorized to perform: kms:GenerateDataKey"
**Solution**: Add GenerateDataKey permission to IAM policy (see Step 2)

### Error: "The ciphertext refers to a customer master key that does not exist"
**Solution**: Check KMS key ID is correct and exists in the specified region

### Error: "Unable to locate credentials"
**Solution**: Configure AWS credentials (see Step 3)

### Error: "The encryption context provided does not match"
**Solution**: Use the same encryption context for decryption as was used for encryption

### Error: "KMS key is pending deletion"
**Solution**: Cancel key deletion or use a different key
```bash
aws kms cancel-key-deletion --key-id 12345678-1234-1234-1234-123456789012
```

## Summary

**Minimum setup checklist:**
- [ ] Create KMS key (Step 1)
- [ ] Configure IAM permissions: `kms:GenerateDataKey`, `kms:Decrypt` (Step 2)
- [ ] Configure AWS credentials (Step 3)
- [ ] Test setup with `aws kms generate-data-key` (Step 4)
- [ ] Configure application with KMS key ID (Step 5)

**Production checklist:**
- [ ] Enable key rotation
- [ ] Use encryption context for auditing
- [ ] Configure CloudTrail logging
- [ ] Use IAM roles instead of access keys
- [ ] Use batch mode for cost optimization
- [ ] Monitor KMS usage and costs
- [ ] Document key purpose and owner
- [ ] Set up key aliases per environment

## References

- [AWS KMS Developer Guide](https://docs.aws.amazon.com/kms/latest/developerguide/)
- [AWS KMS Best Practices](https://docs.aws.amazon.com/kms/latest/developerguide/best-practices.html)
- [AWS KMS Pricing](https://aws.amazon.com/kms/pricing/)
- [AWS SDK for Java - KMS](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/examples-kms.html)
