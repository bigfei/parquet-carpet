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

import java.util.HashMap;
import java.util.Map;

/**
 * Configuration for AWS KMS envelope encryption of Parquet files.
 *
 * Envelope encryption works by:
 * 1. Generating a random data encryption key (DEK) for each file
 * 2. Encrypting the file with the DEK using AES-256-GCM
 * 3. Encrypting the DEK itself with AWS KMS
 * 4. Storing the encrypted DEK alongside the encrypted file
 */
public class KmsEncryptionConfig {
    private String kmsKeyId;
    private Map<String, String> encryptionContext;
    private String algorithm = "AES/GCM/NoPadding";
    private int keySize = 256;
    private String awsRegion;
    private String awsProfile;
    private String kmsEndpointUrl;

    /**
     * Default constructor
     */
    public KmsEncryptionConfig() {
        this.encryptionContext = new HashMap<>();
    }

    /**
     * Create configuration with KMS key ID
     *
     * @param kmsKeyId AWS KMS key ID, ARN, or alias (e.g., "alias/my-key" or
     *                 "arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
     */
    public KmsEncryptionConfig(String kmsKeyId) {
        this();
        this.kmsKeyId = kmsKeyId;
    }

    /**
     * Get the KMS key ID
     */
    public String getKmsKeyId() {
        return kmsKeyId;
    }

    /**
     * Set the KMS key ID
     *
     * @param kmsKeyId AWS KMS key ID, ARN, or alias
     */
    public void setKmsKeyId(String kmsKeyId) {
        this.kmsKeyId = kmsKeyId;
    }

    /**
     * Get the encryption context
     *
     * Encryption context is additional authenticated data (AAD) that provides additional
     * security. It's logged in CloudTrail and can be used for access control.
     */
    public Map<String, String> getEncryptionContext() {
        return encryptionContext;
    }

    /**
     * Set the encryption context
     */
    public void setEncryptionContext(Map<String, String> encryptionContext) {
        this.encryptionContext = encryptionContext;
    }

    /**
     * Add an entry to the encryption context
     */
    public void addEncryptionContext(String key, String value) {
        this.encryptionContext.put(key, value);
    }

    /**
     * Get the encryption algorithm (default: AES/GCM/NoPadding)
     */
    public String getAlgorithm() {
        return algorithm;
    }

    /**
     * Set the encryption algorithm
     */
    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    /**
     * Get the key size in bits (default: 256)
     */
    public int getKeySize() {
        return keySize;
    }

    /**
     * Set the key size in bits (128 or 256)
     */
    public void setKeySize(int keySize) {
        if (keySize != 128 && keySize != 256) {
            throw new IllegalArgumentException("Key size must be 128 or 256 bits");
        }
        this.keySize = keySize;
    }

    /**
     * Get the AWS region
     */
    public String getAwsRegion() {
        return awsRegion;
    }

    /**
     * Set the AWS region for KMS operations
     */
    public void setAwsRegion(String awsRegion) {
        this.awsRegion = awsRegion;
    }

    /**
     * Builder pattern for method chaining
     */
    public static KmsEncryptionConfig builder() {
        return new KmsEncryptionConfig();
    }

    /**
     * Set KMS key ID and return this instance for chaining
     */
    public KmsEncryptionConfig withKmsKeyId(String kmsKeyId) {
        setKmsKeyId(kmsKeyId);
        return this;
    }

    /**
     * Set encryption context and return this instance for chaining
     */
    public KmsEncryptionConfig withEncryptionContext(Map<String, String> encryptionContext) {
        setEncryptionContext(encryptionContext);
        return this;
    }

    /**
     * Add encryption context entry and return this instance for chaining
     */
    public KmsEncryptionConfig withEncryptionContextEntry(String key, String value) {
        addEncryptionContext(key, value);
        return this;
    }

    /**
     * Set algorithm and return this instance for chaining
     */
    public KmsEncryptionConfig withAlgorithm(String algorithm) {
        setAlgorithm(algorithm);
        return this;
    }

    /**
     * Set key size and return this instance for chaining
     */
    public KmsEncryptionConfig withKeySize(int keySize) {
        setKeySize(keySize);
        return this;
    }

    /**
     * Set AWS region and return this instance for chaining
     */
    public KmsEncryptionConfig withAwsRegion(String awsRegion) {
        setAwsRegion(awsRegion);
        return this;
    }

    /**
     * Get the AWS profile name for credentials lookup.
     *
     * When configured, the SDK will use the specified profile from ~/.aws/credentials
     * instead of the default credential provider chain.
     *
     * @return the AWS profile name (e.g., "uat2", "production")
     */
    public String getAwsProfile() {
        return awsProfile;
    }

    /**
     * Set the AWS profile name for credentials lookup.
     *
     * Use this to specify which profile from ~/.aws/credentials to use for
     * AWS authentication. If not set, the default credential provider chain is used.
     *
     * @param awsProfile the profile name (e.g., "uat2", "production")
     */
    public void setAwsProfile(String awsProfile) {
        this.awsProfile = awsProfile;
    }

    /**
     * Set AWS profile and return this instance for chaining.
     *
     * Use this to specify which profile from ~/.aws/credentials to use for
     * AWS authentication.
     *
     * @param awsProfile the profile name (e.g., "uat2", "production")
     * @return this instance for method chaining
     */
    public KmsEncryptionConfig withAwsProfile(String awsProfile) {
        setAwsProfile(awsProfile);
        return this;
    }

    /**
     * Get the KMS VPC endpoint URL for PrivateLink access.
     *
     * When configured, all KMS API calls will be routed through the VPC endpoint
     * instead of the public KMS endpoint, ensuring traffic stays within the VPC.
     *
     * @return the VPC endpoint URL (e.g., "https://vpce-1234567890abcdef0-abcdef01.kms.us-east-1.vpce.amazonaws.com")
     */
    public String getKmsEndpointUrl() {
        return kmsEndpointUrl;
    }

    /**
     * Set the KMS VPC endpoint URL for PrivateLink access.
     *
     * Use this to route KMS API calls through a VPC endpoint for security compliance.
     * The endpoint URL is typically in the format:
     * https://vpce-{id}.kms.{region}.vpce.amazonaws.com
     *
     * @param kmsEndpointUrl the VPC endpoint URL for KMS
     */
    public void setKmsEndpointUrl(String kmsEndpointUrl) {
        this.kmsEndpointUrl = kmsEndpointUrl;
    }

    /**
     * Set KMS VPC endpoint URL and return this instance for chaining.
     *
     * Use this to route KMS API calls through a VPC endpoint (PrivateLink)
     * for security compliance, ensuring traffic does not traverse the public internet.
     *
     * @param kmsEndpointUrl the VPC endpoint URL (e.g., "https://vpce-xxx.kms.us-east-1.vpce.amazonaws.com")
     * @return this instance for method chaining
     */
    public KmsEncryptionConfig withKmsEndpointUrl(String kmsEndpointUrl) {
        setKmsEndpointUrl(kmsEndpointUrl);
        return this;
    }

    /**
     * Validate the configuration
     */
    public void validate() {
        if (kmsKeyId == null || kmsKeyId.trim().isEmpty()) {
            throw new IllegalStateException("KMS key ID must be provided");
        }
    }
}
