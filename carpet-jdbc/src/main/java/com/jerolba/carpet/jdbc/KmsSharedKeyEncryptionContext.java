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

import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

/**
 * Manages a shared Data Encryption Key (DEK) for batch encryption operations.
 *
 * This class generates a single DEK and encrypts it once with KMS, then reuses
 * the same DEK for multiple files. This significantly reduces KMS API calls
 * while maintaining strong security (each file still gets a unique IV).
 *
 * Usage:
 * <pre>
 * try (KmsSharedKeyEncryptionContext context =
 *         new KmsSharedKeyEncryptionContext(kmsConfig)) {
 *
 *     // Export multiple files using the same DEK
 *     for (String table : tables) {
 *         context.createEncryptingOutputStream(new File(table + ".parquet"));
 *     }
 * }
 * </pre>
 */
public class KmsSharedKeyEncryptionContext implements AutoCloseable {

    private final KmsEncryptionConfig config;
    private final KmsClient kmsClient;
    private final SecretKey dataKey;
    private final byte[] encryptedDataKey;
    private volatile boolean closed = false;

    /**
     * Create a shared encryption context with a single DEK
     *
     * @param config KMS encryption configuration
     * @throws Exception if DEK generation or KMS encryption fails
     */
    public KmsSharedKeyEncryptionContext(KmsEncryptionConfig config) throws Exception {
        this.config = config;
        config.validate();

        // Initialize KMS client
        this.kmsClient = createKmsClient(config);

        // Generate a single random DEK for this batch
        this.dataKey = generateDataKey(config.getKeySize());

        // Encrypt the DEK with KMS (done once for the entire batch)
        this.encryptedDataKey = encryptDataKeyWithKms();

        System.out.println("Generated shared DEK for batch encryption (KMS call made once)");
    }

    /**
     * Create KMS client
     */
    private KmsClient createKmsClient(KmsEncryptionConfig config) {
        var builder = KmsClient.builder();

        if (config.getAwsRegion() != null) {
            builder.region(Region.of(config.getAwsRegion()));
        }

        return builder.build();
    }

    /**
     * Generate a random data encryption key
     */
    private SecretKey generateDataKey(int keySize) throws Exception {
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(keySize, new SecureRandom());
        return keyGen.generateKey();
    }

    /**
     * Encrypt the DEK with KMS (called once during initialization)
     */
    private byte[] encryptDataKeyWithKms() throws Exception {
        EncryptRequest encryptRequest = EncryptRequest.builder()
            .keyId(config.getKmsKeyId())
            .plaintext(SdkBytes.fromByteArray(dataKey.getEncoded()))
            .encryptionContext(config.getEncryptionContext())
            .build();

        EncryptResponse encryptResponse = kmsClient.encrypt(encryptRequest);
        return encryptResponse.ciphertextBlob().asByteArray();
    }

    /**
     * Get the shared data encryption key
     */
    public SecretKey getDataKey() {
        if (closed) {
            throw new IllegalStateException("Encryption context has been closed");
        }
        return dataKey;
    }

    /**
     * Get the encrypted data key (base64-encoded for metadata)
     */
    public String getEncryptedDataKeyBase64() {
        if (closed) {
            throw new IllegalStateException("Encryption context has been closed");
        }
        return Base64.getEncoder().encodeToString(encryptedDataKey);
    }

    /**
     * Get the KMS encryption configuration
     */
    public KmsEncryptionConfig getConfig() {
        return config;
    }

    /**
     * Create an encrypting output stream using the shared DEK
     *
     * @param outputFile The file to write encrypted data to
     * @return A new KmsEnvelopeEncryptionOutputStream using the shared DEK
     * @throws Exception if stream creation fails
     */
    public KmsEnvelopeEncryptionOutputStream createEncryptingOutputStream(
            java.io.File outputFile) throws Exception {

        if (closed) {
            throw new IllegalStateException("Encryption context has been closed");
        }

        // Create output stream with shared DEK
        return new KmsEnvelopeEncryptionOutputStream(
            outputFile,
            this  // Pass the shared context
        );
    }

    @Override
    public void close() {
        if (!closed) {
            closed = true;
            if (kmsClient != null) {
                try {
                    kmsClient.close();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }
}
