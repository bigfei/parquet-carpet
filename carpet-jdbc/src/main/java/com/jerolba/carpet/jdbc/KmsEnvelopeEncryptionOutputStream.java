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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.CipherOutputStream;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import software.amazon.awssdk.core.SdkBytes;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.kms.KmsClient;
import software.amazon.awssdk.services.kms.model.DecryptRequest;
import software.amazon.awssdk.services.kms.model.DecryptResponse;
import software.amazon.awssdk.services.kms.model.EncryptRequest;
import software.amazon.awssdk.services.kms.model.EncryptResponse;

/**
 * OutputStream wrapper that provides AWS KMS envelope encryption for Parquet files.
 *
 * This implementation:
 * 1. Generates a random Data Encryption Key (DEK) using AES-256
 * 2. Encrypts the Parquet data with the DEK using AES-GCM
 * 3. Encrypts the DEK with AWS KMS
 * 4. Writes the encrypted DEK metadata to a companion .metadata file
 *
 * The encrypted file can be decrypted by:
 * 1. Reading the .metadata file to get the encrypted DEK and IV
 * 2. Decrypting the DEK using AWS KMS
 * 3. Decrypting the file using the DEK and IV
 */
public class KmsEnvelopeEncryptionOutputStream extends OutputStream {

    private static final int GCM_IV_LENGTH = 12; // 96 bits
    private static final int GCM_TAG_LENGTH = 128; // 128 bits

    private final CipherOutputStream cipherOutputStream;
    private final FileOutputStream metadataOutputStream;
    private final File metadataFile;
    private final KmsClient kmsClient;
    private final KmsEncryptionConfig config;
    private final byte[] iv;
    private final SecretKey dataKey;
    private final String encryptedDataKeyBase64;
    private final boolean ownedKmsClient;  // Track if we own the KMS client
    private boolean closed = false;

    /**
     * Create an encrypting output stream (generates new DEK and makes KMS call)
     *
     * @param outputFile The file to write encrypted data to
     * @param config KMS encryption configuration
     * @throws IOException if file operations fail
     */
    public KmsEnvelopeEncryptionOutputStream(File outputFile, KmsEncryptionConfig config)
            throws IOException {
        this.config = config;
        config.validate();

        // Initialize KMS client (we own it)
        this.kmsClient = createKmsClient(config);
        this.ownedKmsClient = true;

        try {
            // Generate random data encryption key (DEK)
            this.dataKey = generateDataKey(config.getKeySize());

            // Generate random IV for GCM mode
            this.iv = generateIV();

            // Encrypt the DEK with KMS
            this.encryptedDataKeyBase64 = encryptDataKeyWithKms(dataKey);

            // Create cipher for AES-GCM encryption
            Cipher cipher = createCipher(dataKey, iv);

            // Create encrypted output stream
            FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
            this.cipherOutputStream = new CipherOutputStream(fileOutputStream, cipher);

            // Create metadata file for storing encrypted DEK
            this.metadataFile = new File(outputFile.getAbsolutePath() + ".metadata");
            this.metadataOutputStream = new FileOutputStream(metadataFile);

            // Write metadata
            writeEncryptedMetadata();

        } catch (Exception e) {
            cleanup();
            throw new IOException("Failed to initialize KMS envelope encryption", e);
        }
    }

    /**
     * Create an encrypting output stream using a shared DEK (no KMS call)
     *
     * This constructor reuses a DEK from a shared encryption context,
     * avoiding KMS API calls for batch operations.
     *
     * @param outputFile The file to write encrypted data to
     * @param sharedContext Shared encryption context with pre-encrypted DEK
     * @throws IOException if file operations fail
     */
    public KmsEnvelopeEncryptionOutputStream(File outputFile,
                                            KmsSharedKeyEncryptionContext sharedContext)
            throws IOException {
        this.config = sharedContext.getConfig();
        this.kmsClient = null;  // No KMS client needed
        this.ownedKmsClient = false;

        try {
            // Reuse the shared DEK (no new generation needed)
            this.dataKey = sharedContext.getDataKey();

            // Generate unique IV for this file (critical for security)
            this.iv = generateIV();

            // Reuse the pre-encrypted DEK (no KMS call needed)
            this.encryptedDataKeyBase64 = sharedContext.getEncryptedDataKeyBase64();

            // Create cipher for AES-GCM encryption
            Cipher cipher = createCipher(dataKey, iv);

            // Create encrypted output stream
            FileOutputStream fileOutputStream = new FileOutputStream(outputFile);
            this.cipherOutputStream = new CipherOutputStream(fileOutputStream, cipher);

            // Create metadata file for storing encrypted DEK
            this.metadataFile = new File(outputFile.getAbsolutePath() + ".metadata");
            this.metadataOutputStream = new FileOutputStream(metadataFile);

            // Write metadata
            writeEncryptedMetadata();

        } catch (Exception e) {
            cleanup();
            throw new IOException("Failed to initialize KMS envelope encryption with shared key", e);
        }
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
     * Generate random initialization vector for GCM
     */
    private byte[] generateIV() {
        byte[] iv = new byte[GCM_IV_LENGTH];
        new SecureRandom().nextBytes(iv);
        return iv;
    }

    /**
     * Create cipher for AES-GCM encryption
     */
    private Cipher createCipher(SecretKey key, byte[] iv) throws Exception {
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.ENCRYPT_MODE, key, parameterSpec);
        return cipher;
    }

    /**
     * Encrypt the DEK with KMS (called once per DEK)
     */
    private String encryptDataKeyWithKms(SecretKey key) throws Exception {
        EncryptRequest encryptRequest = EncryptRequest.builder()
            .keyId(config.getKmsKeyId())
            .plaintext(SdkBytes.fromByteArray(key.getEncoded()))
            .encryptionContext(config.getEncryptionContext())
            .build();

        EncryptResponse encryptResponse = kmsClient.encrypt(encryptRequest);
        byte[] encryptedKey = encryptResponse.ciphertextBlob().asByteArray();
        return Base64.getEncoder().encodeToString(encryptedKey);
    }

    /**
     * Write metadata file with encryption information
     */
    private void writeEncryptedMetadata() throws IOException {
        try {
            // Write metadata in JSON format
            StringBuilder metadata = new StringBuilder();
            metadata.append("{\n");
            metadata.append("  \"version\": \"1.0\",\n");
            metadata.append("  \"algorithm\": \"").append(config.getAlgorithm()).append("\",\n");
            metadata.append("  \"keySize\": ").append(config.getKeySize()).append(",\n");
            metadata.append("  \"kmsKeyId\": \"").append(config.getKmsKeyId()).append("\",\n");
            metadata.append("  \"encryptedDataKey\": \"")
                .append(encryptedDataKeyBase64).append("\",\n");
            metadata.append("  \"iv\": \"")
                .append(Base64.getEncoder().encodeToString(iv)).append("\",\n");

            // Add encryption context if present
            if (!config.getEncryptionContext().isEmpty()) {
                metadata.append("  \"encryptionContext\": {\n");
                boolean first = true;
                for (var entry : config.getEncryptionContext().entrySet()) {
                    if (!first) {
                        metadata.append(",\n");
                    }
                    metadata.append("    \"").append(entry.getKey()).append("\": \"")
                        .append(entry.getValue()).append("\"");
                    first = false;
                }
                metadata.append("\n  },\n");
            }

            metadata.append("  \"timestamp\": \"").append(System.currentTimeMillis()).append("\"\n");
            metadata.append("}\n");

            metadataOutputStream.write(metadata.toString().getBytes(StandardCharsets.UTF_8));
            metadataOutputStream.close();

        } catch (Exception e) {
            throw new IOException("Failed to encrypt data key with KMS", e);
        }
    }

    @Override
    public void write(int b) throws IOException {
        cipherOutputStream.write(b);
    }

    @Override
    public void write(byte[] b) throws IOException {
        cipherOutputStream.write(b);
    }

    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        cipherOutputStream.write(b, off, len);
    }

    @Override
    public void flush() throws IOException {
        cipherOutputStream.flush();
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            closed = true;
            try {
                cipherOutputStream.close();
            } finally {
                cleanup();
            }
        }
    }

    /**
     * Clean up resources
     */
    private void cleanup() {
        // Only close KMS client if we own it (not shared from context)
        if (ownedKmsClient && kmsClient != null) {
            try {
                kmsClient.close();
            } catch (Exception e) {
                // Ignore cleanup errors
            }
        }
    }

    /**
     * Get the metadata file
     */
    public File getMetadataFile() {
        return metadataFile;
    }

    /**
     * Utility method to decrypt a file encrypted with this class
     *
     * @param encryptedFile The encrypted file
     * @param metadataFile The metadata file containing encryption info
     * @param outputFile The file to write decrypted data to
     * @param config KMS configuration (must have same KMS key and encryption context)
     * @throws IOException if file operations or decryption fails
     */
    public static void decryptFile(File encryptedFile, File metadataFile,
                                   File outputFile, KmsEncryptionConfig config)
            throws IOException {

        if (!encryptedFile.exists()) {
            throw new IOException("Encrypted file not found: " + encryptedFile.getAbsolutePath());
        }
        if (!metadataFile.exists()) {
            throw new IOException("Metadata file not found: " + metadataFile.getAbsolutePath());
        }

        KmsClient kmsClient = null;
        try {
            // 1. Read and parse metadata file
            String metadataJson = new String(
                java.nio.file.Files.readAllBytes(metadataFile.toPath()),
                StandardCharsets.UTF_8
            );

            // Simple JSON parsing (extract values between quotes)
            String encryptedKeyBase64 = extractJsonValue(metadataJson, "encryptedDataKey");
            String ivBase64 = extractJsonValue(metadataJson, "iv");
            String kmsKeyId = extractJsonValue(metadataJson, "kmsKeyId");

            if (encryptedKeyBase64 == null || ivBase64 == null) {
                throw new IOException("Invalid metadata file format: missing required fields");
            }

            byte[] encryptedKey = Base64.getDecoder().decode(encryptedKeyBase64);
            byte[] iv = Base64.getDecoder().decode(ivBase64);

            // 2. Initialize KMS client
            var builder = KmsClient.builder();
            if (config.getAwsRegion() != null) {
                builder.region(Region.of(config.getAwsRegion()));
            }
            kmsClient = builder.build();

            // 3. Decrypt the DEK using KMS
            DecryptRequest decryptRequest = DecryptRequest.builder()
                .ciphertextBlob(SdkBytes.fromByteArray(encryptedKey))
                .keyId(config.getKmsKeyId() != null ? config.getKmsKeyId() : kmsKeyId)
                .encryptionContext(config.getEncryptionContext())
                .build();

            DecryptResponse decryptResponse = kmsClient.decrypt(decryptRequest);
            byte[] dekBytes = decryptResponse.plaintext().asByteArray();
            SecretKey dek = new SecretKeySpec(dekBytes, "AES");

            // 4. Create cipher for decryption
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec parameterSpec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, dek, parameterSpec);

            // 5. Decrypt the file
            try (java.io.FileInputStream fis = new java.io.FileInputStream(encryptedFile);
                 javax.crypto.CipherInputStream cis = new javax.crypto.CipherInputStream(fis, cipher);
                 java.io.FileOutputStream fos = new java.io.FileOutputStream(outputFile)) {

                byte[] buffer = new byte[8192];
                int bytesRead;
                while ((bytesRead = cis.read(buffer)) != -1) {
                    fos.write(buffer, 0, bytesRead);
                }
            }

            System.out.println("Successfully decrypted file to: " + outputFile.getAbsolutePath());

        } catch (Exception e) {
            throw new IOException("Failed to decrypt file: " + e.getMessage(), e);
        } finally {
            if (kmsClient != null) {
                try {
                    kmsClient.close();
                } catch (Exception e) {
                    // Ignore cleanup errors
                }
            }
        }
    }

    /**
     * Simple JSON value extractor (avoids dependency on JSON library)
     * Extracts value for a given key from JSON string
     */
    private static String extractJsonValue(String json, String key) {
        String searchPattern = "\"" + key + "\"";
        int keyIndex = json.indexOf(searchPattern);
        if (keyIndex == -1) {
            return null;
        }

        // Find the colon after the key
        int colonIndex = json.indexOf(":", keyIndex);
        if (colonIndex == -1) {
            return null;
        }

        // Find the opening quote of the value
        int valueStart = json.indexOf("\"", colonIndex);
        if (valueStart == -1) {
            return null;
        }
        valueStart++; // Move past the opening quote

        // Find the closing quote of the value
        int valueEnd = json.indexOf("\"", valueStart);
        if (valueEnd == -1) {
            return null;
        }

        return json.substring(valueStart, valueEnd);
    }
}
