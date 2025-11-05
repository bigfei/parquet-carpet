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
package com.jerolba.carpet.jdbc.example;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.security.SecureRandom;
import java.util.Base64;

import javax.crypto.Cipher;
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
import software.amazon.awssdk.services.kms.model.EncryptionAlgorithmSpec;

/**
 * EDUCATIONAL EXAMPLE: Hypothetical asymmetric KMS approach for envelope encryption.
 *
 * ⚠️ WARNING: This is NOT how the library works! ⚠️
 *
 * This example demonstrates why we DON'T use asymmetric KMS keys:
 *
 * PROBLEMS with asymmetric approach:
 * 1. ❌ Cannot use GenerateDataKey API (not supported for asymmetric keys)
 * 2. ❌ Requires TWO KMS API calls (Encrypt for DEK + Decrypt for reading)
 * 3. ❌ RSA encryption is 100x SLOWER than AES
 * 4. ❌ Larger encrypted DEK size (256-512 bytes vs 32 bytes)
 * 5. ❌ More complex code with no security benefit
 * 6. ❌ Higher AWS costs (more API calls)
 *
 * CORRECT approach (what we actually use):
 * - Use SYMMETRIC KMS key
 * - Use GenerateDataKey API (single call, returns plaintext + encrypted DEK)
 * - See KmsSharedKeyEncryptionContext and KmsEnvelopeEncryptionOutputStream
 *
 * This example is included for:
 * - Understanding why symmetric is better
 * - Demonstrating the asymmetric alternative
 * - Educational comparison purposes
 */
public class AsymmetricKmsExample {

    private static final int GCM_IV_LENGTH = 12;
    private static final int GCM_TAG_LENGTH = 128;

    public static void main(String[] args) throws Exception {
        // REQUIRES: Asymmetric KMS key (RSA_2048 or RSA_4096)
        // Created with: aws kms create-key --key-spec RSA_2048 --key-usage ENCRYPT_DECRYPT
        String asymmetricKmsKeyId = System.getenv("ASYMMETRIC_KMS_KEY_ID");

        if (asymmetricKmsKeyId == null || asymmetricKmsKeyId.isEmpty()) {
            System.err.println("Set ASYMMETRIC_KMS_KEY_ID environment variable");
            System.err.println("Example: export ASYMMETRIC_KMS_KEY_ID=arn:aws:kms:us-east-1:...:key/...");
            System.exit(1);
        }

        File testFile = new File("test_data.txt");
        File encryptedFile = new File("test_data.txt.encrypted");
        File metadataFile = new File("test_data.txt.metadata");
        File decryptedFile = new File("test_data.txt.decrypted");

        try {
            // Create test data
            try (FileOutputStream fos = new FileOutputStream(testFile)) {
                fos.write("This is test data for asymmetric KMS encryption example.".getBytes());
            }

            System.out.println("=== Asymmetric KMS Envelope Encryption Demo ===\n");

            // Encrypt
            long encryptStart = System.currentTimeMillis();
            encryptFileWithAsymmetricKms(testFile, encryptedFile, metadataFile,
                asymmetricKmsKeyId, "us-east-1");
            long encryptTime = System.currentTimeMillis() - encryptStart;

            // Decrypt
            long decryptStart = System.currentTimeMillis();
            decryptFileWithAsymmetricKms(encryptedFile, metadataFile, decryptedFile,
                asymmetricKmsKeyId, "us-east-1");
            long decryptTime = System.currentTimeMillis() - decryptStart;

            System.out.println("\n=== Performance Metrics ===");
            System.out.println("Encryption time: " + encryptTime + " ms");
            System.out.println("Decryption time: " + decryptTime + " ms");
            System.out.println("Encrypted DEK size: " + metadataFile.length() + " bytes");

            System.out.println("\n=== Why This is Suboptimal ===");
            System.out.println("1. TWO KMS calls needed (Encrypt DEK + Decrypt DEK)");
            System.out.println("2. RSA is 100x slower than symmetric operations");
            System.out.println("3. Larger metadata file (RSA ciphertext is 256+ bytes)");
            System.out.println("4. No GenerateDataKey support - manual DEK generation");
            System.out.println("5. Higher cost ($0.000003 × 2 calls vs 1 call)");

            System.out.println("\n=== Symmetric Approach Benefits ===");
            System.out.println("✓ ONE KMS call (GenerateDataKey returns both versions)");
            System.out.println("✓ AES is 100x faster");
            System.out.println("✓ Smaller metadata (32 bytes encrypted DEK)");
            System.out.println("✓ AWS best practice for envelope encryption");
            System.out.println("✓ Lower cost (50% fewer KMS calls)");

        } finally {
            // Cleanup
            testFile.delete();
            encryptedFile.delete();
            metadataFile.delete();
            decryptedFile.delete();
        }
    }

    /**
     * Step 1-2: Encrypt file using asymmetric KMS key (RSA)
     *
     * Process:
     * 1. Generate random AES-256 DEK locally
     * 2. Encrypt data with DEK (fast AES-GCM)
     * 3. Encrypt DEK with KMS asymmetric key (slow RSA)
     * 4. Store encrypted DEK in metadata
     */
    private static void encryptFileWithAsymmetricKms(
            File inputFile,
            File encryptedFile,
            File metadataFile,
            String asymmetricKmsKeyId,
            String awsRegion) throws Exception {

        System.out.println("Step 1: Generate random DEK locally (NOT from KMS)");
        // Generate AES-256 DEK locally (cannot use GenerateDataKey with asymmetric keys)
        KeyGenerator keyGen = KeyGenerator.getInstance("AES");
        keyGen.init(256, new SecureRandom());
        SecretKey dataKey = keyGen.generateKey();
        byte[] dekBytes = dataKey.getEncoded();
        System.out.println("  ✓ Generated " + dekBytes.length + " byte AES key locally");

        System.out.println("\nStep 2: Encrypt DEK with asymmetric KMS key (RSA)");
        // Encrypt DEK using KMS asymmetric key (RSA - SLOW!)
        try (KmsClient kmsClient = KmsClient.builder()
                .region(Region.of(awsRegion))
                .build()) {

            // Must specify encryption algorithm for asymmetric keys
            EncryptRequest encryptRequest = EncryptRequest.builder()
                .keyId(asymmetricKmsKeyId)
                .plaintext(SdkBytes.fromByteArray(dekBytes))
                .encryptionAlgorithm(EncryptionAlgorithmSpec.RSAES_OAEP_SHA_256) // ← Required for asymmetric
                .build();

            EncryptResponse encryptResponse = kmsClient.encrypt(encryptRequest);
            byte[] encryptedDek = encryptResponse.ciphertextBlob().asByteArray();
            System.out.println("  ✓ Encrypted DEK size: " + encryptedDek.length + " bytes (RSA ciphertext)");
            System.out.println("  ⚠ Compare to symmetric: 32 bytes (AES ciphertext)");

            // Store encrypted DEK
            String encryptedDekBase64 = Base64.getEncoder().encodeToString(encryptedDek);
            try (FileOutputStream fos = new FileOutputStream(metadataFile)) {
                fos.write(encryptedDekBase64.getBytes());
            }
        }

        System.out.println("\nStep 3: Encrypt data with DEK (AES-GCM)");
        // Generate random IV
        byte[] iv = new byte[GCM_IV_LENGTH];
        new SecureRandom().nextBytes(iv);

        // Encrypt data with AES-GCM
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
        cipher.init(Cipher.ENCRYPT_MODE, dataKey, spec);

        try (FileInputStream fis = new FileInputStream(inputFile);
             FileOutputStream fos = new FileOutputStream(encryptedFile)) {

            // Write IV first
            fos.write(iv);

            // Encrypt and write data
            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] encrypted = cipher.update(buffer, 0, bytesRead);
                if (encrypted != null) {
                    fos.write(encrypted);
                }
            }
            byte[] finalBytes = cipher.doFinal();
            if (finalBytes != null) {
                fos.write(finalBytes);
            }
        }
        System.out.println("  ✓ Data encrypted with AES-GCM");
    }

    /**
     * Step 3-4: Decrypt file using asymmetric KMS key
     *
     * Process:
     * 1. Read encrypted DEK from metadata
     * 2. Decrypt DEK with KMS asymmetric key (slow RSA)
     * 3. Decrypt data with DEK (fast AES-GCM)
     */
    private static void decryptFileWithAsymmetricKms(
            File encryptedFile,
            File metadataFile,
            File outputFile,
            String asymmetricKmsKeyId,
            String awsRegion) throws Exception {

        System.out.println("\nStep 4: Read encrypted DEK from metadata");
        // Read encrypted DEK
        byte[] metadataBytes = new byte[(int) metadataFile.length()];
        try (FileInputStream fis = new FileInputStream(metadataFile)) {
            fis.read(metadataBytes);
        }
        String encryptedDekBase64 = new String(metadataBytes);
        byte[] encryptedDek = Base64.getDecoder().decode(encryptedDekBase64);
        System.out.println("  ✓ Read " + encryptedDek.length + " byte encrypted DEK");

        System.out.println("\nStep 5: Decrypt DEK with asymmetric KMS key");
        // Decrypt DEK using KMS asymmetric key (RSA - SLOW!)
        SecretKey dataKey;
        try (KmsClient kmsClient = KmsClient.builder()
                .region(Region.of(awsRegion))
                .build()) {

            // Must specify encryption algorithm for asymmetric keys
            DecryptRequest decryptRequest = DecryptRequest.builder()
                .keyId(asymmetricKmsKeyId)
                .ciphertextBlob(SdkBytes.fromByteArray(encryptedDek))
                .encryptionAlgorithm(EncryptionAlgorithmSpec.RSAES_OAEP_SHA_256) // ← Required for asymmetric
                .build();

            DecryptResponse decryptResponse = kmsClient.decrypt(decryptRequest);
            byte[] dekBytes = decryptResponse.plaintext().asByteArray();
            dataKey = new SecretKeySpec(dekBytes, "AES");
            System.out.println("  ✓ Decrypted DEK with RSA");
        }

        System.out.println("\nStep 6: Decrypt data with DEK (AES-GCM)");
        // Read IV and decrypt data
        try (FileInputStream fis = new FileInputStream(encryptedFile);
             FileOutputStream fos = new FileOutputStream(outputFile)) {

            // Read IV
            byte[] iv = new byte[GCM_IV_LENGTH];
            fis.read(iv);

            // Decrypt data
            Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
            GCMParameterSpec spec = new GCMParameterSpec(GCM_TAG_LENGTH, iv);
            cipher.init(Cipher.DECRYPT_MODE, dataKey, spec);

            byte[] buffer = new byte[8192];
            int bytesRead;
            while ((bytesRead = fis.read(buffer)) != -1) {
                byte[] decrypted = cipher.update(buffer, 0, bytesRead);
                if (decrypted != null) {
                    fos.write(decrypted);
                }
            }
            byte[] finalBytes = cipher.doFinal();
            if (finalBytes != null) {
                fos.write(finalBytes);
            }
        }
        System.out.println("  ✓ Data decrypted successfully");
    }

    /**
     * COMPARISON TABLE
     * ================
     *
     * Feature                  | Symmetric (What we use) | Asymmetric (This example)
     * -------------------------|-------------------------|---------------------------
     * KMS API                  | GenerateDataKey         | Encrypt (manual DEK)
     * KMS calls for encrypt    | 1                       | 1 (but after local gen)
     * KMS calls for decrypt    | 1                       | 1
     * Total KMS calls          | 1                       | 2 (gen locally + encrypt)
     * DEK generation           | In AWS HSM              | Local (SecureRandom)
     * Encryption speed         | Fast (AES only)         | Slow (RSA bottleneck)
     * Encrypted DEK size       | 32 bytes                | 256-512 bytes
     * Supports GenerateDataKey | ✅ Yes                  | ❌ No
     * Batch mode benefit       | ✅ 90% reduction        | ❌ No benefit
     * AWS Best Practice        | ✅ Yes                  | ❌ No
     * Use case                 | Envelope encryption     | Cross-boundary encryption
     *
     * WHEN TO USE ASYMMETRIC KMS KEYS:
     * ================================
     *
     * Use Case 1: External Party Encryption
     * - Partner outside AWS encrypts data with your KMS public key
     * - You decrypt inside AWS with private key
     * - Public key can be shared, private key stays in AWS
     *
     * Use Case 2: Digital Signatures
     * - Sign data with private key (proves authenticity)
     * - Verify with public key (anyone can verify)
     * - Non-repudiation (can't deny signing)
     *
     * Use Case 3: PKI Integration
     * - Integration with existing PKI infrastructure
     * - Certificate-based workflows
     * - Hybrid cloud encryption scenarios
     *
     * NOT for: Envelope encryption of files (use symmetric keys instead!)
     */
}
