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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.Test;

class KmsEncryptionConfigTest {

    @Test
    void validateRequiresKeyId() {
        KmsEncryptionConfig config = new KmsEncryptionConfig();
        assertThrows(IllegalStateException.class, config::validate);
    }

    @Test
    void setKeySizeRejectsInvalidValues() {
        KmsEncryptionConfig config = new KmsEncryptionConfig();
        assertThrows(IllegalArgumentException.class, () -> config.setKeySize(192));
    }

    @Test
    void encryptionContextEntryIsStored() {
        KmsEncryptionConfig config = new KmsEncryptionConfig()
            .withKmsKeyId("alias/test-key")
            .withEncryptionContextEntry("env", "test");

        assertEquals("test", config.getEncryptionContext().get("env"));
    }

    @Test
    void withMethodsSetAllFields() {
        Map<String, String> context = new HashMap<>();
        context.put("team", "analytics");

        KmsEncryptionConfig config = KmsEncryptionConfig.builder()
            .withKmsKeyId("alias/my-key")
            .withEncryptionContext(context)
            .withAlgorithm("AES/GCM/NoPadding")
            .withKeySize(128)
            .withAwsRegion("us-east-1")
            .withAwsProfile("uat2")
            .withKmsEndpointUrl("https://vpce-123.kms.us-east-1.vpce.amazonaws.com");

        assertEquals("alias/my-key", config.getKmsKeyId());
        assertEquals("analytics", config.getEncryptionContext().get("team"));
        assertEquals("AES/GCM/NoPadding", config.getAlgorithm());
        assertEquals(128, config.getKeySize());
        assertEquals("us-east-1", config.getAwsRegion());
        assertEquals("uat2", config.getAwsProfile());
        assertEquals("https://vpce-123.kms.us-east-1.vpce.amazonaws.com", config.getKmsEndpointUrl());
    }

    @Test
    void validateSucceedsWithKeyId() {
        KmsEncryptionConfig config = new KmsEncryptionConfig("alias/test-key");
        config.validate();
        assertTrue(config.getKmsKeyId().startsWith("alias/"));
    }
}
