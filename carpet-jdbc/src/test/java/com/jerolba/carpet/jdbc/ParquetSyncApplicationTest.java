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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import org.junit.jupiter.api.Test;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

class ParquetSyncApplicationTest {

    @Test
    void appendDateNormalizesSlash() throws Exception {
        assertEquals("base/20260203/", invokeStatic("appendDate", "base", "20260203"));
        assertEquals("base/20260203/", invokeStatic("appendDate", "base/", "20260203"));
    }

    @Test
    void parseRemoteLocationVariants() throws Exception {
        Object location = invokeStatic("parse", "s3://bucket/prefix/path");
        assertEquals("bucket", invokeRecordAccessor(location, "bucket"));
        assertEquals("prefix/path", invokeRecordAccessor(location, "prefix"));

        Object location2 = invokeStatic("parse", "remote:bucket/prefix");
        assertEquals("bucket", invokeRecordAccessor(location2, "bucket"));
        assertEquals("prefix", invokeRecordAccessor(location2, "prefix"));

        Object location3 = invokeStatic("parse", "remote:/bucket/prefix");
        assertEquals("bucket", invokeRecordAccessor(location3, "bucket"));
        assertEquals("prefix", invokeRecordAccessor(location3, "prefix"));

        Object location4 = invokeStatic("parse", "bucket");
        assertEquals("bucket", invokeRecordAccessor(location4, "bucket"));
        assertEquals("", invokeRecordAccessor(location4, "prefix"));

        Object location5 = invokeStatic("parse", "s3://bucket");
        assertEquals("bucket", invokeRecordAccessor(location5, "bucket"));
        assertEquals("", invokeRecordAccessor(location5, "prefix"));
    }

    @Test
    void parseRejectsBlankLocations() {
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () ->
            invokeStatic("parse", "   "));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void parseRejectsMissingBucket() {
        InvocationTargetException ex = assertThrows(InvocationTargetException.class, () ->
            invokeStatic("parse", "remote:/"));
        assertTrue(ex.getCause() instanceof IllegalArgumentException);
    }

    @Test
    void resolveKmsKeyArnUsesArnOrBuildsFromId() throws Exception {
        Region region = Region.of("ap-southeast-1");
        String arn = "arn:aws:kms:us-east-1:123456789012:key/abc";
        assertEquals(arn, invokeStatic("resolveKmsKeyArn", arn, "123456789012", region));

        String derived = (String) invokeStatic("resolveKmsKeyArn", "my-key", "123456789012", region);
        assertEquals("arn:aws:kms:ap-southeast-1:123456789012:key/my-key", derived);

        String empty = (String) invokeStatic("resolveKmsKeyArn", "", "123456789012", region);
        assertEquals(null, empty);

        String nullKey = (String) invokeStaticWithTypes(
            "resolveKmsKeyArn",
            new Class<?>[] { String.class, String.class, Region.class },
            new Object[] { null, "123456789012", region }
        );
        assertEquals(null, nullKey);
    }

    @Test
    void resolveRegionUsesArnWhenEnvNotSet() throws Exception {
        String envRegion = System.getenv("AWS_REGION");
        Region region = (Region) invokeStatic("resolveRegion", "arn:aws:kms:us-west-2:111122223333:key/abc");
        if (envRegion != null && !envRegion.isBlank()) {
            assertEquals(envRegion, region.id());
        } else {
            assertEquals("us-west-2", region.id());
        }
    }

    @Test
    void resolveRegionDefaultsWhenNoArn() throws Exception {
        String envRegion = System.getenv("AWS_REGION");
        Region region = (Region) invokeStatic("resolveRegion", "my-key");
        if (envRegion != null && !envRegion.isBlank()) {
            assertEquals(envRegion, region.id());
        } else {
            assertEquals("ap-southeast-1", region.id());
        }
    }

    @Test
    void envOrDefaultFallsBack() throws Exception {
        String value = (String) invokeStatic("envOrDefault", "PARQUET_SYNC_TEST_ENV_NOT_SET", "fallback");
        assertEquals("fallback", value);
    }

    @Test
    void envOrDefaultUsesEnvValue() throws Exception {
        String pathValue = System.getenv("PATH");
        String result = (String) invokeStatic("envOrDefault", "PATH", "fallback");
        assertEquals(pathValue, result);
    }

    @Test
    void buildClientAcceptsEndpointOverride() throws Exception {
        S3Client client = (S3Client) invokeStatic(
            "buildClient",
            Region.of("ap-southeast-1"),
            "http://localhost:9000"
        );
        assertTrue(client != null);
        client.close();
    }

    private static Object invokeStatic(String method, Object... args) throws Exception {
        Class<?>[] types = new Class<?>[args.length];
        for (int i = 0; i < args.length; i++) {
            types[i] = args[i].getClass();
        }
        Method target = ParquetSyncApplication.class.getDeclaredMethod(method, types);
        target.setAccessible(true);
        return target.invoke(null, args);
    }

    private static Object invokeStaticWithTypes(String method, Class<?>[] types, Object[] args) throws Exception {
        Method target = ParquetSyncApplication.class.getDeclaredMethod(method, types);
        target.setAccessible(true);
        return target.invoke(null, args);
    }

    private static String invokeRecordAccessor(Object instance, String method) throws Exception {
        Method target = instance.getClass().getDeclaredMethod(method);
        target.setAccessible(true);
        return (String) target.invoke(instance);
    }
}
