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

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

import com.jerolba.carpet.CarpetReader;

import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3ClientBuilder;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.DeleteObjectRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;

class ParquetSyncServiceTest {

    @Test
    void copiesParquetFilesAndCreatesMarker() throws IOException {
        InMemoryObjectStorage source = new InMemoryObjectStorage("20251015/");
        source.add("nested/report-a.parquet", "parquet-a".getBytes());
        source.add("nested/report-b.parquet.metadata", "metadata-b".getBytes());
        source.add("nested/report-c.csv", "should-be-ignored".getBytes());
        source.add("report-d.txt", "also-ignore".getBytes());

        InMemoryObjectStorage destination = new InMemoryObjectStorage("20251015/");

        ParquetSyncService service = new ParquetSyncService(source, destination);
        ParquetSyncService.SyncResult result = service.sync();

        assertEquals(2, result.filesCopied(), "Only parquet and parquet.metadata files are copied");
        assertEquals(
            Set.of("report-a.parquet", "report-b.parquet.metadata"),
            Set.copyOf(result.copiedFileNames()),
            "Destination should receive flattened file names");

        assertTrue(destination.contains("report-a.parquet"));
        assertTrue(destination.contains("report-b.parquet.metadata"));
        assertTrue(destination.contains("SUCCESS.txt"));

        assertArrayEquals("parquet-a".getBytes(), destination.readBytes("report-a.parquet"));
        assertArrayEquals("metadata-b".getBytes(), destination.readBytes("report-b.parquet.metadata"));
        assertEquals(0, destination.readBytes("SUCCESS.txt").length, "SUCCESS marker should be empty");

        assertFalse(destination.contains("report-c.csv"), "Non parquet files are not copied");
    }

    @Test
    void integrationCopiesParquetFromObsToAws(@TempDir Path tempDir) throws Exception {
        assumeTrue(DockerClientFactory.instance().isDockerAvailable(), "Docker is required to generate a real parquet file");

        String srcBucket = System.getenv("PARQUET_SYNC_IT_SRC_BUCKET");
        String dstBucket = System.getenv("PARQUET_SYNC_IT_DST_BUCKET");
        assumeTrue(srcBucket != null && !srcBucket.isBlank()
                && dstBucket != null && !dstBucket.isBlank(),
            "Set PARQUET_SYNC_IT_SRC_BUCKET and PARQUET_SYNC_IT_DST_BUCKET to run the S3 integration test");

        String regionId = System.getenv().getOrDefault("PARQUET_SYNC_IT_REGION", "ap-southeast-1");
        Region region = Region.of(regionId);
        String srcEndpoint = System.getenv("PARQUET_SYNC_IT_SRC_ENDPOINT");
        String dstEndpoint = System.getenv("PARQUET_SYNC_IT_DST_ENDPOINT");
        String srcPrefixBase = ensureTrailingSlash(System.getenv().getOrDefault("PARQUET_SYNC_IT_SRC_PREFIX", "carpet-sync/source"));
        String dstPrefixBase = ensureTrailingSlash(System.getenv().getOrDefault("PARQUET_SYNC_IT_DST_PREFIX", "carpet-sync/destination"));

        String runPrefix = "it-" + UUID.randomUUID();
        String srcPrefix = srcPrefixBase + runPrefix + "/";
        String dstPrefix = dstPrefixBase + runPrefix + "/";

        Path parquetFile = tempDir.resolve("orders.parquet");
        long exportedRows = exportOrdersParquet(parquetFile);
        assumeTrue(Files.exists(parquetFile) && Files.size(parquetFile) > 0, "Parquet export failed");

        S3Client sourceClient = buildS3Client(region, srcEndpoint);
        S3Client destinationClient = buildS3Client(region, dstEndpoint);

        String sourceObjectKey = srcPrefix + "nested/orders.parquet";
        String ignoredObjectKey = srcPrefix + "ignored.txt";

        try {
            sourceClient.putObject(
                PutObjectRequest.builder().bucket(srcBucket).key(sourceObjectKey).build(),
                RequestBody.fromFile(parquetFile));
            sourceClient.putObject(
                PutObjectRequest.builder().bucket(srcBucket).key(ignoredObjectKey).build(),
                RequestBody.fromString("ignore me"));

            ParquetSyncService.Storage source = new ParquetSyncService.S3Storage(sourceClient, srcBucket, srcPrefix);
            ParquetSyncService.Storage destination = new ParquetSyncService.S3Storage(destinationClient, dstBucket, dstPrefix);

            ParquetSyncService.SyncResult result = new ParquetSyncService(source, destination).sync();

            assertEquals(1, result.filesCopied(), "Only parquet files should be copied");
            assertTrue(result.copiedFileNames().contains("orders.parquet"));
            assertTrue(objectExists(destinationClient, dstBucket, dstPrefix + "orders.parquet"));
            assertTrue(objectExists(destinationClient, dstBucket, dstPrefix + "SUCCESS.txt"));

            Path downloadedParquet = tempDir.resolve("orders-from-dst.parquet");
            try (InputStream downloaded = destination.read("orders.parquet")) {
                Files.copy(downloaded, downloadedParquet, StandardCopyOption.REPLACE_EXISTING);
            }
            List<Map<String, Object>> records = readParquet(downloadedParquet.toFile());
            assertEquals(exportedRows, records.size(), "Copied parquet should preserve row count");
            assertEquals(1L, ((Number) records.get(0).get("id")).longValue(), "First row should survive round-trip");
        } finally {
            deleteQuietly(destinationClient, dstBucket, dstPrefix + "orders.parquet");
            deleteQuietly(destinationClient, dstBucket, dstPrefix + "SUCCESS.txt");
            deleteQuietly(sourceClient, srcBucket, sourceObjectKey);
            deleteQuietly(sourceClient, srcBucket, ignoredObjectKey);
            sourceClient.close();
            destinationClient.close();
        }
    }

    private long exportOrdersParquet(Path parquetFile) throws SQLException, IOException {
        @SuppressWarnings("resource") // handled manually
        PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>(DockerImageName.parse("postgres:15-alpine"))
            .withDatabaseName("syncdb")
            .withUsername("syncuser")
            .withPassword("syncpass");
        postgres.start();

        try (Connection connection = DriverManager.getConnection(
                postgres.getJdbcUrl(),
                postgres.getUsername(),
                postgres.getPassword());
             Statement statement = connection.createStatement()) {

            statement.execute("CREATE TABLE orders (id SERIAL PRIMARY KEY, customer_name TEXT, total_amount NUMERIC)");
            statement.execute("""
                INSERT INTO orders (customer_name, total_amount) VALUES
                ('Alice', 120.50),
                ('Bob', 80.00),
                ('Carol', 45.25)
                """);

            return DynamicJdbcExporter.exportResultSetToParquet(
                connection,
                "SELECT id, customer_name, total_amount FROM orders ORDER BY id",
                parquetFile.toFile());
        } finally {
            postgres.stop();
        }
    }

    private S3Client buildS3Client(Region region, String endpointOverride) {
        S3ClientBuilder builder = S3Client.builder()
            .region(region)
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());

        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(java.net.URI.create(endpointOverride));
        }

        return builder.build();
    }

    private boolean objectExists(S3Client client, String bucket, String key) {
        return client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(key)
                .build())
            .contents()
            .stream()
            .map(S3Object::key)
            .anyMatch(key::equals);
    }

    private void deleteQuietly(S3Client client, String bucket, String key) {
        try {
            client.deleteObject(DeleteObjectRequest.builder().bucket(bucket).key(key).build());
        } catch (Exception ignored) {
            // best-effort cleanup
        }
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquet(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }

    private String ensureTrailingSlash(String prefix) {
        if (prefix == null || prefix.isBlank()) {
            return "";
        }
        return prefix.endsWith("/") ? prefix : prefix + "/";
    }

    private static class InMemoryObjectStorage implements ParquetSyncService.Storage {
        private final String prefix;
        private final Map<String, byte[]> objects = new HashMap<>();

        InMemoryObjectStorage(String prefix) {
            this.prefix = prefix.endsWith("/") ? prefix : prefix + "/";
        }

        void add(String relativeKey, byte[] content) {
            objects.put(prefix + relativeKey, content);
        }

        boolean contains(String key) {
            return objects.containsKey(prefix + key);
        }

        byte[] readBytes(String key) {
            return objects.get(prefix + key);
        }

        @Override
        public List<ParquetSyncService.Item> list() {
            List<ParquetSyncService.Item> list = new ArrayList<>();
            for (Map.Entry<String, byte[]> entry : objects.entrySet()) {
                if (entry.getKey().startsWith(prefix)) {
                    list.add(new ParquetSyncService.Item(entry.getKey().substring(prefix.length()), entry.getValue().length));
                }
            }
            return list;
        }

        @Override
        public InputStream read(String key) {
            byte[] data = objects.get(prefix + key);
            if (data == null) {
                throw new IllegalArgumentException("Missing key: " + key);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public void write(String key, InputStream content, long size) throws IOException {
            objects.put(prefix + key, content.readAllBytes());
        }

        @Override
        public void writeEmpty(String key) {
            objects.put(prefix + key, new byte[0]);
        }
    }
}
