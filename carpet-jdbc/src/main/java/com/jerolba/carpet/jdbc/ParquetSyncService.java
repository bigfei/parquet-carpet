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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.core.ResponseInputStream;

/**
 * Copies parquet files from a source location to a destination, flattening nested directories
 * and writing a SUCCESS.txt marker. Uses S3-compatible APIs so it works with Huawei OBS -> AWS S3.
 */
public class ParquetSyncService {

    private final Storage source;
    private final Storage destination;

    public ParquetSyncService(Storage source, Storage destination) {
        this.source = source;
        this.destination = destination;
    }

    public SyncResult sync() throws IOException {
        List<String> copied = new ArrayList<>();

        for (Item object : source.list()) {
            if (!isParquet(object.key())) {
                continue;
            }
            String fileName = Paths.get(object.key()).getFileName().toString();
            try (InputStream content = source.read(object.key())) {
                destination.write(fileName, content, object.size());
            }
            copied.add(fileName);
        }

        destination.writeEmpty("SUCCESS.txt");
        return new SyncResult(copied.size(), List.copyOf(copied));
    }

    private boolean isParquet(String key) {
        return key.endsWith(".parquet") || key.endsWith(".parquet.metadata");
    }

    public record SyncResult(int filesCopied, List<String> copiedFileNames) {
    }

    public interface Storage {
        List<Item> list() throws IOException;

        InputStream read(String key) throws IOException;

        void write(String key, InputStream content, long size) throws IOException;

        void writeEmpty(String key) throws IOException;
    }

    public record Item(String key, long size) {
    }

    /**
     * S3-backed storage that can point to OBS (source) or AWS S3 (destination).
     */
    public static class S3Storage implements Storage {
        private final S3Client client;
        private final String bucket;
        private final String prefix;
        private final String kmsKeyArn;

        public S3Storage(S3Client client, String bucket, String prefix) {
            this(client, bucket, prefix, null);
        }

        public S3Storage(S3Client client, String bucket, String prefix, String kmsKeyArn) {
            this.client = client;
            this.bucket = bucket;
            this.prefix = normalizePrefix(prefix);
            this.kmsKeyArn = kmsKeyArn;
        }

        @Override
        public List<Item> list() throws IOException {
            try {
                List<Item> items = new ArrayList<>();
                String token = null;
                do {
                    ListObjectsV2Request.Builder builder = ListObjectsV2Request.builder()
                        .bucket(bucket)
                        .prefix(prefix);
                    if (token != null) {
                        builder.continuationToken(token);
                    }
                    ListObjectsV2Response response = client.listObjectsV2(builder.build());
                    for (S3Object obj : response.contents()) {
                        String relativeKey = toRelativeKey(obj.key());
                        if (relativeKey.isEmpty() || relativeKey.endsWith("/")) {
                            continue;
                        }
                        items.add(new Item(relativeKey, obj.size()));
                    }
                    token = response.isTruncated() ? response.nextContinuationToken() : null;
                } while (token != null);
                return items;
            } catch (SdkException e) {
                throw new IOException("Failed to list objects under " + bucket + "/" + prefix, e);
            }
        }

        @Override
        public InputStream read(String key) throws IOException {
            try {
                GetObjectRequest request = GetObjectRequest.builder()
                    .bucket(bucket)
                    .key(resolveKey(key))
                    .build();
                ResponseInputStream<GetObjectResponse> response = client.getObject(request);
                return response;
            } catch (SdkException e) {
                throw new IOException("Failed to read " + key + " from " + bucket, e);
            }
        }

        @Override
        public void write(String key, InputStream content, long size) throws IOException {
            try {
                PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(resolveKey(key));
                if (kmsKeyArn != null && !kmsKeyArn.isBlank()) {
                    builder.serverSideEncryption(ServerSideEncryption.AWS_KMS)
                        .ssekmsKeyId(kmsKeyArn);
                }
                client.putObject(builder.build(), RequestBody.fromInputStream(content, Math.max(size, 0)));
            } catch (SdkException e) {
                throw new IOException("Failed to write " + key + " to " + bucket, e);
            }
        }

        @Override
        public void writeEmpty(String key) throws IOException {
            try {
                PutObjectRequest.Builder builder = PutObjectRequest.builder()
                    .bucket(bucket)
                    .key(resolveKey(key));
                if (kmsKeyArn != null && !kmsKeyArn.isBlank()) {
                    builder.serverSideEncryption(ServerSideEncryption.AWS_KMS)
                        .ssekmsKeyId(kmsKeyArn);
                }
                client.putObject(builder.build(), RequestBody.empty());
            } catch (SdkException e) {
                throw new IOException("Failed to write empty " + key + " to " + bucket, e);
            }
        }

        private String toRelativeKey(String fullKey) {
            if (fullKey.startsWith(prefix)) {
                return fullKey.substring(prefix.length());
            }
            return fullKey;
        }

        private String resolveKey(String key) {
            return prefix + key;
        }

        private static String normalizePrefix(String raw) {
            if (raw == null || raw.isBlank()) {
                return "";
            }
            String normalized = raw.replace('\\', '/');
            if (!normalized.endsWith("/")) {
                normalized = normalized + "/";
            }
            if (normalized.startsWith("/")) {
                normalized = normalized.substring(1);
            }
            return normalized;
        }
    }
}
