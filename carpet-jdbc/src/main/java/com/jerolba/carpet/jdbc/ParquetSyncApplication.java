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
import java.net.URI;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.S3ClientBuilder;

/**
 * CLI application that mirrors the provided rclone script using AWS SDK.
 */
public class ParquetSyncApplication {

    private static final String DEFAULT_SRC_BASE = "huawei_obs_uat:/uat-sunline-obs/6666/report/";
    private static final String DEFAULT_DST_BASE = "cbs_s3:/etl-sunline-mastertables/raw/corebanking/";
    private static final String DEFAULT_AWS_ACCOUNT_ID = "431312751623";
    private static final String DEFAULT_AWS_KMS_KEY_ID = "e6df0452-a104-4bc2-ae99-385abd703578";
    private static final String DEFAULT_REGION = "ap-southeast-1";

    public static void main(String[] args) throws IOException {
        if (args.length == 0) {
            System.err.println("Error: Date parameter required");
            System.err.println("Usage: java ParquetSyncApplication <YYYYMMDD>");
            System.err.println("Example: java ParquetSyncApplication 20251015");
            System.exit(1);
        }

        String date = args[0];

        String srcBase = envOrDefault("SRC_BASE", DEFAULT_SRC_BASE);
        String dstBase = envOrDefault("DST_BASE", DEFAULT_DST_BASE);
        String accountId = envOrDefault("AWS_ACCOUNT_ID", DEFAULT_AWS_ACCOUNT_ID);
        String kmsKeyId = envOrDefault("AWS_KMS_KEY_ID", DEFAULT_AWS_KMS_KEY_ID);
        String srcEndpoint = System.getenv("SRC_S3_ENDPOINT");
        String dstEndpoint = System.getenv("DST_S3_ENDPOINT");

        RemoteLocation sourceLocation = parse(appendDate(srcBase, date));
        RemoteLocation destinationLocation = parse(appendDate(dstBase, date));

        Region region = resolveRegion(kmsKeyId);
        String kmsKeyArn = resolveKmsKeyArn(kmsKeyId, accountId, region);

        System.out.println("Source: " + sourceLocation.bucket() + "/" + sourceLocation.prefix());
        System.out.println("Destination (flat): " + destinationLocation.bucket() + "/" + destinationLocation.prefix());

        try (S3Client sourceClient = buildClient(region, srcEndpoint);
             S3Client destinationClient = buildClient(region, dstEndpoint)) {

            ParquetSyncService.Storage source = new ParquetSyncService.S3Storage(
                sourceClient,
                sourceLocation.bucket(),
                sourceLocation.prefix()
            );
            ParquetSyncService.Storage destination = new ParquetSyncService.S3Storage(
                destinationClient,
                destinationLocation.bucket(),
                destinationLocation.prefix(),
                kmsKeyArn
            );

            ParquetSyncService.SyncResult result = new ParquetSyncService(source, destination).sync();
            System.out.println("Copied " + result.filesCopied() + " parquet files");
            System.out.println("Created SUCCESS.txt marker");
        }
    }

    private static String envOrDefault(String key, String defaultValue) {
        String value = System.getenv(key);
        return (value == null || value.isBlank()) ? defaultValue : value;
    }

    private static String appendDate(String base, String date) {
        String normalized = base.endsWith("/") ? base : base + "/";
        return normalized + date + "/";
    }

    private static Region resolveRegion(String kmsKeyId) {
        String regionFromEnv = System.getenv("AWS_REGION");
        if (regionFromEnv != null && !regionFromEnv.isBlank()) {
            return Region.of(regionFromEnv);
        }

        if (kmsKeyId != null && kmsKeyId.startsWith("arn:aws:kms:")) {
            String[] parts = kmsKeyId.split(":");
            if (parts.length > 3) {
                return Region.of(parts[3]);
            }
        }

        return Region.of(DEFAULT_REGION);
    }

    private static String resolveKmsKeyArn(String kmsKeyId, String accountId, Region region) {
        if (kmsKeyId == null || kmsKeyId.isBlank()) {
            return null;
        }
        if (kmsKeyId.startsWith("arn:")) {
            return kmsKeyId;
        }
        return "arn:aws:kms:" + region.id() + ":" + accountId + ":key/" + kmsKeyId;
    }

    private static S3Client buildClient(Region region, String endpointOverride) {
        S3ClientBuilder builder = S3Client.builder()
            .region(region)
            .serviceConfiguration(S3Configuration.builder()
                .pathStyleAccessEnabled(true)
                .build());

        if (endpointOverride != null && !endpointOverride.isBlank()) {
            builder.endpointOverride(URI.create(endpointOverride));
        }

        return builder.build();
    }

    private static RemoteLocation parse(String rawLocation) {
        if (rawLocation == null || rawLocation.isBlank()) {
            throw new IllegalArgumentException("Remote location cannot be null or blank");
        }
        String location = rawLocation.trim();
        if (location.startsWith("s3://")) {
            location = location.substring("s3://".length());
        }
        int colonIndex = location.indexOf(':');
        if (colonIndex >= 0) {
            location = location.substring(colonIndex + 1);
        }
        location = location.replaceFirst("^/+", "");
        if (location.isEmpty()) {
            throw new IllegalArgumentException("Remote location missing bucket: " + rawLocation);
        }
        int slash = location.indexOf('/');
        if (slash < 0) {
            return new RemoteLocation(location, "");
        }
        String bucket = location.substring(0, slash);
        String prefix = location.substring(slash + 1);
        return new RemoteLocation(bucket, prefix);
    }

    private record RemoteLocation(String bucket, String prefix) {
    }
}
