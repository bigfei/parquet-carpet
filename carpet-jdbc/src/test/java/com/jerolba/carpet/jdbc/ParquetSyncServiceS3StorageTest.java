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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Response;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.ServerSideEncryption;
import software.amazon.awssdk.core.exception.SdkClientException;

class ParquetSyncServiceS3StorageTest {

    @Test
    void listUsesPrefixAndSkipsDirectoryMarkers() throws Exception {
        S3Client client = mock(S3Client.class);

        ListObjectsV2Response page1 = ListObjectsV2Response.builder()
            .isTruncated(true)
            .nextContinuationToken("t1")
            .contents(
                S3Object.builder().key("prefix/").size(0L).build(),
                S3Object.builder().key("prefix/nested/file1.parquet").size(10L).build(),
                S3Object.builder().key("prefix/file2.parquet.metadata").size(5L).build())
            .build();

        ListObjectsV2Response page2 = ListObjectsV2Response.builder()
            .isTruncated(false)
            .contents(S3Object.builder().key("prefix/ignored.txt").size(4L).build())
            .build();

        when(client.listObjectsV2(any(ListObjectsV2Request.class))).thenReturn(page1, page2);

        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(client, "bucket", "prefix");
        List<ParquetSyncService.Item> items = storage.list();

        List<String> keys = items.stream().map(ParquetSyncService.Item::key).collect(Collectors.toList());
        assertEquals(List.of("nested/file1.parquet", "file2.parquet.metadata", "ignored.txt"), keys);
    }

    @Test
    void readResolvesKeyWithPrefix() throws Exception {
        byte[] payload = "payload".getBytes();
        @SuppressWarnings("unchecked")
        ResponseInputStream<GetObjectResponse> response = mock(ResponseInputStream.class);
        when(response.readAllBytes()).thenReturn(payload);

        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class))).thenReturn(response);

        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(client, "bucket", "prefix");
        try (InputStream in = storage.read("file.parquet")) {
            assertArrayEquals(payload, in.readAllBytes());
        }

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(client).getObject(captor.capture());
        assertEquals("bucket", captor.getValue().bucket());
        assertEquals("prefix/file.parquet", captor.getValue().key());
    }

    @Test
    void writeAppliesKmsSettingsAndPrefix() throws Exception {
        S3Client client = mock(S3Client.class);
        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(
            client, "bucket", "prefix", "arn:aws:kms:ap-southeast-1:123:key/abc");

        byte[] payload = "data".getBytes();
        storage.write("data.parquet", new ByteArrayInputStream(payload), payload.length);
        storage.writeEmpty("SUCCESS.txt");

        ArgumentCaptor<PutObjectRequest> captor = ArgumentCaptor.forClass(PutObjectRequest.class);
        verify(client, times(2)).putObject(captor.capture(), any(RequestBody.class));

        PutObjectRequest first = captor.getAllValues().get(0);
        assertEquals("bucket", first.bucket());
        assertEquals("prefix/data.parquet", first.key());
        assertEquals(ServerSideEncryption.AWS_KMS, first.serverSideEncryption());
        assertEquals("arn:aws:kms:ap-southeast-1:123:key/abc", first.ssekmsKeyId());

        PutObjectRequest second = captor.getAllValues().get(1);
        assertEquals("prefix/SUCCESS.txt", second.key());
        assertEquals(ServerSideEncryption.AWS_KMS, second.serverSideEncryption());
    }

    @Test
    void listWrapsSdkException() {
        S3Client client = mock(S3Client.class);
        when(client.listObjectsV2(any(ListObjectsV2Request.class)))
            .thenThrow(SdkClientException.create("boom"));

        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(client, "bucket", "prefix");
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class, storage::list);
    }

    @Test
    void readWrapsSdkException() {
        S3Client client = mock(S3Client.class);
        when(client.getObject(any(GetObjectRequest.class)))
            .thenThrow(SdkClientException.create("boom"));

        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(client, "bucket", "prefix");
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class, () -> storage.read("file.parquet"));
    }

    @Test
    void writeWrapsSdkException() {
        S3Client client = mock(S3Client.class);
        when(client.putObject(any(PutObjectRequest.class), any(RequestBody.class)))
            .thenThrow(SdkClientException.create("boom"));

        ParquetSyncService.S3Storage storage = new ParquetSyncService.S3Storage(client, "bucket", "prefix");
        org.junit.jupiter.api.Assertions.assertThrows(IOException.class,
            () -> storage.write("file.parquet", new ByteArrayInputStream(new byte[]{1}), 1));
    }
}
