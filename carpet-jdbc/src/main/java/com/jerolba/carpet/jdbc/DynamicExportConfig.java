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

import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import com.jerolba.carpet.ColumnNamingStrategy;
import java.time.ZoneId;

/**
 * Configuration class for dynamic JDBC to Parquet export operations.
 */
public class DynamicExportConfig {
    private int batchSize = 10000;
    private int fetchSize = 10000;
    private CompressionCodecName compressionCodec = CompressionCodecName.SNAPPY;
    private ColumnNamingStrategy columnNamingStrategy = ColumnNamingStrategy.SNAKE_CASE;
    private boolean convertCamelCase = true;
    private boolean includeSchemaInfo = false;
    private boolean useMetadataCaching = true;  // Performance optimization toggle
    private KmsEncryptionConfig kmsEncryptionConfig;
    private int threadPoolSize = 0;
    private ZoneId outputTimeZone = ZoneId.systemDefault();

    /**
     * Default constructor with sensible defaults
     */
    public DynamicExportConfig() {
        // Default values set above
    }

    /**
     * Get the batch size for writing records
     */
    public int getBatchSize() {
        return batchSize;
    }

    /**
     * Set the batch size for writing records (default: 1000)
     */
    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    /**
     * Get the JDBC fetch size for ResultSet processing
     */
    public int getFetchSize() {
        return fetchSize;
    }

    /**
     * Set the JDBC fetch size for ResultSet processing (default: 1000)
     */
    public void setFetchSize(int fetchSize) {
        this.fetchSize = fetchSize;
    }

    /**
     * Get the compression codec for Parquet files
     */
    public CompressionCodecName getCompressionCodec() {
        return compressionCodec;
    }

    /**
     * Set the compression codec for Parquet files (default: SNAPPY)
     */
    public void setCompressionCodec(CompressionCodecName compressionCodec) {
        this.compressionCodec = compressionCodec;
    }

    /**
     * Get the column naming strategy
     */
    public ColumnNamingStrategy getColumnNamingStrategy() {
        return columnNamingStrategy;
    }

    /**
     * Set the column naming strategy (default: SNAKE_CASE)
     */
    public void setColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
        this.columnNamingStrategy = columnNamingStrategy;
    }

    /**
     * Check if camelCase to snake_case conversion is enabled
     */
    public boolean isConvertCamelCase() {
        return convertCamelCase;
    }

    /**
     * Enable or disable camelCase to snake_case conversion (default: true)
     */
    public void setConvertCamelCase(boolean convertCamelCase) {
        this.convertCamelCase = convertCamelCase;
    }

    /**
     * Check if metadata caching is enabled (performance optimization)
     */
    public boolean isUseMetadataCaching() {
        return useMetadataCaching;
    }

    /**
     * Enable or disable metadata caching optimization (default: true)
     * When enabled, column metadata is cached before processing rows, eliminating
     * repeated calls to getColumnLabel(), getColumnType(), and naming conversions.
     * Disabling this is only useful for performance benchmarking.
     */
    public void setUseMetadataCaching(boolean useMetadataCaching) {
        this.useMetadataCaching = useMetadataCaching;
    }

    /**
     * Check if schema information should be included
     */
    public boolean isIncludeSchemaInfo() {
        return includeSchemaInfo;
    }

    /**
     * Enable or disable schema information export (default: false)
     */
    public void setIncludeSchemaInfo(boolean includeSchemaInfo) {
        this.includeSchemaInfo = includeSchemaInfo;
    }

    /**
     * Builder pattern for creating configuration with method chaining
     */
    public static DynamicExportConfig builder() {
        return new DynamicExportConfig();
    }

    /**
     * Set batch size and return this instance for chaining
     */
    public DynamicExportConfig withBatchSize(int batchSize) {
        setBatchSize(batchSize);
        return this;
    }

    /**
     * Set fetch size and return this instance for chaining
     */
    public DynamicExportConfig withFetchSize(int fetchSize) {
        setFetchSize(fetchSize);
        return this;
    }

    /**
     * Set compression codec and return this instance for chaining
     */
    public DynamicExportConfig withCompressionCodec(CompressionCodecName compressionCodec) {
        setCompressionCodec(compressionCodec);
        return this;
    }

    /**
     * Set column naming strategy and return this instance for chaining
     */
    public DynamicExportConfig withColumnNamingStrategy(ColumnNamingStrategy columnNamingStrategy) {
        setColumnNamingStrategy(columnNamingStrategy);
        return this;
    }

    /**
     * Enable/disable camel case conversion and return this instance for chaining
     */
    public DynamicExportConfig withConvertCamelCase(boolean convertCamelCase) {
        setConvertCamelCase(convertCamelCase);
        return this;
    }

    /**
     * Enable/disable schema info export and return this instance for chaining
     */
    public DynamicExportConfig withIncludeSchemaInfo(boolean includeSchemaInfo) {
        setIncludeSchemaInfo(includeSchemaInfo);
        return this;
    }

    /**
     * Get the KMS encryption configuration
     */
    public KmsEncryptionConfig getKmsEncryptionConfig() {
        return kmsEncryptionConfig;
    }

    /**
     * Set the KMS encryption configuration for envelope encryption
     */
    public void setKmsEncryptionConfig(KmsEncryptionConfig kmsEncryptionConfig) {
        this.kmsEncryptionConfig = kmsEncryptionConfig;
    }

    /**
     * Enable KMS envelope encryption and return this instance for chaining
     */
    public DynamicExportConfig withKmsEncryption(KmsEncryptionConfig kmsEncryptionConfig) {
        setKmsEncryptionConfig(kmsEncryptionConfig);
        return this;
    }

    /**
     * Check if KMS encryption is enabled
     */
    public boolean isKmsEncryptionEnabled() {
        return kmsEncryptionConfig != null;
    }

    public int getThreadPoolSize() {
        return threadPoolSize;
    }

    public void setThreadPoolSize(int threadPoolSize) {
        this.threadPoolSize = threadPoolSize;
    }

    public DynamicExportConfig withThreadPoolSize(int threadPoolSize) {
        setThreadPoolSize(threadPoolSize);
        return this;
    }

    public ZoneId getOutputTimeZone() {
        return outputTimeZone;
    }

    public void setOutputTimeZone(ZoneId outputTimeZone) {
        if (outputTimeZone == null) {
            throw new IllegalArgumentException("outputTimeZone cannot be null");
        }
        this.outputTimeZone = outputTimeZone;
    }

    public DynamicExportConfig withOutputTimeZone(ZoneId outputTimeZone) {
        setOutputTimeZone(outputTimeZone);
        return this;
    }
}
