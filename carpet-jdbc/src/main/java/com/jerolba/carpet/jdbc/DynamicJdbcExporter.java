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
import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jerolba.carpet.CarpetWriter;
import com.jerolba.carpet.WriteModelFactory;
import com.jerolba.carpet.io.FileSystemOutputFile;
import com.jerolba.carpet.io.OutputStreamOutputFile;
import com.jerolba.carpet.model.FieldType;
import com.jerolba.carpet.model.FieldTypes;
import com.jerolba.carpet.model.WriteRecordModelType;

/**
 * Dynamic JDBC to Parquet exporter that works without predefined Java record classes.
 * Uses Map<String, Object> as the record format and generates Parquet schema automatically
 * from ResultSet metadata.
 */
public class DynamicJdbcExporter {

    private static final Logger logger = LoggerFactory.getLogger(DynamicJdbcExporter.class);

    /**
     * Export multiple tables in parallel to date-based output folder.
     *
     * This method exports multiple tables concurrently using a fixed thread pool.
     * Each table is exported to a separate Parquet file under a date-based folder
     * (yyyyMMdd format using the configured output timezone).
     *
     * Fail-fast behavior: If any table export fails, all remaining tasks are cancelled,
     * the executor shuts down, and the failed table's output file (including .metadata
     * sidecar if KMS enabled) is deleted. No partial results are returned.
     *
     * Thread pool size: configurable, default max(1, availableProcessors - 1) capped by table count
     *
     * @param tableNames List of table names to export
     * @param queryPattern SQL query pattern with %s placeholder for table name (e.g., "SELECT * FROM %s")
     * @param outputBaseDir Base directory for output (date folder will be created inside)
     * @param config Export configuration (compression, KMS encryption, etc.)
     * @param connectionSupplier Supplier for JDBC connections (one per thread, not shared)
     * @return Map of table names to row counts on success
     * @throws IOException if any export fails (no partial results)
     * @throws IllegalArgumentException if queryPattern doesn't contain %s
     */
    public static Map<String, Long> exportParallelWithConfig(
            List<String> tableNames,
            String queryPattern,
            File outputBaseDir,
            DynamicExportConfig config,
            Supplier<Connection> connectionSupplier) throws IOException {

        if (!queryPattern.contains("%s")) {
            logger.warn("Query pattern validation failed: pattern must contain %s placeholder");
            throw new IllegalArgumentException("Query pattern must contain %s placeholder for table name");
        }

        String dateFolder = LocalDate.now(config.getOutputTimeZone()).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = new File(outputBaseDir, dateFolder);
        if (!outputDir.exists() && !outputDir.mkdirs()) {
            throw new IOException("Failed to create output directory: " + outputDir);
        }

        for (String tableName : tableNames) {
            validateTableName(tableName);
        }

        int numThreads = resolveThreadCount(tableNames.size(), config.getThreadPoolSize());
        ExecutorService executor = Executors.newFixedThreadPool(numThreads);

        logger.info("Starting parallel export of {} tables using {} threads", tableNames.size(), numThreads);
        long exportStartTime = System.currentTimeMillis();

        Map<String, Long> results = new LinkedHashMap<>();
        List<Future<TableExportResult>> futures = new ArrayList<>();
        long totalRows = 0;
        int completedTables = 0;

        try {
            for (String tableName : tableNames) {
                Callable<TableExportResult> task = () -> {
                    Connection connection = connectionSupplier.get();
                    try {
                        String sanitizedName = tableName.replace("/", "_").replace("\\", "_");
                        File outputFile = new File(outputDir, sanitizedName + ".parquet");

                        String query = String.format(queryPattern, tableName);

                        long startTime = System.currentTimeMillis();

                        long rowCount;
                        if (config.getKmsEncryptionConfig() != null) {
                            rowCount = exportWithKmsEncryption(connection, query, outputFile, config);
                        } else {
                            rowCount = exportWithConfig(connection, query, outputFile, config);
                        }

                        long duration = System.currentTimeMillis() - startTime;

                        return new TableExportResult(tableName, rowCount, outputFile, null);
                    } catch (Exception e) {
                        logger.error("Failed to export table: {} - {}", tableName, e.getMessage(), e);
                        String sanitizedName = tableName.replace("/", "_").replace("\\", "_");
                        File outputFile = new File(outputDir, sanitizedName + ".parquet");
                        return new TableExportResult(tableName, 0L, outputFile, e);
                    } finally {
                        try {
                            connection.close();
                        } catch (SQLException e) {
                            logger.warn("Error closing connection for table: {}", tableName, e);
                        }
                    }
                };

                futures.add(executor.submit(task));
            }

            for (Future<TableExportResult> future : futures) {
                TableExportResult result = future.get();

                if (result.error != null) {
                    logger.error("Export failed for table: {}, cancelling remaining tasks", result.tableName);

                    executor.shutdownNow();
                    try {
                        executor.awaitTermination(60, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }

                    deleteIfExists(result.outputFile);
                    deleteIfExists(new File(result.outputFile.getAbsolutePath() + ".metadata"));

                    throw new IOException("Failed to export table: " + result.tableName, result.error);
                }

                results.put(result.tableName, result.rowCount);
                totalRows += result.rowCount;
                completedTables++;

                // Log progress after each table completion
                long elapsedMs = System.currentTimeMillis() - exportStartTime;
                double throughput = (totalRows * 1000.0) / elapsedMs;
                logger.info("Exported {}/{} tables, {} rows, {} rows/sec",
                    completedTables, tableNames.size(), totalRows, (long)throughput);
            }

            long totalDuration = System.currentTimeMillis() - exportStartTime;
            double finalThroughput = (totalRows * 1000.0) / totalDuration;
            logger.info("Parallel export completed successfully: {} tables, {} rows in {} ms ({} rows/sec)",
                results.size(), totalRows, totalDuration, (long)finalThroughput);
            return results;

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.error("Export interrupted", e);
            throw new IOException("Export interrupted", e);
        } catch (Exception e) {
            logger.error("Parallel export failed: {}", e.getMessage(), e);
            throw new IOException("Parallel export failed", e);
        } finally {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(60, TimeUnit.SECONDS)) {
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                executor.shutdownNow();
            }
        }
    }

    /**
     * Helper class to hold export results
     */
    private static class TableExportResult {
        final String tableName;
        final long rowCount;
        final File outputFile;
        final Exception error;

        TableExportResult(String tableName, long rowCount, File outputFile, Exception error) {
            this.tableName = tableName;
            this.rowCount = rowCount;
            this.outputFile = outputFile;
            this.error = error;
        }
    }

    private static final Pattern SAFE_TABLE_NAME = Pattern.compile("[A-Za-z0-9_$.]+", Pattern.CASE_INSENSITIVE);

    private static void validateTableName(String tableName) {
        if (tableName == null || tableName.isBlank() || !SAFE_TABLE_NAME.matcher(tableName).matches()) {
            throw new IllegalArgumentException("Invalid table name: " + tableName);
        }
    }

    private static int resolveThreadCount(int tableCount, int configuredThreads) {
        if (configuredThreads > 0) {
            return configuredThreads;
        }
        int defaultThreads = Math.max(1, Runtime.getRuntime().availableProcessors() - 1);
        return Math.max(1, Math.min(tableCount, defaultThreads));
    }

    private static void deleteIfExists(File file) {
        if (file.exists() && !file.delete()) {
            logger.warn("Failed to delete file: {}", file);
        }
    }

    /**
     * Export any JDBC ResultSet to Parquet without predefined record classes
     *
     * @return the total number of rows processed
     */
    public static long exportResultSetToParquet(
            Connection connection,
            String sqlQuery,
            File outputFile) throws SQLException, IOException {

        try (PreparedStatement statement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = statement.executeQuery()) {

            // Create dynamic WriteModelFactory based on ResultSet metadata
            WriteModelFactory<Map> modelFactory = createDynamicModelFactory(resultSet.getMetaData());

            // Create CarpetWriter with dynamic model
            try (CarpetWriter<Map> writer = new CarpetWriter.Builder<>(
                    new FileSystemOutputFile(outputFile),
                    Map.class)
                    .withWriteRecordModel(modelFactory)
                    .build()) {

                // Count rows as we process them
                long totalRows = 0;
                ResultSetMetaData metaData = resultSet.getMetaData();
                int columnCount = metaData.getColumnCount();

                List<Map> batch = new ArrayList<>(1000);
                while (resultSet.next()) {
                    Map<String, Object> row = new LinkedHashMap<>();
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = metaData.getColumnLabel(i);
                        int sqlType = metaData.getColumnType(i);
                        Object value = getResultSetValue(resultSet, i, sqlType);
                        row.put(columnName, value);
                    }
                    batch.add(row);
                    totalRows++;

                    if (batch.size() >= 1000) {
                        writer.write(batch);
                        batch.clear();
                    }
                }

                // Write remaining records
                if (!batch.isEmpty()) {
                    writer.write(batch);
                }

                return totalRows;
            }
        }
    }

    /**
     * Export with advanced configuration
     *
     * @return the total number of rows processed
     */
    public static long exportWithConfig(
            Connection connection,
            String sqlQuery,
            File outputFile,
            DynamicExportConfig config) throws SQLException, IOException {

        try (PreparedStatement statement = connection.prepareStatement(
                sqlQuery,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            // Configure fetch size
            if (config.getFetchSize() > 0) {
                statement.setFetchSize(config.getFetchSize());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                // Create dynamic WriteModelFactory based on ResultSet metadata
                WriteModelFactory<Map> modelFactory = createDynamicModelFactory(resultSet.getMetaData());

                // Create CarpetWriter with configuration
                CarpetWriter.Builder<Map> builder =
                    new CarpetWriter.Builder<>(
                        new FileSystemOutputFile(outputFile),
                        Map.class)
                    .withWriteRecordModel(modelFactory);

                // Apply configuration
                if (config.getCompressionCodec() != null) {
                    builder.withCompressionCodec(config.getCompressionCodec());
                }

                if (config.getColumnNamingStrategy() != null) {
                    builder.withColumnNamingStrategy(config.getColumnNamingStrategy());
                }

                try (CarpetWriter<Map> writer = builder.build()) {
                    // Process in batches and return count
                    return exportInBatches(resultSet, writer, config);
                }
            }
        }
    }

    /**
     * Export with AWS KMS envelope encryption support.
     *
     * This method encrypts Parquet files on-the-fly using AWS KMS envelope encryption:
     * 1. Generates a random data encryption key (DEK) for each file
     * 2. Encrypts the Parquet data with the DEK using AES-256-GCM
     * 3. Encrypts the DEK with AWS KMS
     * 4. Stores the encrypted DEK in a companion .metadata file
     *
     * Prerequisites:
     * - AWS SDK KMS library must be on classpath (software.amazon.awssdk:kms)
     * - Valid AWS credentials configured (environment variables, IAM role, or credentials file)
     * - KMS key must grant encrypt permission to the caller
     *
     * @param connection JDBC connection
     * @param sqlQuery SQL query to execute
     * @param outputFile Output file for encrypted Parquet data
     * @param config Export configuration including KMS encryption settings
     * @return the total number of rows processed
     * @throws SQLException if database operation fails
     * @throws IOException if file or encryption operations fail
     * @throws IllegalStateException if KMS encryption is not properly configured
     */
    public static long exportWithKmsEncryption(
            Connection connection,
            String sqlQuery,
            File outputFile,
            DynamicExportConfig config) throws SQLException, IOException {

        if (!config.isKmsEncryptionEnabled()) {
            throw new IllegalStateException(
                "KMS encryption not configured. Use config.withKmsEncryption() to enable encryption.");
        }

        try (PreparedStatement statement = connection.prepareStatement(
                sqlQuery,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            // Configure fetch size
            if (config.getFetchSize() > 0) {
                statement.setFetchSize(config.getFetchSize());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                // Create dynamic WriteModelFactory based on ResultSet metadata
                WriteModelFactory<Map> modelFactory = createDynamicModelFactory(resultSet.getMetaData());

                // Create KMS encrypting output stream
                KmsEnvelopeEncryptionOutputStream encryptingStream =
                    new KmsEnvelopeEncryptionOutputStream(outputFile, config.getKmsEncryptionConfig());

                try {
                    // Create CarpetWriter with encrypting stream
                    CarpetWriter.Builder<Map> builder =
                        new CarpetWriter.Builder<>(
                            new OutputStreamOutputFile(encryptingStream),
                            Map.class)
                        .withWriteRecordModel(modelFactory);

                    // Apply configuration
                    if (config.getCompressionCodec() != null) {
                        builder.withCompressionCodec(config.getCompressionCodec());
                    }

                    if (config.getColumnNamingStrategy() != null) {
                        builder.withColumnNamingStrategy(config.getColumnNamingStrategy());
                    }

                    try (CarpetWriter<Map> writer = builder.build()) {
                        // Process in batches and return count
                        long totalRows = exportInBatches(resultSet, writer, config);

                        System.out.println("KMS encryption metadata saved to: " +
                            encryptingStream.getMetadataFile().getAbsolutePath());
                        System.out.flush();

                        return totalRows;
                    }
                } finally {
                    encryptingStream.close();
                }
            }
        }
    }

    /**
     * Export with AWS KMS encryption using a shared DEK context.
     *
     * This method is optimized for batch exports where multiple files are created.
     * It reuses a single Data Encryption Key (DEK) across all exports, making only
     * ONE KMS API call instead of one per file. Each file still gets a unique IV
     * for security.
     *
     * Use this when exporting multiple tables/queries in a batch operation.
     *
     * @param connection JDBC connection
     * @param sqlQuery SQL query to execute
     * @param outputFile Output file for encrypted Parquet data
     * @param config Export configuration
     * @param sharedContext Shared encryption context with pre-encrypted DEK
     * @return the total number of rows processed
     * @throws SQLException if database operation fails
     * @throws IOException if file or encryption operations fail
     */
    public static long exportWithSharedKmsEncryption(
            Connection connection,
            String sqlQuery,
            File outputFile,
            DynamicExportConfig config,
            KmsSharedKeyEncryptionContext sharedContext) throws SQLException, IOException {

        try (PreparedStatement statement = connection.prepareStatement(
                sqlQuery,
                ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY)) {

            // Configure fetch size
            if (config.getFetchSize() > 0) {
                statement.setFetchSize(config.getFetchSize());
            }

            try (ResultSet resultSet = statement.executeQuery()) {
                // Create dynamic WriteModelFactory based on ResultSet metadata
                WriteModelFactory<Map> modelFactory = createDynamicModelFactory(resultSet.getMetaData());

                // Create KMS encrypting output stream with shared DEK (no KMS call!)
                KmsEnvelopeEncryptionOutputStream encryptingStream =
                    new KmsEnvelopeEncryptionOutputStream(outputFile, sharedContext);

                try {
                    // Create CarpetWriter with encrypting stream
                    CarpetWriter.Builder<Map> builder =
                        new CarpetWriter.Builder<>(
                            new OutputStreamOutputFile(encryptingStream),
                            Map.class)
                        .withWriteRecordModel(modelFactory);

                    // Apply configuration
                    if (config.getCompressionCodec() != null) {
                        builder.withCompressionCodec(config.getCompressionCodec());
                    }

                    if (config.getColumnNamingStrategy() != null) {
                        builder.withColumnNamingStrategy(config.getColumnNamingStrategy());
                    }

                    try (CarpetWriter<Map> writer = builder.build()) {
                        // Process in batches and return count
                        return exportInBatches(resultSet, writer, config);
                    }
                } finally {
                    encryptingStream.close();
                }
            }
        }
    }

    /**
     * Export multiple tables/queries with KMS encryption using a shared DEK.
     *
     * This method creates a single DEK and reuses it across all exports, making
     * only ONE KMS API call for the entire batch. This is significantly more
     * efficient than calling exportWithKmsEncryption() multiple times.
     *
     * Example:
     * <pre>
     * Map&lt;String, String&gt; queries = Map.of(
     *     "customers", "SELECT * FROM customers",
     *     "orders", "SELECT * FROM orders"
     * );
     * exportBatchWithKmsEncryption(connection, queries, outputDir, config);
     * </pre>
     *
     * @param connection JDBC connection
     * @param tableQueries Map of filename (without extension) to SQL query
     * @param outputDirectory Directory for output files
     * @param config Export configuration including KMS encryption settings
     * @return Map of table name to row count
     * @throws SQLException if database operation fails
     * @throws IOException if file or encryption operations fail
     * @throws IllegalStateException if KMS encryption is not properly configured
     */
    public static java.util.Map<String, Long> exportBatchWithKmsEncryption(
            Connection connection,
            java.util.Map<String, String> tableQueries,
            File outputDirectory,
            DynamicExportConfig config) throws SQLException, IOException {

        if (!config.isKmsEncryptionEnabled()) {
            throw new IllegalStateException(
                "KMS encryption not configured. Use config.withKmsEncryption() to enable encryption.");
        }

        if (!outputDirectory.exists() && !outputDirectory.mkdirs()) {
            throw new IOException("Failed to create output directory: " + outputDirectory);
        }

        java.util.Map<String, Long> results = new java.util.LinkedHashMap<>();

        // Create shared encryption context - this makes ONE KMS call for the entire batch
        try (KmsSharedKeyEncryptionContext sharedContext =
                new KmsSharedKeyEncryptionContext(config.getKmsEncryptionConfig())) {

            System.out.println("=== Batch KMS Encryption (1 KMS call for " + tableQueries.size() + " tables) ===");
            System.out.flush();

            for (var entry : tableQueries.entrySet()) {
                String tableName = entry.getKey();
                String query = entry.getValue();
                File outputFile = new File(outputDirectory, tableName + ".parquet");

                System.out.println("Exporting " + tableName + "...");
                System.out.flush();

                long rowCount = exportWithSharedKmsEncryption(
                    connection,
                    query,
                    outputFile,
                    config,
                    sharedContext
                );

                results.put(tableName, rowCount);
                System.out.println("  ✓ " + tableName + ": " + rowCount + " rows");
                System.out.flush();
            }

            System.out.println("=== Batch export complete: " + results.size() + " tables ===");
            System.out.flush();
        } catch (Exception e) {
            throw new IOException("Failed to export batch with KMS encryption", e);
        }

        return results;
    }

    /**
     * Process ResultSet in batches for memory efficiency
     *
     * @return the total number of rows processed
     */
    private static long exportInBatches(
            ResultSet resultSet,
            CarpetWriter<Map> writer,
            DynamicExportConfig config) throws SQLException, IOException {

        if (config.isUseMetadataCaching()) {
            return exportInBatchesWithCaching(resultSet, writer, config);
        } else {
            return exportInBatchesWithoutCaching(resultSet, writer, config);
        }
    }

    /**
     * Export with metadata caching optimization (default behavior)
     */
    private static long exportInBatchesWithCaching(
            ResultSet resultSet,
            CarpetWriter<Map> writer,
            DynamicExportConfig config) throws SQLException, IOException {

        List<Map> batch = new ArrayList<>(config.getBatchSize());
        long totalRows = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Pre-compute column metadata to avoid repeated lookups (performance optimization)
        ColumnMetadata[] columnMetadata = new ColumnMetadata[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int colIdx = i + 1; // JDBC is 1-indexed
            String columnName = metaData.getColumnLabel(colIdx);

            // Apply column naming strategy once
            if (config.isConvertCamelCase()) {
                columnName = camelToSnake(columnName);
            }

            int sqlType = metaData.getColumnType(colIdx);
            columnMetadata[i] = new ColumnMetadata(columnName, sqlType);
        }

        // Process rows with pre-computed metadata
        while (resultSet.next()) {
            Map<String, Object> row = new LinkedHashMap<>(columnCount);

            for (int i = 0; i < columnCount; i++) {
                ColumnMetadata col = columnMetadata[i];
                Object value = getResultSetValue(resultSet, i + 1, col.sqlType);
                row.put(col.name, value);
            }

            batch.add(row);

            if (batch.size() >= config.getBatchSize()) {
                writer.write(batch);
                totalRows += batch.size();
                batch.clear();

                // Reduce logging frequency for better performance
                if (totalRows % 100000 == 0) {
                    System.out.println("Processed " + totalRows + " rows");
                    System.out.flush();
                }
            }
        }

        // Write remaining records
        if (!batch.isEmpty()) {
            writer.write(batch);
            totalRows += batch.size();
        }

        System.out.println("Export completed. Total rows: " + totalRows);
        System.out.flush();
        return totalRows;
    }

    /**
     * Export without metadata caching (for performance comparison/benchmarking only)
     * This simulates the old behavior with repeated metadata lookups per row
     */
    private static long exportInBatchesWithoutCaching(
            ResultSet resultSet,
            CarpetWriter<Map> writer,
            DynamicExportConfig config) throws SQLException, IOException {

        List<Map> batch = new ArrayList<>(config.getBatchSize());
        long totalRows = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        // Process rows WITHOUT pre-computed metadata (old behavior)
        while (resultSet.next()) {
            Map<String, Object> row = new LinkedHashMap<>(columnCount);

            for (int i = 1; i <= columnCount; i++) {
                // Repeated metadata lookups (expensive!)
                String columnName = metaData.getColumnLabel(i);
                int sqlType = metaData.getColumnType(i);

                // Apply column naming strategy per row (expensive!)
                if (config.isConvertCamelCase()) {
                    columnName = camelToSnake(columnName);
                }

                Object value = getResultSetValue(resultSet, i, sqlType);
                row.put(columnName, value);
            }

            batch.add(row);

            if (batch.size() >= config.getBatchSize()) {
                writer.write(batch);
                totalRows += batch.size();
                batch.clear();

                // Reduce logging frequency for better performance
                if (totalRows % 100000 == 0) {
                    System.out.println("Processed " + totalRows + " rows");
                    System.out.flush();
                }
            }
        }

        // Write remaining records
        if (!batch.isEmpty()) {
            writer.write(batch);
            totalRows += batch.size();
        }

        System.out.println("Export completed. Total rows: " + totalRows);
        System.out.flush();
        return totalRows;
    }

    /**
     * Helper class to cache column metadata for performance optimization
     */
    private static class ColumnMetadata {
        final String name;
        final int sqlType;

        ColumnMetadata(String name, int sqlType) {
            this.name = name;
            this.sqlType = sqlType;
        }
    }



    /**
     * Get typed value from ResultSet
     */
    private static Object getResultSetValue(ResultSet resultSet, int columnIndex, int sqlType)
            throws SQLException {

        Object value = resultSet.getObject(columnIndex);

        // Handle SQL NULL values
        if (resultSet.wasNull()) {
            return null;
        }

        // Handle types that might come through as raw objects that need conversion
        if (value instanceof java.sql.Timestamp) {
            return ((java.sql.Timestamp) value).toLocalDateTime();
        }

        // Convert UUID to String to avoid type casting issues
        if (value instanceof java.util.UUID) {
            return value.toString();
        }

        // Convert JSON nodes to String
        if (value != null && value.getClass().getName().contains("JsonNode")) {
            return value.toString();
        }

        // Convert PostgreSQL arrays to String
        if (value != null && value.getClass().getName().contains("PgArray")) {
            try {
                java.sql.Array array = (java.sql.Array) value;
                Object arrayData = array.getArray();
                return java.util.Arrays.toString((Object[]) arrayData);
            } catch (SQLException e) {
                return value.toString(); // Fallback to toString
            }
        }

        // Handle byte arrays (BLOB data) - convert to Parquet Binary
        if (value instanceof byte[]) {
            return org.apache.parquet.io.api.Binary.fromConstantByteArray((byte[]) value);
        }

        // MySQL-specific type handling (before switch to catch numeric types)
        if (value instanceof Byte) {
            return ((Byte) value).intValue();
        }
        if (value instanceof Short) {
            return ((Short) value).intValue();
        }

        // SQLite-specific type handling (REAL type comes as Double, convert to Float)
        if (value instanceof Double && sqlType == java.sql.Types.REAL) {
            return ((Double) value).floatValue();
        }

        // Convert specific SQL types to appropriate Java types
        return switch (sqlType) {
            case java.sql.Types.ARRAY -> {
                java.sql.Array array = resultSet.getArray(columnIndex);
                yield array != null ? java.util.Arrays.toString((Object[]) array.getArray()) : null;
            }
            case java.sql.Types.DATE -> {
                java.sql.Date date = resultSet.getDate(columnIndex);
                yield date != null ? date.toLocalDate() : null;
            }

            case java.sql.Types.TIME, java.sql.Types.TIME_WITH_TIMEZONE -> {
                java.sql.Time time = resultSet.getTime(columnIndex);
                yield time != null ? time.toLocalTime() : null;
            }

            case java.sql.Types.TIMESTAMP, java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> {
                java.sql.Timestamp timestamp = resultSet.getTimestamp(columnIndex);
                yield timestamp != null ? timestamp.toLocalDateTime() : null;
            }

            case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL -> {
                BigDecimal decimal = resultSet.getBigDecimal(columnIndex);
                yield decimal;
            }

            case java.sql.Types.BINARY, java.sql.Types.VARBINARY, java.sql.Types.LONGVARBINARY -> {
                byte[] bytes = resultSet.getBytes(columnIndex);
                yield bytes != null ? org.apache.parquet.io.api.Binary.fromConstantByteArray(bytes) : null;
            }

            case java.sql.Types.CHAR, java.sql.Types.VARCHAR, java.sql.Types.LONGVARCHAR,
                 java.sql.Types.NCHAR, java.sql.Types.NVARCHAR, java.sql.Types.LONGNVARCHAR -> {
                // Handle special types that come as VARCHAR
                if (value instanceof java.util.UUID) {
                    yield value.toString();
                }
                // Handle JSON nodes (check class name to avoid import issues)
                if (value != null && value.getClass().getName().contains("JsonNode")) {
                    yield value.toString();
                }
                yield value;
            }

            case java.sql.Types.OTHER -> {
                // Handle various complex types
                if (value instanceof java.util.UUID) {
                    yield value.toString();
                }
                // Handle JSON nodes (check class name to avoid import issues)
                if (value != null && value.getClass().getName().contains("JsonNode")) {
                    yield value.toString();
                }
                // Handle any other complex types by converting to String
                if (value != null && !(value instanceof String) &&
                    !(value instanceof Number) && !(value instanceof Boolean) &&
                    !(value instanceof java.time.LocalDate) && !(value instanceof java.time.LocalDateTime) &&
                    !(value instanceof java.time.LocalTime) && !(value instanceof BigDecimal)) {
                    yield value.toString();
                }
                yield value;
            }

            // MySQL-specific type handling
            case java.sql.Types.TINYINT -> {
                // Convert MySQL TINYINT to Integer instead of Byte for consistency
                if (value instanceof Number) {
                    yield ((Number) value).intValue();
                }
                yield value;
            }

            case java.sql.Types.SMALLINT -> {
                // Convert MySQL SMALLINT to Integer instead of Short for consistency
                if (value instanceof Number) {
                    yield ((Number) value).intValue();
                }
                yield value;
            }

            default -> value;
        };
    }

    /**
     * Convert camelCase to snake_case
     */
    private static String camelToSnake(String str) {
        if (str == null || str.isEmpty()) {
            return str;
        }
        return str.replaceAll("([a-z])([A-Z])", "$1_$2").toLowerCase();
    }

    /**
     * Analyze ResultSet and return column information
     */
    public static List<ColumnInfo> analyzeResultSet(ResultSetMetaData metaData) throws SQLException {
        List<ColumnInfo> columns = new ArrayList<>();
        int columnCount = metaData.getColumnCount();

        for (int i = 1; i <= columnCount; i++) {
            ColumnInfo info = new ColumnInfo(
                metaData.getColumnLabel(i),
                metaData.getColumnName(i),
                metaData.getColumnType(i),
                metaData.getColumnTypeName(i),
                metaData.getColumnClassName(i),
                metaData.getPrecision(i),
                metaData.getScale(i),
                metaData.isNullable(i) == ResultSetMetaData.columnNullable,
                metaData.isAutoIncrement(i)
            );
            columns.add(info);
        }

        return columns;
    }

    /**
     * Print schema information for debugging
     */
    public static void printSchemaInfo(ResultSetMetaData metaData) throws SQLException {
        List<ColumnInfo> columns = analyzeResultSet(metaData);

        System.out.println("Schema Information:");
        System.out.println("==================");

        for (ColumnInfo column : columns) {
            System.out.printf("%-20s %-15s %-10s %s%n",
                column.label(),
                column.typeName(),
                column.nullable() ? "NULLABLE" : "REQUIRED",
                column.autoIncrement() ? "AUTO_INC" : ""
            );
        }
    }

    /**
     * Create a dynamic WriteModelFactory based on ResultSet metadata
     */
    private static WriteModelFactory<Map> createDynamicModelFactory(ResultSetMetaData metaData) throws SQLException {
        return (writeClass, context) -> {
            WriteRecordModelType<Map> model = FieldTypes.writeRecordModel(Map.class);

            try {
                int columnCount = metaData.getColumnCount();

                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    int sqlType = metaData.getColumnType(i);
                    boolean isNullable = metaData.isNullable(i) == ResultSetMetaData.columnNullable;

                FieldType fieldType = getFieldTypeForSqlType(sqlType, i, metaData, !isNullable);

                    model.withField(columnName, fieldType, map -> map.get(columnName));
                }
            } catch (SQLException e) {
                throw new RuntimeException("Failed to create dynamic model from ResultSet metadata", e);
            }

            return model;
        };
    }

    /**
     * Map SQL type to appropriate Parquet FieldType
     */
    private static FieldType getFieldTypeForSqlType(int sqlType, int columnIndex, ResultSetMetaData metaData, boolean isNotNull) throws SQLException {
        return switch (sqlType) {
            case java.sql.Types.BOOLEAN, java.sql.Types.BIT -> isNotNull ? FieldTypes.BOOLEAN.notNull() : FieldTypes.BOOLEAN;
            case java.sql.Types.TINYINT -> isNotNull ? FieldTypes.BYTE.notNull() : FieldTypes.BYTE;
            case java.sql.Types.SMALLINT -> isNotNull ? FieldTypes.SHORT.notNull() : FieldTypes.SHORT;
            case java.sql.Types.INTEGER -> isNotNull ? FieldTypes.INTEGER.notNull() : FieldTypes.INTEGER;
            case java.sql.Types.BIGINT -> isNotNull ? FieldTypes.LONG.notNull() : FieldTypes.LONG;
            case java.sql.Types.REAL -> isNotNull ? FieldTypes.FLOAT.notNull() : FieldTypes.FLOAT;
            case java.sql.Types.FLOAT, java.sql.Types.DOUBLE -> isNotNull ? FieldTypes.DOUBLE.notNull() : FieldTypes.DOUBLE;
            case java.sql.Types.NUMERIC, java.sql.Types.DECIMAL -> {
                int precision = metaData.getPrecision(columnIndex);
                int scale = metaData.getScale(columnIndex);

                // Handle databases that report precision as 0 (like SQLite)
                if (precision <= 0) {
                    precision = 18; // Default precision
                    scale = 10;    // Use a flexible scale to accommodate various decimal values
                }

                var decimalType = FieldTypes.BIG_DECIMAL.withPrecisionScale(precision, scale);
                yield isNotNull ? decimalType.notNull() : decimalType;
            }
            case java.sql.Types.CHAR, java.sql.Types.VARCHAR, java.sql.Types.LONGVARCHAR,
                 java.sql.Types.NCHAR, java.sql.Types.NVARCHAR, java.sql.Types.LONGNVARCHAR -> {
                // Check if this is a special type masquerading as VARCHAR
                try {
                    String typeName = metaData.getColumnTypeName(columnIndex);
                    if ("UUID".equalsIgnoreCase(typeName)) {
                        // For UUID, use STRING type but ensure values are converted to String
                        yield isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
                    }
                    if ("JSON".equalsIgnoreCase(typeName)) {
                        // For JSON, use STRING type but ensure values are converted to String
                        yield isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
                    }
                    // Handle SQLite BLOB columns that might be reported as VARCHAR
                    if ("BLOB".equalsIgnoreCase(typeName)) {
                        yield isNotNull ? FieldTypes.BINARY.notNull() : FieldTypes.BINARY;
                    }
                } catch (SQLException e) {
                    // Fall back to regular STRING handling
                }
                yield isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
            }
            case java.sql.Types.BINARY, java.sql.Types.VARBINARY, java.sql.Types.LONGVARBINARY,
             java.sql.Types.BLOB -> isNotNull ? FieldTypes.BINARY.notNull() : FieldTypes.BINARY;
            case java.sql.Types.ARRAY -> {
                // Arrays are converted to String representations for Parquet compatibility
                yield isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
            }
            case java.sql.Types.DATE -> isNotNull ? FieldTypes.LOCAL_DATE.notNull() : FieldTypes.LOCAL_DATE;
            case java.sql.Types.TIME, java.sql.Types.TIME_WITH_TIMEZONE -> isNotNull ? FieldTypes.LOCAL_TIME.notNull() : FieldTypes.LOCAL_TIME;
            case java.sql.Types.TIMESTAMP, java.sql.Types.TIMESTAMP_WITH_TIMEZONE -> isNotNull ? FieldTypes.LOCAL_DATE_TIME.notNull() : FieldTypes.LOCAL_DATE_TIME;
            case java.sql.Types.OTHER -> {
                // For UUID types, use STRING to be safe since some drivers return UUID as String
                yield isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
            }
            default -> isNotNull ? FieldTypes.STRING.notNull() : FieldTypes.STRING;
        };
    }

    /**
     * Column information record
     */
    public record ColumnInfo(
        String label,
        String name,
        int type,
        String typeName,
        String className,
        int precision,
        int scale,
        boolean nullable,
        boolean autoIncrement
    ) {}
}
