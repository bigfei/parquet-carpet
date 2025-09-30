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
package com.jerolba.carpet.samples;

import java.sql.*;
import java.io.*;
import java.math.BigDecimal;
import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import com.jerolba.carpet.*;
import com.jerolba.carpet.io.FileSystemOutputFile;
import com.jerolba.carpet.WriteModelFactory;
import com.jerolba.carpet.model.WriteRecordModelType;
import com.jerolba.carpet.model.FieldTypes;
import com.jerolba.carpet.model.FieldType;

/**
 * Dynamic JDBC to Parquet exporter that works without predefined Java record classes.
 * Uses Map<String, Object> as the record format and generates Parquet schema automatically
 * from ResultSet metadata.
 */
public class DynamicJdbcExporter {

    /**
     * Export any JDBC ResultSet to Parquet without predefined record classes
     */
    public static void exportResultSetToParquet(
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

                // Stream rows as Maps
                Stream<Map> recordStream =
                    resultSetToMapStream(resultSet);

                // Write all records
                writer.write(recordStream);
            }
        }
    }

    /**
     * Export with advanced configuration
     */
    public static void exportWithConfig(
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
                    // Process in batches
                    exportInBatches(resultSet, writer, config);
                }
            }
        }
    }

    /**
     * Convert ResultSet to Stream of Maps
     */
    private static Stream<Map> resultSetToMapStream(ResultSet resultSet) {
        try {
            return StreamSupport.stream(new ResultSetMapSpliterator(resultSet), false);
        } catch (SQLException e) {
            throw new RuntimeException("Failed to create stream from ResultSet", e);
        }
    }

    /**
     * Process ResultSet in batches for memory efficiency
     */
    private static void exportInBatches(
            ResultSet resultSet,
            CarpetWriter<Map> writer,
            DynamicExportConfig config) throws SQLException, IOException {

        List<Map> batch = new ArrayList<>(config.getBatchSize());
        int totalRows = 0;
        ResultSetMetaData metaData = resultSet.getMetaData();
        int columnCount = metaData.getColumnCount();

        while (resultSet.next()) {
            Map row = convertRowToMap(resultSet, metaData, columnCount, config);
            batch.add(row);

            if (batch.size() >= config.getBatchSize()) {
                writer.write(batch);
                batch.clear();

                totalRows += config.getBatchSize();
                if (totalRows % 10000 == 0) {
                    System.out.println("Processed " + totalRows + " rows");
                }
            }
        }

        // Write remaining records
        if (!batch.isEmpty()) {
            writer.write(batch);
            totalRows += batch.size();
        }

        System.out.println("Export completed. Total rows: " + totalRows);
    }

    /**
     * Convert ResultSet row to Map<String, Object>
     */
    private static Map convertRowToMap(
            ResultSet resultSet, ResultSetMetaData metaData,
            int columnCount, DynamicExportConfig config) throws SQLException {

        Map<String, Object> row = new LinkedHashMap<>();

        for (int i = 1; i <= columnCount; i++) {
            String columnName = metaData.getColumnLabel(i);
            int sqlType = metaData.getColumnType(i);

            // Apply column naming strategy
            if (config.isConvertCamelCase()) {
                columnName = camelToSnake(columnName);
            }

            Object value = getResultSetValue(resultSet, i, sqlType);
            row.put(columnName, value);
        }

        return row;
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

    /**
     * ResultSet to Map Spliterator implementation
     */
    private static class ResultSetMapSpliterator extends Spliterators.AbstractSpliterator<Map> {
        private final ResultSet resultSet;
        private final ResultSetMetaData metaData;
        private final int columnCount;

        public ResultSetMapSpliterator(ResultSet resultSet) throws SQLException {
            super(Long.MAX_VALUE, Spliterator.ORDERED | Spliterator.NONNULL);
            this.resultSet = resultSet;
            this.metaData = resultSet.getMetaData();
            this.columnCount = metaData.getColumnCount();
        }

        @Override
        public boolean tryAdvance(java.util.function.Consumer<? super Map> action) {
            try {
                if (!resultSet.next()) {
                    return false;
                }

                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = metaData.getColumnLabel(i);
                    int sqlType = metaData.getColumnType(i);
                    Object value = getResultSetValue(resultSet, i, sqlType);
                    row.put(columnName, value);
                }

                action.accept(row);
                return true;

            } catch (SQLException e) {
                throw new RuntimeException("Error processing ResultSet row", e);
            }
        }
    }
}