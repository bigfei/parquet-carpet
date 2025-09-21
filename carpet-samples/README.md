# Carpet Samples

This module contains example code and usage patterns for the Carpet Parquet library.

## Dynamic JDBC to Parquet Export

The `DynamicJdbcExporter` class provides a way to export JDBC ResultSet data to Parquet files without requiring predefined Java record classes. This is useful for:

- Database migrations
- Data export from any JDBC source
- ETL processes
- Data archiving

### Supported Databases

- **DuckDB** (In-memory for testing)
- **PostgreSQL** (Full support with advanced data types)
- Any JDBC-compliant database (with potential limitations)

### Basic Usage

```java
import com.jerolba.carpet.samples.*;
import java.sql.*;
import java.io.File;

// Simple export
String sql = "SELECT id, name, department FROM employees";
File outputFile = new File("employees.parquet");

try (Connection connection = DriverManager.getConnection("jdbc:your-database-url")) {
    DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
}
```

### Advanced Usage with Configuration

```java
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import com.jerolba.carpet.ColumnNamingStrategy;

DynamicExportConfig config = new DynamicExportConfig()
    .withBatchSize(1000)
    .withFetchSize(1000)
    .withCompressionCodec(CompressionCodecName.GZIP)
    .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE);

DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
```

## Database-Specific Features

### PostgreSQL Support

PostgreSQL is fully supported with special handling for:

- **JSON/JSONB types**: Automatically converted to String for Parquet compatibility
- **Array types**: Converted to String representations
- **Timestamp with time zone**: Properly handled
- **UUID types**: Converted to String
- **NULL handling**: Respects database constraints

#### PostgreSQL Example

```java
// PostgreSQL connection
String url = "jdbc:postgresql://localhost:5432/yourdb";
Properties props = new Properties();
props.setProperty("user", "username");
props.setProperty("password", "password");

try (Connection pgConnection = DriverManager.getConnection(url, props)) {
    String sql = "SELECT * FROM employees WHERE department = 'Engineering'";
    File outputFile = new File("pg_employees.parquet");

    DynamicJdbcExporter.exportResultSetToParquet(pgConnection, sql, outputFile);
}
```

### DuckDB Support

DuckDB is perfect for testing and in-memory operations:

```java
// In-memory DuckDB
try (Connection connection = DriverManager.getConnection("jdbc:duckdb:")) {
    // Create tables and data
    connection.createStatement().execute("CREATE TABLE employees (id INT, name VARCHAR)");
    connection.createStatement().execute("INSERT INTO employees VALUES (1, 'John')");

    // Export to Parquet
    String sql = "SELECT * FROM employees";
    File outputFile = new File("duckdb_employees.parquet");

    DynamicJdbcExporter.exportResultSetToParquet(connection, sql, outputFile);
}
```

## Data Type Mapping

The exporter automatically maps SQL types to Parquet types:

| SQL Type | Parquet Type | Notes |
|----------|--------------|-------|
| INTEGER | INT32 | |
| BIGINT | INT64 | |
| VARCHAR/TEXT | BINARY | String data |
| DECIMAL/NUMERIC | DECIMAL | With precision/scale |
| TIMESTAMP | INT64 | Unix timestamp in microseconds |
| DATE | INT32 | Days from Unix epoch |
| BOOLEAN | BOOLEAN | |
| JSON/JSONB | BINARY | Converted to String |
| UUID | BINARY | Converted to String |
| Arrays | BINARY | Converted to String representation |

## Running Examples

### DuckDB Example (No setup required)

```bash
# Run the example class
./gradlew :carpet-samples:test --tests DynamicJdbcExportExample

# Run only DuckDB tests
./gradlew :carpet-samples:test --tests DynamicJdbcExporterTest
```

### PostgreSQL Integration Tests (Requires Docker)

```bash
# Enable and run PostgreSQL integration tests
./gradlew :carpet-samples:test --tests DynamicJdbcExporterPostgreSQLTest
```

## Dependencies

To use the dynamic JDBC export functionality, you need:

```gradle
dependencies {
    implementation project(':carpet-record')

    // For DuckDB (testing)
    testImplementation "org.duckdb:duckdb_jdbc:1.1.3"

    // For PostgreSQL
    implementation "org.postgresql:postgresql:42.7.3"

    // For integration tests (optional)
    testImplementation "org.testcontainers:junit-jupiter:1.19.7"
    testImplementation "org.testcontainers:postgresql:1.19.7"
}
```

## Configuration Options

The `DynamicExportConfig` class provides these configuration options:

- **batchSize**: Number of records to batch before writing (default: 1000)
- **fetchSize**: JDBC fetch size for ResultSet processing (default: 1000)
- **compressionCodec**: Parquet compression algorithm (default: SNAPPY)
- **columnNamingStrategy**: Strategy for column name conversion (default: SNAKE_CASE)
- **convertCamelCase**: Whether to convert camelCase to snake_case (default: true)

## Error Handling

The exporter handles common issues:

- **Type conversion**: Automatic conversion of complex types (UUID, JSON, arrays)
- **NULL values**: Proper handling of nullable and non-nullable columns
- **Large datasets**: Memory-efficient streaming and batching
- **Schema evolution**: Dynamic schema generation from ResultSet metadata

## Performance Tips

1. **Use appropriate batch sizes**: Larger batches improve performance but use more memory
2. **Set fetch size**: Reduces memory usage for large ResultSets
3. **Choose compression**: SNAPPY for speed, GZIP for better compression
4. **Filter data early**: Use WHERE clauses to reduce the amount of data exported
5. **Use column projections**: Select only the columns you need

## Schema Analysis

You can analyze the schema of any JDBC query:

```java
try (ResultSet resultSet = statement.executeQuery(sql)) {
    ResultSetMetaData metaData = resultSet.getMetaData();
    List<DynamicJdbcExporter.ColumnInfo> columns =
        DynamicJdbcExporter.analyzeResultSet(metaData);

    for (DynamicJdbcExporter.ColumnInfo column : columns) {
        System.out.printf("Column: %s, Type: %s, Nullable: %s%n",
            column.label(), column.typeName(), column.nullable());
    }
}
```

This helps you understand the structure of your data before exporting.