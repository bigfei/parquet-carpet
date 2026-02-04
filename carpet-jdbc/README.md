# Carpet JDBC

This module provides JDBC-to-Parquet export capabilities for the Carpet library, enabling seamless data export from any JDBC-compliant database to Parquet format without requiring predefined Java record classes.

## Dynamic JDBC to Parquet Export

The `DynamicJdbcExporter` class provides a way to export JDBC ResultSet data to Parquet files without requiring predefined Java record classes. This is useful for:

- Database migrations
- Data export from any JDBC source
- ETL processes
- Data archiving

### Supported Databases

- **DuckDB** (In-memory for testing)
- **PostgreSQL** (Full support with advanced data types)
- **MySQL** (Full support with MySQL-specific data types)
- **SQLite** (Full support with file-based databases and dynamic typing)
- **GaussDB** (Full support - PostgreSQL-compatible with Huawei extensions)
- Any JDBC-compliant database (with potential limitations)

### Basic Usage

```java
import com.jerolba.carpet.jdbc.*;
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

**PostgreSQL-Specific Field Mappings:**
- `JSON`/`JSONB` → `String` (BINARY in Parquet)
- `TEXT[]`/`INTEGER[]` → `String` (array representation)
- `TIMESTAMPTZ` → `INT64` (microseconds from epoch)
- `UUID` → `String` (UUID.toString())
- `BYTEA` → `BINARY` (byte array)
- `NUMERIC(p,s)` → `DECIMAL(p,s)` (with precision/scale preserved)

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

**DuckDB-Specific Field Mappings:**
- `INTEGER` → `INT32` (standard integer)
- `BIGINT` → `INT64` (64-bit integer)
- `VARCHAR` → `BINARY` (string data)
- `DOUBLE` → `DOUBLE` (64-bit floating point)
- `BOOLEAN` → `BOOLEAN` (true/false)
- `DATE` → `INT32` (days from Unix epoch)
- `TIMESTAMP` → `INT64` (microseconds from epoch)
- `DECIMAL(p,s)` → `DECIMAL(p,s)` (precision/scale preserved)
- `STRUCT` types → `String` (JSON representation)

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
| REAL/FLOAT | FLOAT | SQLite REAL type handling |
| BLOB | BINARY | Binary data (SQLite-specific handling) |

## Running Examples

### DuckDB Example (No setup required)

```bash
# Run the example class
./gradlew :carpet-jdbc:test --tests DynamicJdbcExportExample

# Run only DuckDB tests
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterTest
```

### SQLite Example (No setup required)

```bash
# Run SQLite integration tests
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterSQLiteTest
```

### PostgreSQL Integration Tests (Requires Docker)

```bash
# Enable and run PostgreSQL integration tests
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterPostgreSQLTest
```

### MySQL Support
MySQL is fully supported with special handling for:
- **JSON type**: Automatically converted to String for Parquet compatibility
- **ENUM/SET types**: Converted to String representations
- **TINYINT/SMALLINT**: Converted to Integer for consistency
- **YEAR type**: Handled appropriately
- **Timestamp/Datetime types**: Properly handled
- **BLOB/TEXT types**: Supported with appropriate conversions
- **Character sets**: UTF-8 and UTF8MB4 supported

**MySQL-Specific Field Mappings:**
- `TINYINT` → `INT32` (converted from Byte)
- `SMALLINT` → `INT32` (converted from Short)
- `MEDIUMINT` → `INT32` (standard integer)
- `INT` → `INT32` (standard integer)
- `BIGINT` → `INT64` (64-bit integer)
- `JSON` → `String` (BINARY in Parquet)
- `ENUM('a','b')` → `String` (enum value)
- `SET('a','b')` → `String` (comma-separated values)
- `YEAR` → `INT32` (year as integer)
- `DATETIME` → `INT64` (microseconds from epoch)
- `TEXT`/`LONGTEXT` → `BINARY` (string data)
- `BLOB`/`LONGBLOB` → `BINARY` (binary data)
- `DECIMAL(p,s)` → `DECIMAL(p,s)` (precision/scale preserved)
- `VARCHAR(n)` → `BINARY` (respects character set)

#### MySQL Example
```java
// MySQL connection
String url = "jdbc:mysql://localhost:3306/yourdb";
Properties props = new Properties();
props.setProperty("user", "username");
props.setProperty("password", "password");
try (Connection mysqlConnection = DriverManager.getConnection(url, props)) {
    String sql = "SELECT id, name, email FROM users WHERE active = 1";
    File outputFile = new File("users.parquet");
    DynamicJdbcExporter.exportResultSetToParquet(mysqlConnection, sql, outputFile);
}
```

### MySQL Integration Tests (Requires Docker)
```bash
# Enable and run MySQL integration tests
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterMySQLTest
```

### SQLite Support

SQLite is fully supported with special handling for:

- **File-based databases**: No server required, uses local files
- **Type affinity**: Proper handling of SQLite's dynamic typing system
- **BLOB data**: Binary data support (with some limitations)
- **REAL type**: Floating-point number handling
- **NULL behavior**: SQLite-specific nullable column reporting
- **JSON in TEXT**: JSON data stored in TEXT columns

**SQLite-Specific Field Mappings:**
- `INTEGER` → `INT32` (standard integer)
- `REAL` → `FLOAT` (32-bit floating point, converted from Double)
- `TEXT` → `BINARY` (string data, UTF-8 encoded)
- `BLOB` → `BINARY` (binary data, byte array support)
- `NUMERIC` → `DECIMAL(18,10)` (default precision/scale)
- `BOOLEAN` → `BOOLEAN` (0/1 converted to true/false)
- `DATE` → `INT32` (days from Unix epoch)
- `DATETIME` → `INT64` (microseconds from epoch)
- `JSON (in TEXT)` → `String` (JSON string preserved)
- `FTS3/FTS5` → `BINARY` (virtual table text columns)
- `NULL` handling → Respects column constraints and dynamic typing
- `AUTOINCREMENT` → Standard integer handling (auto-increment values preserved)

#### SQLite Example

```java
// SQLite connection (file-based)
String sqliteUrl = "jdbc:sqlite:sample.db";

try (Connection sqliteConnection = DriverManager.getConnection(sqliteUrl)) {

    // Setup sample data (creates tables if they don't exist)
    setupSampleData(sqliteConnection);

    // Export with custom configuration
    String sql = "SELECT e.employee_id, e.employee_name, e.salary, e.hire_date, " +
               "e.is_active, d.department_name, e.created_at " +
               "FROM employees e " +
               "JOIN departments d ON e.department_id = d.department_id " +
               "WHERE e.department_id = 1";

    File outputFile = new File("sqlite_employees_export.parquet");

    DynamicExportConfig config = new DynamicExportConfig()
        .withBatchSize(1000)
        .withCompressionCodec(CompressionCodecName.SNAPPY)
        .withFetchSize(500);

    DynamicJdbcExporter.exportWithConfig(sqliteConnection, sql, outputFile, config);

    System.out.println("SQLite export completed successfully!");

    // Verify export
    List<Map<String, Object>> records = verifyExport(outputFile);
    System.out.println("Exported " + records.size() + " records from SQLite");

    // Clean up
    outputFile.delete();
}
```

### SQLite Integration Tests
```bash
# Run SQLite integration tests (no setup required)
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterSQLiteTest
```

### GaussDB Support

GaussDB (Huawei's enterprise-grade relational database) is fully supported. GaussDB is based on PostgreSQL, so most PostgreSQL features work seamlessly, with additional support for Huawei-specific extensions.

**GaussDB Compatibility:**
- Full PostgreSQL compatibility (based on PostgreSQL 9.x/10.x+)
- Support for standard SQL types and PostgreSQL extensions
- JSON/JSONB types supported
- Array types supported
- Spatial types (if enabled)
- Advanced data types (UUID, INET, etc.)

**GaussDB-Specific Field Mappings:**
Since GaussDB is PostgreSQL-compatible, it uses the same type mappings:
- `INTEGER` → `INT32` (standard integer)
- `BIGINT` → `INT64` (64-bit integer)
- `VARCHAR`/`TEXT` → `BINARY` (string data, UTF-8)
- `DECIMAL(p,s)` → `DECIMAL(p,s)` (precision/scale preserved)
- `TIMESTAMP` → `INT64` (microseconds from epoch)
- `DATE` → `INT32` (days from Unix epoch)
- `BOOLEAN` → `BOOLEAN` (true/false)
- `JSON`/`JSONB` → `String` (BINARY in Parquet)
- Arrays → `String` (array representation)
- `UUID` → `String` (UUID.toString())
- `BYTEA` → `BINARY` (byte array)

#### GaussDB Connection Example

```java
// GaussDB connection using environment variables
String url = System.getenv().getOrDefault("GAUSSDB_URL",
    "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb");
String username = System.getenv("GAUSSDB_USERNAME");
String password = System.getenv("GAUSSDB_PASSWORD");

try (Connection gaussConnection = DriverManager.getConnection(url, username, password)) {
    String sql = "SELECT employee_id, employee_name, department, salary, hire_date " +
                 "FROM employees WHERE department = 'Engineering'";

    File outputFile = new File("gaussdb_employees.parquet");

    // Simple export
    DynamicJdbcExporter.exportResultSetToParquet(gaussConnection, sql, outputFile);

    System.out.println("GaussDB export completed successfully!");
}
```

#### GaussDB Export with Configuration

```java
// GaussDB connection
String url = "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb";
String username = System.getenv("GAUSSDB_USERNAME");
String password = System.getenv("GAUSSDB_PASSWORD");

try (Connection connection = DriverManager.getConnection(url, username, password)) {
    String sql = "SELECT * FROM large_table WHERE created_at >= CURRENT_DATE - INTERVAL '30 days'";
    File outputFile = new File("gaussdb_large_export.parquet");

    // Configure for optimal performance
    DynamicExportConfig config = new DynamicExportConfig()
        .withBatchSize(5000)           // Larger batches for better performance
        .withFetchSize(1000)           // Optimize network roundtrips
        .withCompressionCodec(CompressionCodecName.GZIP)  // Better compression
        .withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE);

    DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);
}
```

#### GaussDB Integration Tests

The GaussDB tests require a running GaussDB instance and use environment variables for connection:

```bash
# Set environment variables
export GAUSSDB_URL="jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb"
export GAUSSDB_USERNAME="your_username"
export GAUSSDB_PASSWORD="your_password"

# Run GaussDB integration tests
./gradlew :carpet-jdbc:test --tests DynamicJdbcExporterGaussDBTest

# Tests will be skipped if credentials are not set
```

**Important Notes:**

1. **Database Permissions**: The test user needs `CREATE TABLE` permission in the database. If you get a "Permission denied for schema public" error, grant the necessary permissions:

   ```sql
   -- Option 1: Grant permissions on public schema
   GRANT CREATE ON SCHEMA public TO your_username;
   GRANT USAGE ON SCHEMA public TO your_username;

   -- Option 2: Create a dedicated test schema
   CREATE SCHEMA test_schema AUTHORIZATION your_username;
   -- Then modify GAUSSDB_URL to include the schema:
   -- export GAUSSDB_URL="jdbc:gaussdb://host:port/database?currentSchema=test_schema"
   ```

2. **Test Behavior**: Unlike PostgreSQL and MySQL tests which use Testcontainers, GaussDB tests connect to a real database instance since GaussDB container images are not publicly available. The tests will be automatically skipped if:
   - Required environment variables are not set
   - Connection to the database fails
   - User doesn't have sufficient permissions to create test tables

3. **Cleanup**: Tests automatically clean up all created tables in the `@AfterEach` method.

**📖 For detailed setup instructions, troubleshooting, and usage examples, see the [GaussDB Support Guide](../docs/gaussdb-support.md).**

#### GaussDB JDBC Dependency

To use GaussDB with the Carpet JDBC module, include the GaussDB JDBC driver:

```gradle
dependencies {
    implementation project(':carpet-record')
    implementation "com.huaweicloud.gaussdb:gaussdbjdbc:506.0.0.b058"
}
```

Or with Maven:

```xml
<dependency>
    <groupId>com.huaweicloud.gaussdb</groupId>
    <artifactId>gaussdbjdbc</artifactId>
    <version>506.0.0.b058</version>
</dependency>
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

    // For MySQL
    implementation "com.mysql:mysql-connector-j:8.0.33"

    // For SQLite
    implementation "org.xerial:sqlite-jdbc:3.45.1.0"

    // For GaussDB
    implementation "com.huaweicloud.gaussdb:gaussdbjdbc:506.0.0.b058"

    // For integration tests (optional)
    testImplementation "org.testcontainers:junit-jupiter:1.19.7"
    testImplementation "org.testcontainers:postgresql:1.19.7"
    testImplementation "org.testcontainers:mysql:1.19.7"
}
```

## Configuration Options

The `DynamicExportConfig` class provides these configuration options:

- **batchSize**: Number of records to batch before writing (default: 1000)
- **fetchSize**: JDBC fetch size for ResultSet processing (default: 1000)
- **compressionCodec**: Parquet compression algorithm (default: SNAPPY)
- **columnNamingStrategy**: Strategy for column name conversion (default: SNAKE_CASE)
- **convertCamelCase**: Whether to convert camelCase to snake_case (default: true)

## AWS KMS Encryption

The module supports AWS KMS envelope encryption for Parquet files. This provides secure at-rest encryption using AWS Key Management Service.

### Basic KMS Encryption

```java
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012")
    .withAwsRegion("us-east-1")
    .withEncryptionContextEntry("table", "customers");

DynamicExportConfig config = new DynamicExportConfig()
    .withBatchSize(1000)
    .withKmsEncryption(kmsConfig);

DynamicJdbcExporter.exportWithKmsEncryption(connection, sql, outputFile, config);
```

### Using AWS Profiles

To use a specific profile from `~/.aws/credentials`, configure the `awsProfile` option:

```java
KmsEncryptionConfig kmsConfig = KmsEncryptionConfig.builder()
    .withKmsKeyId("arn:aws:kms:ap-southeast-1:123456789012:key/your-key-id")
    .withAwsRegion("ap-southeast-1")
    .withAwsProfile("uat2")  // Use [uat2] section from ~/.aws/credentials
    .withEncryptionContextEntry("environment", "uat");
```

This is useful when:
- Running locally with multiple AWS accounts
- Testing with different credential sets
- CI/CD environments with named profiles

If `awsProfile` is not set, the default AWS credential provider chain is used.

### KMS Configuration Options

The `KmsEncryptionConfig` class provides:

| Option | Description |
|--------|-------------|
| `kmsKeyId` | AWS KMS key ID, ARN, or alias (required) |
| `awsRegion` | AWS region for KMS operations |
| `awsProfile` | AWS profile name from `~/.aws/credentials` |
| `kmsEndpointUrl` | VPC endpoint URL for PrivateLink access |
| `encryptionContext` | Additional authenticated data for KMS |
| `keySize` | DEK key size: 128 or 256 bits (default: 256) |

### Running KMS Tests

Configure `src/test/resources/test.properties`:

```properties
aws.kms.keyId=arn:aws:kms:ap-southeast-1:123456789012:key/your-key-id
aws.kms.region=ap-southeast-1
aws.profile=uat2
```

Run the tests:

```bash
./gradlew :carpet-jdbc:test --tests KmsEncryptionTest
```

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

## Multi-Table Parallel Export CLI

The `DynamicJdbcExportCli` provides a command-line interface for exporting multiple database tables to Parquet files in parallel.

### Building the Executable JAR

**Requirements:**
- Java 17 or higher
- Gradle (included via wrapper)

**Two JAR options are available:**

#### Option 1: Lightweight JAR (without JDBC drivers)

```bash
./gradlew :carpet-jdbc:fatJar
```

Creates `carpet-jdbc-0.5.0-all.jar` (~42MB) containing:
- All runtime dependencies (Parquet, Hadoop, AWS SDK, etc.)
- **No JDBC drivers** - you must provide your own driver JAR

Use this when:
- You already have JDBC drivers in your classpath
- You want a smaller download size
- You need a specific driver version

```bash
# Run with external JDBC driver
java -cp "carpet-jdbc-0.5.0-all.jar:postgresql-42.7.3.jar" \
  com.jerolba.carpet.jdbc.cli.DynamicJdbcExportCli \
  --properties export.properties \
  --tables tables.txt
```

#### Option 2: Full JAR (with JDBC drivers) - Recommended

```bash
./gradlew :carpet-jdbc:fatJarWithDrivers
```

Creates `carpet-jdbc-0.5.0-full.jar` (~131MB) containing:
- All runtime dependencies (Parquet, Hadoop, AWS SDK, etc.)
- **Bundled JDBC drivers** (no additional JAR files needed):
  - PostgreSQL (`postgresql-42.7.3.jar`)
  - MySQL (`mysql-connector-j-8.0.33.jar`)
  - SQLite (`sqlite-jdbc-3.45.1.0.jar`)
  - DuckDB (`duckdb_jdbc-1.1.3.jar`)
  - GaussDB (`gaussdbjdbc-506.0.0.b058.jar`)

**No additional JDBC driver downloads required** - ready to connect to any supported database.

```bash
# Run directly - batteries included
java -jar carpet-jdbc-0.5.0-full.jar \
  --properties export.properties \
  --tables tables.txt

# Export all user tables (no tables file)
java -jar carpet-jdbc-0.5.0-full.jar \
  --properties export.properties
```

#### Build Both JARs

```bash
./gradlew :carpet-jdbc:build
```

This creates both JAR variants in `carpet-jdbc/build/libs/`.

### CLI Usage

```bash
# Using full JAR (recommended)
java -jar carpet-jdbc-0.5.0-full.jar \
  --properties export.properties \
  --tables tables.txt

# Export all user tables (no tables file)
java -jar carpet-jdbc-0.5.0-full.jar \
  --properties export.properties
```

**Required Arguments:**
- `--properties <file>`: Path to the properties configuration file

**Optional Arguments:**
- `--tables <file>`: Path to the table list file (one table name per line). If omitted, all user tables are exported.
- `--kms`: Force-enable KMS encryption (requires `aws.kms.keyId`).
- `--no-kms`: Disable KMS encryption even if `aws.kms.*` is configured.
- `--help`: Display usage information

### Properties File Format

Create a `.properties` file with the following configuration:

```properties
# Required: JDBC connection settings
jdbc.url=jdbc:postgresql://localhost:5432/mydb
jdbc.user=myuser
jdbc.password=secret

# Required: Output directory
output.baseDir=output

# Optional: Export configuration
export.queryPattern=SELECT * FROM %s
export.batchSize=1000
export.fetchSize=1000
export.compression=SNAPPY
export.namingStrategy=SNAKE_CASE
export.convertCamelCase=true
export.includeSchemaInfo=true

# Optional: AWS KMS encryption
aws.kms.keyId=arn:aws:kms:ap-southeast-1:123456789012:key/your-key-id
aws.kms.region=ap-southeast-1
aws.profile=uat2
aws.kms.endpointUrl=https://vpce-xxxx.kms.ap-southeast-1.vpce.amazonaws.com
```

**Property Reference:**

| Property | Required | Default | Description |
|----------|----------|---------|-------------|
| `jdbc.url` | Yes | - | JDBC connection URL |
| `jdbc.user` | Yes | - | Database username |
| `jdbc.password` | Yes | - | Database password |
| `output.baseDir` | Yes | - | Base directory for Parquet files |
| `export.queryPattern` | No | `SELECT * FROM %s` | SQL query pattern (%s = table name) |
| `export.batchSize` | No | 1000 | Records per batch |
| `export.fetchSize` | No | 1000 | JDBC fetch size |
| `export.compression` | No | SNAPPY | Compression codec (UNCOMPRESSED, SNAPPY, GZIP, LZO, BROTLI, LZ4, ZSTD) |
| `export.namingStrategy` | No | SNAKE_CASE | Column naming strategy (FIELD_NAME, SNAKE_CASE) |
| `export.convertCamelCase` | No | true | Convert camelCase to snake_case |
| `export.includeSchemaInfo` | No | true | Include Parquet schema metadata |
| `aws.kms.keyId` | No | - | AWS KMS key ID or ARN for encryption |
| `aws.kms.region` | No | - | AWS region for KMS operations |
| `aws.profile` | No | - | AWS profile from ~/.aws/credentials |
| `aws.kms.endpointUrl` | No | - | Custom KMS endpoint URL |

### Table List File Format

Create a text file with one table name per line:

```
# Database tables to export
employees
departments
projects

# Comments are ignored (lines starting with #)
customers
orders
```

- One table name per line
- Empty lines are ignored
- Lines starting with `#` are treated as comments
- Duplicate table names are automatically deduplicated with a warning

If `--tables` is not provided, the CLI discovers tables using JDBC metadata (`DatabaseMetaData.getTables`) and exports all user tables (system schemas like `pg_*`, `information_schema`, `mysql`, `performance_schema`, `sqlite_*` are filtered out). Schema-qualified names are used when available.

### Output Structure

Exported files are organized by date:

```
output/
└── 20260128/          # yyyyMMdd format (system default timezone)
    ├── employees.parquet
    ├── departments.parquet
    ├── projects.parquet
    ├── customers.parquet
    └── orders.parquet
```

With KMS encryption enabled, metadata sidecars are also created:

```
output/
└── 20260128/
    ├── employees.parquet
    ├── employees.parquet.metadata
    ├── departments.parquet
    └── departments.parquet.metadata
```

### Parallel Export Behavior

- **Thread Pool**: Uses `max(1, CPU_count - 1)` threads
- **Fail-Fast**: Stops on first error, cancels remaining tasks
- **Cleanup**: Automatically removes failed table outputs
- **Logging**: Progress updates via SLF4J (exported X/Y tables, rows/sec)

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success - all tables exported |
| 1 | Export failure - one or more tables failed |
| 2 | Validation error - invalid arguments or configuration |

### Complete Example

**1. Create `export.properties`:**

```properties
jdbc.url=jdbc:postgresql://localhost:5432/production
jdbc.user=etl_user
jdbc.password=etl_password
output.baseDir=/data/exports

export.queryPattern=SELECT * FROM %s WHERE archived = false
export.batchSize=5000
export.fetchSize=1000
export.compression=GZIP
export.namingStrategy=SNAKE_CASE

aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/abc-123
aws.kms.region=us-east-1
aws.profile=production
```

**2. Create `tables.txt` (optional):**

```
users
transactions
audit_logs
```

**3. Run export:**

```bash
java -cp carpet-jdbc.jar com.jerolba.carpet.jdbc.cli.DynamicJdbcExportCli \
  --properties export.properties \
  --tables tables.txt
```

**4. Sample output:**

```
Exported 1/3 tables, 15000 rows, 7500 rows/sec
Exported 2/3 tables, 28000 rows, 9333 rows/sec
Exported 3/3 tables, 35000 rows, 8750 rows/sec
Export completed in 4.2 seconds. Total rows: 35000, Average throughput: 8333 rows/sec
```

### Schema Analysis

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

## Adding Support for New Databases

The JDBC exporter is designed to be extensible. To add support for a new database, follow these patterns:

### 1. Database-Specific Type Mapping

Create a type mapper for your database by extending or customizing the type conversion logic:

```java
// Example: Oracle-specific type mapper
public class OracleTypeMapper {

    public static FieldType mapOracleType(int sqlType, String typeName,
                                          int precision, int scale) {
        switch (sqlType) {
            case Types.NUMERIC:
                // Oracle NUMBER type special handling
                if (scale == 0 && precision <= 10) {
                    return FieldTypes.INT;
                } else if (scale == 0 && precision <= 19) {
                    return FieldTypes.LONG;
                } else {
                    return FieldTypes.decimal(precision, scale);
                }

            case Types.TIMESTAMP:
                // Oracle TIMESTAMP WITH TIME ZONE
                if (typeName.contains("WITH TIME ZONE")) {
                    return FieldTypes.INSTANT;
                }
                return FieldTypes.LOCAL_DATE_TIME;

            case Types.OTHER:
                // Oracle-specific types (XMLTYPE, JSON, etc.)
                if ("XMLTYPE".equals(typeName)) {
                    return FieldTypes.STRING;
                }
                if ("JSON".equals(typeName)) {
                    return FieldTypes.STRING;
                }
                break;

            default:
                return null; // Fallback to default handling
        }
    }
}
```

### 2. Value Conversion Strategy

Implement custom value converters for database-specific types:

```java
// Example: Converting Oracle CLOB to String
public class OracleValueConverter {

    public static Object convertValue(ResultSet rs, int columnIndex,
                                      int sqlType, String typeName)
            throws SQLException {

        // Handle CLOB
        if (sqlType == Types.CLOB) {
            Clob clob = rs.getClob(columnIndex);
            if (clob != null) {
                try (Reader reader = clob.getCharacterStream()) {
                    return readAll(reader);
                }
            }
            return null;
        }

        // Handle BLOB as byte array
        if (sqlType == Types.BLOB) {
            Blob blob = rs.getBlob(columnIndex);
            if (blob != null) {
                return blob.getBytes(1, (int) blob.length());
            }
            return null;
        }

        // Handle Oracle spatial types (SDO_GEOMETRY)
        if ("SDO_GEOMETRY".equals(typeName)) {
            Object obj = rs.getObject(columnIndex);
            // Convert to WKT or GeoJSON string
            return convertSdoGeometryToWkt(obj);
        }

        return null; // Use default handling
    }
}
```

### 3. Database-Specific Exporter

Create a specialized exporter that incorporates your custom mappings:

```java
public class OracleJdbcExporter {

    public static void exportToParquet(
            Connection connection,
            String sqlQuery,
            File outputFile) throws SQLException, IOException {

        try (PreparedStatement statement = connection.prepareStatement(sqlQuery);
             ResultSet resultSet = statement.executeQuery()) {

            // Create model factory with Oracle-specific mappings
            WriteModelFactory<Map> modelFactory =
                createOracleModelFactory(resultSet.getMetaData());

            try (CarpetWriter<Map> writer = new CarpetWriter.Builder<>(
                    new FileSystemOutputFile(outputFile),
                    Map.class)
                    .withWriteRecordModel(modelFactory)
                    .build()) {

                // Stream with Oracle-specific value conversion
                Stream<Map> records = convertOracleResultSet(resultSet);
                writer.write(records);
            }
        }
    }

    private static WriteModelFactory<Map> createOracleModelFactory(
            ResultSetMetaData metaData) throws SQLException {

        WriteRecordModelType<Map> model =
            WriteRecordModelType.writeRecordModel(Map.class);

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnLabel(i);
            int sqlType = metaData.getColumnType(i);
            String typeName = metaData.getColumnTypeName(i);
            int precision = metaData.getPrecision(i);
            int scale = metaData.getScale(i);

            // Try Oracle-specific mapping first
            FieldType fieldType = OracleTypeMapper.mapOracleType(
                sqlType, typeName, precision, scale);

            // Fallback to standard JDBC mapping
            if (fieldType == null) {
                fieldType = standardJdbcTypeMapping(sqlType, precision, scale);
            }

            final int columnIndex = i;
            model.withField(columnName, fieldType,
                map -> map.get(columnName));
        }

        return model::build;
    }
}
```

### 4. Testing Pattern

Create comprehensive tests for your database adapter:

```java
@Testcontainers
class OracleJdbcExporterTest {

    @Container
    private static final OracleContainer oracle =
        new OracleContainer("gvenzl/oracle-xe:21-slim")
            .withDatabaseName("testdb")
            .withUsername("test")
            .withPassword("test");

    @Test
    void exportOracleNumberTypes() throws Exception {
        try (Connection conn = oracle.createConnection("")) {
            // Setup schema
            conn.createStatement().execute(
                "CREATE TABLE numbers_test (" +
                "  small_int NUMBER(5,0), " +
                "  big_int NUMBER(15,0), " +
                "  decimal_val NUMBER(10,2), " +
                "  float_val NUMBER(5,2)" +
                ")"
            );

            // Insert test data
            conn.createStatement().execute(
                "INSERT INTO numbers_test VALUES (123, 123456789, 123.45, 12.34)"
            );

            // Export
            File output = File.createTempFile("oracle", ".parquet");
            OracleJdbcExporter.exportToParquet(
                conn,
                "SELECT * FROM numbers_test",
                output
            );

            // Verify
            try (CarpetReader<Map> reader =
                    new CarpetReader<>(output, Map.class)) {
                Map record = reader.stream().findFirst().orElseThrow();
                assertEquals(123, record.get("small_int"));
                assertEquals(123456789L, record.get("big_int"));
                // ... more assertions
            }
        }
    }

    @Test
    void exportOracleLobTypes() throws Exception {
        // Test CLOB and BLOB handling
    }

    @Test
    void exportOracleDateTypes() throws Exception {
        // Test TIMESTAMP, DATE, TIMESTAMP WITH TIME ZONE
    }
}
```

### 5. Integration Checklist

When adding a new database adapter, ensure you handle:

- ✅ **Numeric Types**: INTEGER, BIGINT, DECIMAL with proper precision/scale
- ✅ **String Types**: VARCHAR, TEXT, CLOB with character encoding
- ✅ **Date/Time Types**: DATE, TIMESTAMP, TIME with timezone awareness
- ✅ **Binary Types**: BLOB, VARBINARY, BYTEA with size limits
- ✅ **Boolean Types**: BOOLEAN, BIT with database-specific representations
- ✅ **Special Types**: JSON, XML, UUID, ENUM with serialization strategy
- ✅ **Array Types**: Array columns with element type handling
- ✅ **NULL Handling**: Nullable vs non-nullable columns
- ✅ **Character Encoding**: UTF-8, UTF-16, database-specific encodings
- ✅ **Connection Properties**: Fetch size, batch size, timeout settings

### 6. Common Patterns

**Pattern 1: Type Detection**
```java
// Always check type name in addition to SQL type code
if (sqlType == Types.OTHER) {
    switch (typeName.toUpperCase()) {
        case "JSON": return FieldTypes.STRING;
        case "UUID": return FieldTypes.STRING;
        case "GEOMETRY": return FieldTypes.STRING;
    }
}
```

**Pattern 2: Safe Value Extraction**
```java
// Always handle potential nulls
Object value = rs.getObject(columnIndex);
if (rs.wasNull()) {
    return null;
}
return convertValue(value, targetType);
```

**Pattern 3: Fallback Strategy**
```java
// Provide graceful degradation
try {
    return mapDatabaseSpecificType(sqlType, typeName);
} catch (Exception e) {
    logger.warn("Using String fallback for type: " + typeName);
    return FieldTypes.STRING;
}
```

### 7. Performance Optimization

For large datasets, consider:

```java
// Streaming with cursor
statement.setFetchSize(1000); // Database-specific optimal value
statement.setFetchDirection(ResultSet.FETCH_FORWARD);

// Connection tuning
properties.setProperty("defaultRowPrefetch", "1000");
properties.setProperty("useCursors", "true");

// Parallel processing for multi-table exports
ExecutorService executor = Executors.newFixedThreadPool(4);
tables.parallelStream().forEach(table ->
    exportTable(connection, table, outputDir)
);
```

### Contributing Database Adapters

To contribute a new database adapter:

1. Add JDBC driver dependency to `build.gradle`
2. Create type mapper in `com.jerolba.carpet.jdbc.adapters` package
3. Add comprehensive test suite using Testcontainers
4. Update this README with database-specific documentation
5. Submit PR with examples and integration tests

See existing adapters (PostgreSQL, MySQL, SQLite) for reference implementations.
