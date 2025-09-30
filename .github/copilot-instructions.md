# Carpet: Parquet Serialization Library for Java

## Project Overview

Carpet is a Java library that simplifies Parquet file serialization/deserialization using Java records. The project provides a high-level API wrapping Apache Parquet's complex internals while minimizing transitive dependencies.

**Core modules:**
- `carpet-record`: Main library with writer/reader APIs
- `carpet-jdbc`: JDBC-to-Parquet export adapters for multiple databases (PostgreSQL, MySQL, SQLite, DuckDB)

## Architecture

### Key Components

**Public API Layer** (`com.jerolba.carpet`):
- `CarpetWriter<T>`: Main writer class with fluent builder pattern for Parquet configuration
- `CarpetReader<T>`: Iterable/streamable reader with configuration methods
- `CarpetParquetWriter<T>` / `CarpetParquetReader<T>`: Lower-level alternatives exposing full ParquetWriter/Reader APIs
- `CarpetRecordGenerator`: Utility to generate Java record code from existing Parquet files

**Internal Architecture** (`com.jerolba.carpet.impl`):
- `write/`: Java record → Parquet schema mapping and writing logic
- `read/`: Parquet schema → Java record conversion and reading logic
- Schema validation and field mapping with configurable strategies
- Reflection-based record component inspection

**Model Package** (`com.jerolba.carpet.model`):
- `WriteRecordModelType<T>`: Write-side type representation with field accessors
- Sealed interface hierarchy: `FieldType` → `BasicType`, `ListType`, `MapType`, `RecordType`

### Data Flow

**Writing:** `Java Record` → `JavaRecord2WriteModel` → `WriteRecordModel2Schema` → `ParquetWriter`
**Reading:** `ParquetFile` → `SchemaFilter` (projection) → `ParquetReader` → `Java Record`

## Critical Patterns

### 1. Java Records as First-Class Citizens

All data models MUST be Java records (not POJOs). Record components are introspected via reflection:

```java
record MyData(long id, String name, @NotNull List<String> tags) {}

// Usage
try (var writer = new CarpetWriter<>(outputStream, MyData.class)) {
    writer.write(data);
}
```

### 2. Annotations for Schema Control

- `@NotNull`: Marks fields/collection elements as required (REQUIRED repetition in Parquet)
- `@Alias("columnName")`: Maps field to different Parquet column name
- `@PrecisionScale(precision, scale)`: Per-field BigDecimal configuration
- `@ParquetJson`, `@ParquetBson`, `@ParquetGeometry`, `@ParquetGeography`: Logical type annotations

### 3. Builder Pattern for Configuration

Both writer and reader use extensive builder configuration:

```java
var writer = new CarpetWriter.Builder<>(outputFile, MyRecord.class)
    .withCompressionCodec(CompressionCodecName.GZIP)
    .withPageRowCountLimit(100_000)
    .withDefaultDecimal(20, 3)
    .withDefaultTimeUnit(TimeUnit.MICROS)
    .withBloomFilterEnabled("name", true)
    .build();
```

### 4. Schema Validation Strategies

`SchemaValidation` and `SchemaFilter` handle schema mismatches with configurable behaviors:
- `failOnMissingColumn`: Throw if Parquet file lacks expected fields
- `failNarrowingPrimitiveConversion`: Prevent lossy int64→int32 conversions
- `failOnNullForPrimitives`: Reject nullable Parquet columns for `@NotNull` primitives

### 5. Column Naming Strategy

`ColumnNamingStrategy` enum controls field name conversion:
- `FIELD_NAME`: Use Java field names as-is
- `SNAKE_CASE`: Convert camelCase → snake_case

Set globally via builder: `.withColumnNamingStrategy(ColumnNamingStrategy.SNAKE_CASE)`

## Build & Test

**Build:** `./gradlew build` (runs tests + checkstyle + jacoco coverage)
**Test only:** `./gradlew test`
**Assemble JARs:** `./gradlew assemble`

**Key requirements:**
- Java 17+ toolchain required
- All code must include Apache 2.0 license header (enforced by `com.github.hierynomus.license` plugin)
- Tests use JUnit 5 with nested test classes for organization

## Development Conventions

### Code Organization

- Use sealed interfaces where type hierarchies are fixed (`FieldType`, `CollectionType`)
- Internal implementation classes in `impl/` package are not public API
- Test classes mirror production structure with extensive `@Nested` test organization

### Dependency Management

The project aggressively excludes Hadoop/Parquet transitive dependencies to keep the library lightweight. When adding dependencies to `carpet-record/build.gradle`, review the massive `exclude` blocks in `configurations.implementation` to maintain minimal footprint.

### Testing Patterns

Test record definitions are inlined in test methods (not top-level classes):

```java
@Test
void testSomething() {
    record TestRecord(@NotNull String id, List<Integer> values) {}
    // test using TestRecord
}
```

Use `writeRecordModel()` fluent API for programmatic schema construction in tests.

## Common Tasks

**Generate record from Parquet file:**
```java
List<String> recordCode = CarpetRecordGenerator.generateCode("file.parquet");
```

**Handle BigDecimal scale mismatches:**
```java
.withDefaultDecimal(20, 3)
.withBigDecimalScaleAdjustment(RoundingMode.HALF_UP)
```

**Read with schema flexibility:**
```java
new CarpetReader<>(file, MyRecord.class)
    .withFailOnMissingColumn(false)
    .withFieldMatchingStrategy(FieldMatchingStrategy.BEST_EFFORT)
    .toList();
```

## JDBC Adapters (`carpet-jdbc` module)

### Purpose

The `carpet-jdbc` module provides dynamic JDBC-to-Parquet export without predefined Java records. It uses `Map<String, Object>` as the data model and generates Parquet schemas from ResultSet metadata.

### Core Components

- **`DynamicJdbcExporter`**: Main entry point for exporting JDBC data
- **`DynamicExportConfig`**: Configuration for batch size, compression, fetch size
- Database-specific type mappings for PostgreSQL, MySQL, SQLite, DuckDB

### Adding New Database Adapters

When adding support for a new database:

1. **Type Mapping Strategy**: Map SQL types to Parquet FieldTypes
   - Handle numeric types (precision/scale for DECIMAL)
   - String types (CLOB, TEXT with encoding)
   - Date/time types (timezone awareness)
   - Binary types (BLOB size limits)
   - Database-specific types (JSON, XML, UUID, arrays, spatial)

2. **Value Conversion**: Implement safe extraction from ResultSet
   - Check `rs.wasNull()` after each `getObject()`
   - Convert LOBs (CLOB/BLOB) to strings/byte arrays
   - Handle vendor-specific object types

3. **Testing Pattern**: Use Testcontainers for integration tests
   - Create test schema with diverse types
   - Insert test data covering edge cases
   - Export to Parquet and verify roundtrip

4. **Common Patterns**:
   - Always check type name in addition to SQL type code
   - Provide fallback to STRING for unknown types
   - Optimize with appropriate fetch sizes (database-specific)

### Database-Specific Considerations

**PostgreSQL**: JSON/JSONB → String, arrays → String representation, UUID → String
**MySQL**: TINYINT/SMALLINT → INT32, ENUM/SET → String, JSON → String
**SQLite**: REAL → FLOAT (from Double), BLOB → byte array, dynamic typing
**DuckDB**: STRUCT → JSON String, standard types map directly

### Example: Oracle Adapter Pattern

```java
// 1. Type mapper
FieldType mapOracleType(int sqlType, String typeName, int precision, int scale) {
    if (sqlType == Types.NUMERIC && scale == 0 && precision <= 10) {
        return FieldTypes.INT;
    }
    // Handle Oracle NUMBER, TIMESTAMP WITH TIME ZONE, XMLTYPE, etc.
}

// 2. Value converter  
Object convertOracleValue(ResultSet rs, int idx, int sqlType, String typeName) {
    if (sqlType == Types.CLOB) {
        return readClob(rs.getClob(idx));
    }
    // Handle BLOB, SDO_GEOMETRY, etc.
}

// 3. Tests with Testcontainers
@Container
OracleContainer oracle = new OracleContainer("gvenzl/oracle-xe:21-slim");
```

## Documentation

Full documentation site: https://carpet.jerolba.com/

Key doc pages:
- Column mapping: `/advanced/column-mapping/`
- Data types: `/advanced/data-types/`
- Configuration: `/advanced/configuration/`
- Schema mismatches: `/advanced/schema-mismatch/`
- JDBC adapters: `carpet-jdbc/README.md`
