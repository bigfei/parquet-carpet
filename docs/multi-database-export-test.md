# Multi-Database Parquet Export Test

This document describes the `MultiDatabaseTableExportTest` test class that exports GaussDB database tables to Parquet files.

## Overview

The test automates the process of exporting table data from two GaussDB database schemas to Parquet files. It mirrors the functionality of the Python script `generate_sql.py` but actually performs the data export rather than just generating SQL statements.

## Architecture

### Database Categorization

The test automatically routes tables to the correct database based on naming conventions:

| Database Schema | Table Prefix | Examples |
|-----------------|--------------|----------|
| `sit_suncbs_acctdb` (Accounting) | `kfa*`, `kgl*` | `kfab_gl_vchr`, `kfah_gl_vchr`, `kfap_acctg_sbj` |
| `sit_suncbs_coredb` (Core) | All others | `kapp_txn_sys_dt`, `kbrp_org`, `kcfb_*`, `kdpa_*`, `klna_*` |

This logic is implemented in the `categorizeTables()` method, matching the Python script's `get_schema_for_table()` function.

### Data Flow

```
table-list.txt
     ↓
categorizeTables()
     ↓
  ┌──────┴──────┐
  ↓             ↓
acctdb        coredb
tables        tables
  ↓             ↓
export to     export to
parquets/     parquets/
```

## Configuration

### Environment Variables

The test uses environment variables for database credentials and configuration:

#### Required Variables

**Accounting Database:**
```bash
GAUSSDB_ACCTDB_URL       # Default: jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_acctdb?currentSchema=sit_suncbs_acctdb
GAUSSDB_ACCTDB_USERNAME  # No default - REQUIRED
GAUSSDB_ACCTDB_PASSWORD  # No default - REQUIRED
```

**Core Database:**
```bash
GAUSSDB_COREDB_URL       # Default: jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb?currentSchema=sit_suncbs_coredb
GAUSSDB_COREDB_USERNAME  # No default - REQUIRED
GAUSSDB_COREDB_PASSWORD  # No default - REQUIRED
```

#### Optional Variables

```bash
GAUSSDB_LGL_PERN_CODE    # Default: 6666 (legal person code filter)
```

### Setup Script

Use the provided setup script to configure environment variables:

```bash
# 1. Copy the template
cp carpet-jdbc/scripts/setup-env.sh carpet-jdbc/scripts/setup-env-local.sh

# 2. Edit setup-env-local.sh with your actual credentials
# 3. Source the file
source carpet-jdbc/scripts/setup-env-local.sh
```

**Note:** `setup-env-local.sh` is in `.gitignore` to prevent credential leakage.

## Running the Test

### Prerequisites

1. GaussDB instances running and accessible
2. Database users with SELECT permissions on all tables
3. Table list file at `carpet-jdbc/scripts/table-list.txt`
4. Environment variables configured

### Execution

```bash
# Run the multi-database export test
./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest

# Run with specific Java version
./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest -Dorg.gradle.java.home=/path/to/jdk17

# Run with verbose output
./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest --info

# Run all carpet-jdbc tests
./gradlew :carpet-jdbc:test
```

## Export Process

For each table in `table-list.txt`, the test:

1. **Categorizes** the table (acctdb vs coredb)
2. **Checks** if the database connection is available
3. **Queries** the table: `SELECT a.* FROM {table} a WHERE a.lgl_pern_code = '{lgl_pern_code}'`
4. **Validates** that the table has data
5. **Exports** to `carpet-jdbc/parquets/{table_name}.parquet`
6. **Reports** success, skip, or error status

### Export Configuration

The test uses `DynamicExportConfig` with:

```java
.withFetchSize(1000)                           // Fetch 1000 rows at a time
.withBatchSize(10000)                          // Write 10000 rows per batch
.withCompressionCodec(CompressionCodecName.SNAPPY)  // SNAPPY compression
.withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME)  // Preserve column names
```

### SQL Query Pattern

Each table is queried with:

```sql
SELECT a.*
FROM {table_name} a
WHERE a.lgl_pern_code = '{lgl_pern_code}'
```

This mirrors the `expr_sql_sentc` column in the Python script's INSERT statements.

## Output

### Parquet Files

Files are written to `carpet-jdbc/parquets/`:

- `kapp_txn_sys_dt.parquet`
- `kbrp_org.parquet`
- `kfab_gl_vchr.parquet`
- `klna_loan_acct.parquet`
- ... (one file per table)

### Console Output

The test provides detailed progress and summary:

```
📁 Parquet output directory: /path/to/carpet-jdbc/parquets
✅ Connected to Accounting DB at: jdbc:gaussdb://...
✅ Connected to Core DB at: jdbc:gaussdb://...
📋 Found 65 tables in table-list.txt
📊 Table distribution:
   - Accounting DB (kfa*, kgl*): 3 tables
   - Core DB (others): 62 tables

🔄 Exporting Core DB tables...
  ✅ kapp_txn_sys_dt → 1234 rows
  ✅ kbrp_org → 567 rows
  ⏭️  kcfb_corp_aml_info (empty or no access)
  ...

📈 CORE DB Summary: 45 success, 15 skipped, 2 errors

🔄 Exporting Accounting DB tables...
  ✅ kfab_gl_vchr → 98765 rows
  ...

================================================================================
📊 EXPORT SUMMARY
================================================================================
Total tables processed: 65
  ✅ Successful exports: 48
  ⏭️  Skipped (empty/no access): 15
  ❌ Errors: 2
  📦 Total rows exported: 123456
  📁 Output directory: /path/to/carpet-jdbc/parquets
================================================================================
```

## Table List Format

The `table-list.txt` file contains one table name per line:

```
kapp_txn_sys_dt
kbrp_org
kcfb_corp_aml_info
...
kfab_gl_vchr
kfah_gl_vchr
kfap_acctg_sbj
```

- Blank lines are ignored
- Lines starting with `#` are treated as comments
- Table names are trimmed of whitespace

## Comparison with Python Script

| Feature | Python Script (`generate_sql.py`) | Java Test (`MultiDatabaseTableExportTest`) |
|---------|-----------------------------------|-------------------------------------------|
| **Purpose** | Generate SQL INSERT statements | Actually export data to Parquet files |
| **Database Logic** | Same categorization (kfa/kgl = acctdb) | Same categorization (kfa/kgl = acctdb) |
| **SQL Pattern** | Generates SQL with `lgl_pern_code` filter | Executes SQL with `lgl_pern_code` filter |
| **Output** | SQL files (acct.sql, core.sql) | Parquet files (*.parquet) |
| **Configuration** | Hardcoded in script | Environment variables |
| **Execution** | `python generate_sql.py` | `./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest` |

## Error Handling

The test handles various scenarios gracefully:

### Skipped Tables

Tables are skipped (not errors) when:
- Table doesn't exist in the database
- User lacks SELECT permission
- No rows match the `lgl_pern_code` filter
- Table is genuinely empty

### Error Cases

Errors are reported for:
- SQL syntax errors
- Network connectivity issues
- Schema mismatches
- Parquet writing failures

### Connection Issues

If a database connection fails:
- Only that database's tables are affected
- The test continues with the other database if available
- Clear warnings are printed to console

## Troubleshooting

### Connection Refused

```
⚠️  Could not connect to Core DB: Connection refused
```

**Solutions:**
- Verify GaussDB is running
- Check the URL and port
- Verify network connectivity
- Check firewall rules

### Authentication Failed

```
⚠️  Could not connect to Accounting DB: Invalid username or password
```

**Solutions:**
- Verify credentials in environment variables
- Check user exists in database
- Verify password hasn't expired

### Permission Denied

```
❌ kapp_txn_sys_dt → Error: SELECT command denied to user
```

**Solutions:**
- Grant SELECT permission: `GRANT SELECT ON kapp_txn_sys_dt TO username;`
- Or grant schema-level access: `GRANT SELECT ON ALL TABLES IN SCHEMA sit_suncbs_coredb TO username;`

### Empty Result Set

```
⏭️  kcfb_corp_aml_info (empty or no access)
```

This is normal behavior when:
- No rows match the `lgl_pern_code` filter
- Table is empty
- Table doesn't exist

## Advanced Usage

### Custom Legal Person Code

```bash
export GAUSSDB_LGL_PERN_CODE="8888"
./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest
```

### Running Against Different Databases

```bash
# Production databases
export GAUSSDB_ACCTDB_URL="jdbc:gaussdb://prod-db:5432/prod_acctdb?currentSchema=prod_acctdb"
export GAUSSDB_COREDB_URL="jdbc:gaussdb://prod-db:5432/prod_coredb?currentSchema=prod_coredb"

# Test databases
export GAUSSDB_ACCTDB_URL="jdbc:gaussdb://test-db:5432/test_acctdb?currentSchema=test_acctdb"
export GAUSSDB_COREDB_URL="jdbc:gaussdb://test-db:5432/test_coredb?currentSchema=test_coredb"
```

### Testing Subset of Tables

Create a custom table list:

```bash
# Create subset
head -10 carpet-jdbc/scripts/table-list.txt > carpet-jdbc/scripts/table-list-test.txt

# Modify test to use custom file (requires code change)
# Or manually edit table-list.txt temporarily
```

## Integration with CI/CD

The test is designed to be skipped gracefully in CI environments where databases aren't available:

```yaml
# GitHub Actions example
- name: Run Database Export Tests
  env:
    GAUSSDB_ACCTDB_USERNAME: ${{ secrets.DB_USER }}
    GAUSSDB_ACCTDB_PASSWORD: ${{ secrets.DB_PASS }}
    GAUSSDB_COREDB_USERNAME: ${{ secrets.DB_USER }}
    GAUSSDB_COREDB_PASSWORD: ${{ secrets.DB_PASS }}
  run: ./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest
```

Without credentials, the test will print warnings and skip execution.

## Performance Considerations

### Batch Sizing

Current settings:
- Fetch size: 1,000 rows (memory vs. network balance)
- Batch size: 10,000 rows (Parquet row group size)

For large tables (>10M rows), consider:
```java
.withFetchSize(5000)   // Larger fetch for fewer round trips
.withBatchSize(100000) // Larger row groups for better compression
```

### Compression

SNAPPY compression provides:
- Fast compression/decompression
- Moderate compression ratio (~2-3x)
- Good CPU efficiency

Alternatives:
- GZIP: Better compression, slower
- UNCOMPRESSED: Fastest, largest files
- ZSTD: Best compression, moderate speed

### Parallelization

Currently, tables are exported sequentially. For faster export:
- Modify test to use parallel streams
- Be careful with database connection pooling
- Monitor database server load

## Related Files

- `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/MultiDatabaseTableExportTest.java` - Test implementation
- `carpet-jdbc/scripts/table-list.txt` - List of tables to export
- `carpet-jdbc/scripts/generate_sql.py` - Python script for SQL generation (reference)
- `carpet-jdbc/scripts/setup-env.sh` - Environment variable setup template
- `carpet-jdbc/parquets/` - Output directory for Parquet files
- `carpet-jdbc/parquets/README.md` - Quick reference guide

## See Also

- [carpet-jdbc README](../README.md) - JDBC adapters overview
- [DynamicJdbcExporter Javadoc](../src/main/java/com/jerolba/carpet/jdbc/DynamicJdbcExporter.java) - Core export API
- [GaussDB Test](../src/test/java/com/jerolba/carpet/jdbc/DynamicJdbcExporterGaussDBTest.java) - Individual GaussDB tests
