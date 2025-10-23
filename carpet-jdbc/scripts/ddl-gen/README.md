# DDL Extraction Tool

This directory contains a Python script to extract individual table DDL statements from large SQL dump files.

## Files

- **`extract-ddl.py`**: Main extraction script
- **`core_db.sql`**: Source SQL file containing core database table definitions (uat_suncbs_coredb schema)
- **`acct_db.sql`**: Source SQL file containing account database table definitions (uat_suncbs_acctdb schema)
- **`../table-list.txt`**: List of tables to extract (one table name per line)
- **`table_ddls.zip`**: Output ZIP file containing individual SQL files for each table

## Usage

```bash
# Make the script executable (first time only)
chmod +x extract-ddl.py

# Run the extraction
python3 extract-ddl.py
```

## How It Works

1. **Reads table list**: The script reads table names from `../table-list.txt`

2. **Routes to correct source**:
   - Tables starting with `kfa*` or `kgl*` → extracted from `acct_db.sql`
   - All other tables → extracted from `core_db.sql`

3. **Extracts complete DDL**: For each table, extracts:
   - DROP TABLE statement
   - SET search_path
   - CREATE TABLE with all columns and constraints
   - CREATE INDEX statements
   - ALTER TABLE constraints
   - COMMENT statements (if present)

4. **Creates ZIP archive**: All individual SQL files are packaged into `table_ddls.zip`

## Example Output

```
Reading table list from ../table-list.txt...
Found 64 tables to extract

Processing kapp_txn_sys_dt from core_db.sql...
  ✓ Extracted kapp_txn_sys_dt
Processing kfab_gl_vchr from acct_db.sql...
  ✓ Extracted kfab_gl_vchr
...

============================================================
Extraction Summary:
  Total tables requested: 64
  Successfully extracted: 64
  Missing or not found: 0

✓ Created: table_ddls.zip
============================================================
```

## Extracting Individual Files from ZIP

```bash
# List all files in the archive
unzip -l table_ddls.zip

# Extract a specific table
unzip table_ddls.zip kfab_gl_vchr.sql

# Extract all files
unzip table_ddls.zip

# View a file without extracting
unzip -p table_ddls.zip kfab_gl_vchr.sql
```

## Requirements

- Python 3.6+
- Standard library only (no external dependencies)

## Script Details

The script uses regular expressions to:
- Locate table definition boundaries in the SQL dump files
- Extract complete DDL blocks including all associated objects
- Preserve formatting and structure of the original DDL

Tables are identified by the pattern:
```
-- ----------------------------
-- Table structure for uat_suncbs_<schema>.<table_name>
-- ----------------------------
```

The extraction continues until the next table definition or end of file.
