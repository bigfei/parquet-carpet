#!/usr/bin/env python3
"""
DDL Extraction Script

This script extracts DDL statements for specific tables from SQL dump files
and creates individual SQL files for each table, packaged in a ZIP archive.

Usage:
    python extract-ddl.py

Input files:
    - core_db.sql: DDL for core database tables
    - acct_db.sql: DDL for account database tables
    - table-list.txt: List of tables to extract

Output:
    - table_ddls.zip: ZIP archive containing individual table DDL files
"""

import re
import zipfile
from pathlib import Path
from typing import Dict, List, Optional, Tuple


def read_table_list(table_list_file: str) -> List[str]:
    """Read the list of tables to extract from table-list.txt"""
    with open(table_list_file, 'r') as f:
        tables = [line.strip() for line in f if line.strip()]
    return tables


def determine_source_file(table_name: str) -> str:
    """Determine which SQL file contains the table DDL"""
    if table_name.startswith('kfa') or table_name.startswith('kgl'):
        return 'acct_db.sql'
    else:
        return 'core_db.sql'


def extract_table_ddl(sql_file: str, table_name: str) -> Optional[str]:
    """
    Extract the DDL for a specific table from the SQL file.

    Returns the complete DDL including:
    - DROP TABLE statement
    - SET search_path
    - CREATE TABLE statement
    - All indexes
    - ALTER TABLE constraints
    - COMMENT statements (if any)
    """
    with open(sql_file, 'r', encoding='utf-8') as f:
        content = f.read()

    # Pattern to match table section header
    # Example: -- Table structure for uat_suncbs_acctdb.kfab_prod_bal_agrgtd
    table_header_pattern = rf'^-- ----------------------------\s*\n-- Table structure for (?:uat_suncbs_(?:acct|core)db\.)?{re.escape(table_name)}\s*\n-- ----------------------------'

    # Find the start of the table DDL
    match = re.search(table_header_pattern, content, re.MULTILINE | re.IGNORECASE)
    if not match:
        return None

    start_pos = match.start()

    # Find the end of the table DDL (next table header or end of file)
    next_table_pattern = r'^-- ----------------------------\s*\n-- Table structure for'
    next_match = re.search(next_table_pattern, content[match.end():], re.MULTILINE)

    if next_match:
        end_pos = match.end() + next_match.start()
    else:
        end_pos = len(content)

    # Extract the DDL section
    ddl = content[start_pos:end_pos].strip()

    # Add DROP TABLE at the beginning if not present
    if not re.search(rf'DROP TABLE.*{re.escape(table_name)}', ddl, re.IGNORECASE):
        schema_name = 'uat_suncbs_acctdb' if 'acct_db' in sql_file else 'uat_suncbs_coredb'
        drop_statement = f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}";\n\n'
        # Insert after the header comment
        header_end = ddl.find('----------------------------', ddl.find('----------------------------') + 1)
        if header_end != -1:
            header_end = ddl.find('\n', header_end) + 1
            ddl = ddl[:header_end] + drop_statement + ddl[header_end:]

    return ddl


def create_zip_with_ddls(tables: List[str], output_zip: str):
    """
    Extract DDLs for all tables and create a ZIP file with individual SQL files.
    """
    script_dir = Path(__file__).parent

    extracted_count = 0
    missing_tables = []

    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for table_name in tables:
            # Determine which source file to use
            source_file = determine_source_file(table_name)
            source_path = script_dir / source_file

            if not source_path.exists():
                print(f"⚠️  Source file not found: {source_file}")
                missing_tables.append(table_name)
                continue

            # Extract the DDL
            print(f"Processing {table_name} from {source_file}...")
            ddl = extract_table_ddl(str(source_path), table_name)

            if ddl:
                # Create SQL file in ZIP
                sql_filename = f"{table_name}.sql"
                zipf.writestr(sql_filename, ddl)
                extracted_count += 1
                print(f"  ✓ Extracted {table_name}")
            else:
                print(f"  ✗ Table {table_name} not found in {source_file}")
                missing_tables.append(table_name)

    # Print summary
    print(f"\n{'='*60}")
    print(f"Extraction Summary:")
    print(f"  Total tables requested: {len(tables)}")
    print(f"  Successfully extracted: {extracted_count}")
    print(f"  Missing or not found: {len(missing_tables)}")

    if missing_tables:
        print(f"\nMissing tables:")
        for table in missing_tables:
            print(f"  - {table}")

    print(f"\n✓ Created: {output_zip}")
    print(f"{'='*60}")


def main():
    """Main function to orchestrate the DDL extraction process"""
    script_dir = Path(__file__).parent

    # Input files
    table_list_file = script_dir.parent / 'table-list.txt'

    # Output file
    output_zip = script_dir / 'table_ddls.zip'

    # Check if table list exists
    if not table_list_file.exists():
        print(f"❌ Error: {table_list_file} not found")
        return 1

    # Read tables to extract
    print(f"Reading table list from {table_list_file}...")
    tables = read_table_list(str(table_list_file))
    print(f"Found {len(tables)} tables to extract\n")

    # Extract and create ZIP
    create_zip_with_ddls(tables, str(output_zip))

    return 0


if __name__ == '__main__':
    exit(main())
