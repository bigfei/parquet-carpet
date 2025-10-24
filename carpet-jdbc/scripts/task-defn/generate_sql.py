#!/usr/bin/env python3
"""
Generate SQL statements for data export definitions based on table list.
Creates DELETE and INSERT statements for each table in table-list.txt.
Tables are categorized by database schema:
- Accounting DB (sit_suncbs_acctdb): tables starting with 'kfa' or 'kgl'
- Core DB (sit_suncbs_coredb): all other tables
"""

def get_schema_for_table(table_name):
    """
    Determine which database schema a table belongs to.

    Args:
        table_name: Name of the table

    Returns:
        Schema name (sit_suncbs_acctdb or sit_suncbs_coredb)
    """
    if table_name.startswith('kfa') or table_name.startswith('kgl'):
        return 'sit_suncbs_acctdb'
    else:
        return 'sit_suncbs_coredb'

def generate_sql_statements(table_list_file, output_dir):
    """
    Read table names from file and generate SQL statements.
    Creates two separate SQL files: one for accounting DB and one for core DB.

    Args:
        table_list_file: Path to file containing table names (one per line)
        output_dir: Directory path for output SQL files
    """
    with open(table_list_file, 'r', encoding='utf-8') as f:
        tables = [line.strip() for line in f if line.strip()]

    acct_statements = []
    core_statements = []
    core_db_count = 0
    acct_db_count = 0

    for table_name in tables:
        # data_expr_id is table name (no suffix based on the example)
        data_expr_id = table_name

        # Determine which schema this table belongs to
        schema_name = get_schema_for_table(table_name)

        # Determine data_expr_grp_code based on database
        # Accounting DB tables use 'acct', Core DB tables use 'core'
        if schema_name == 'sit_suncbs_acctdb':
            data_expr_grp_code = 'acct'
            acct_db_count += 1
            statements_list = acct_statements
        else:
            data_expr_grp_code = 'core'
            core_db_count += 1
            statements_list = core_statements

        # Generate DELETE statement
        delete_stmt = (
            f"delete from kapp_data_expr_defn "
            f"where lgl_pern_code = '6666' "
            f"and data_expr_id = '{data_expr_id}' "
            f"and data_expr_grp_code = '{data_expr_grp_code}' "
            f"and file_tp = 'parquet';"
        )

        # Generate SELECT clause without schema qualification
        select_clause = (
            f"select a.* from {table_name} a "
            f"where a.lgl_pern_code = ''${{lgl_pern_code}}'' "
        )

        # Generate INSERT statement
        insert_stmt = (
            f"INSERT INTO kapp_data_expr_defn "
            f"(lgl_pern_code,data_expr_id,data_expr_grp_code,data_expr_tab_nm,"
            f"expr_sql_sentc,wthr_rgst_tmstp,wthr_concurrent_exec,concurrent_num,"
            f"fld_seprtr,file_tp,file_gen_way,file_nm,file_comps_way,file_comps_type,"
            f"wthr_gen_succ_file,wthr_file_upld,file_local_path,file_remote_path,"
            f"wthr_vld,comt_info,sharding_hash_key) VALUES\n"
            f"\t ('6666','{data_expr_id}','{data_expr_grp_code}','{table_name}','{select_clause}',"
            f"'0','0',5,'@|@','parquet','0','{table_name}','0',NULL,'1','1',"
            f"'{table_name}','{table_name}','1',NULL,NULL);"
        )

        statements_list.append(delete_stmt)
        statements_list.append(insert_stmt)
        statements_list.append("")  # Empty line between table statements

    # Write accounting DB statements to file
    acct_output_file = os.path.join(output_dir, "acct.sql")
    with open(acct_output_file, 'w', encoding='utf-8') as f:
        f.write("-- Generated SQL statements for Accounting DB data export definitions\n")
        f.write("-- Based on table-list.txt\n")
        f.write(f"-- Accounting DB (sit_suncbs_acctdb): {acct_db_count} tables\n\n")
        f.write('\n'.join(acct_statements))

    # Write core DB statements to file
    core_output_file = os.path.join(output_dir, "core.sql")
    with open(core_output_file, 'w', encoding='utf-8') as f:
        f.write("-- Generated SQL statements for Core DB data export definitions\n")
        f.write("-- Based on table-list.txt\n")
        f.write(f"-- Core DB (sit_suncbs_coredb): {core_db_count} tables\n\n")
        f.write('\n'.join(core_statements))

    print(f"Generated SQL statements for {len(tables)} tables")

    print(f"  - Accounting DB (sit_suncbs_acctdb): {acct_db_count} tables")
    print(f"    Output: {acct_output_file}")
    print(f"  - Core DB (sit_suncbs_coredb): {core_db_count} tables")
    print(f"    Output: {core_output_file}")


if __name__ == "__main__":
    import os

    # Get script directory
    script_dir = os.path.dirname(os.path.abspath(__file__))

    # Input file and output directory
    table_list_file = os.path.join(script_dir, "..", "table-list.txt")
    output_dir = script_dir

    # Check if input file exists
    if not os.path.exists(table_list_file):
        print(f"Error: {table_list_file} not found!")
        exit(1)

    # Generate SQL statements
    generate_sql_statements(table_list_file, output_dir)
    print(f"\nYou can now review the generated SQL files in: {output_dir}")
