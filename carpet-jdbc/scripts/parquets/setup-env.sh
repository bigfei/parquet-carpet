#!/bin/bash
#
# Environment setup script for MultiDatabaseTableExportTest
#
# This script sets up environment variables needed to run the multi-database
# table export test. Copy this file to setup-env-local.sh and customize with
# your actual database credentials.
#
# Usage:
#   source carpet-jdbc/scripts/setup-env.sh
#   ./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest

# Accounting Database (sit_suncbs_acctdb)
# Tables: kfa*, kgl* (e.g., kfab_gl_vchr, kfah_gl_vchr, kfap_acctg_sbj)
export GAUSSDB_ACCTDB_URL="jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_acctdb?currentSchema=sit_suncbs_acctdb"
export GAUSSDB_ACCTDB_USERNAME="your_acctdb_username"
export GAUSSDB_ACCTDB_PASSWORD="your_acctdb_password"

# Core Database (sit_suncbs_coredb)
# Tables: all others (e.g., kapp_txn_sys_dt, kbrp_org, kcfb_*, kdpa_*, klna_*, etc.)
export GAUSSDB_COREDB_URL="jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb?currentSchema=sit_suncbs_coredb"
export GAUSSDB_COREDB_USERNAME="your_coredb_username"
export GAUSSDB_COREDB_PASSWORD="your_coredb_password"

# Optional: Legal Person Code filter (default: 6666)
# This is used in WHERE clause: WHERE a.lgl_pern_code = '${GAUSSDB_LGL_PERN_CODE}'
export GAUSSDB_LGL_PERN_CODE="6666"

# Verify environment is set up
echo "Environment configured:"
echo "  Accounting DB URL: ${GAUSSDB_ACCTDB_URL}"
echo "  Accounting DB User: ${GAUSSDB_ACCTDB_USERNAME}"
echo "  Core DB URL: ${GAUSSDB_COREDB_URL}"
echo "  Core DB User: ${GAUSSDB_COREDB_USERNAME}"
echo "  Legal Person Code: ${GAUSSDB_LGL_PERN_CODE}"
echo ""
echo "Ready to run: ./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest"
