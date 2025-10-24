#!/bin/bash
#
# Example: Run the multi-database table export test
#
# This script demonstrates the complete workflow for exporting
# GaussDB tables to Parquet files using the MultiDatabaseTableExportTest

set -e  # Exit on error

echo "════════════════════════════════════════════════════════════════"
echo "Multi-Database Table Export - Example Usage"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Step 1: Check environment setup
echo "Step 1: Checking environment configuration..."
echo ""

if [ -z "$GAUSSDB_ACCTDB_USERNAME" ] || [ -z "$GAUSSDB_COREDB_USERNAME" ]; then
    echo "⚠️  Environment variables not set!"
    echo ""
    echo "Please set up your environment first:"
    echo ""
    echo "  1. Copy the template:"
    echo "     cp carpet-jdbc/scripts/setup-env.sh carpet-jdbc/scripts/setup-env-local.sh"
    echo ""
    echo "  2. Edit with your credentials:"
    echo "     nano carpet-jdbc/scripts/setup-env-local.sh"
    echo ""
    echo "  3. Source the file:"
    echo "     source carpet-jdbc/scripts/setup-env-local.sh"
    echo ""
    echo "  4. Run this script again"
    echo ""
    exit 1
fi

echo "✅ Environment configured:"
echo "   Accounting DB: $GAUSSDB_ACCTDB_USERNAME @ ${GAUSSDB_ACCTDB_URL:-default}"
echo "   Core DB: $GAUSSDB_COREDB_USERNAME @ ${GAUSSDB_COREDB_URL:-default}"
echo "   Legal Person Code: ${GAUSSDB_LGL_PERN_CODE:-6666}"
echo ""

# Step 2: Check table list
echo "Step 2: Checking table list..."
echo ""

if [ ! -f "carpet-jdbc/scripts/table-list.txt" ]; then
    echo "❌ Table list not found: carpet-jdbc/scripts/table-list.txt"
    exit 1
fi

TABLE_COUNT=$(grep -v "^#" carpet-jdbc/scripts/table-list.txt | grep -v "^$" | wc -l)
echo "✅ Found $TABLE_COUNT tables to export"
echo ""

# Step 3: Ensure output directory exists
echo "Step 3: Preparing output directory..."
echo ""

mkdir -p carpet-jdbc/parquets
echo "✅ Output directory ready: carpet-jdbc/parquets"
echo ""

# Step 4: Run the test
echo "Step 4: Running export test..."
echo "════════════════════════════════════════════════════════════════"
echo ""

./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest --info

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "Export Complete!"
echo "════════════════════════════════════════════════════════════════"
echo ""

# Step 5: Show results
echo "Step 5: Export results..."
echo ""

if [ -d "carpet-jdbc/parquets" ]; then
    PARQUET_COUNT=$(find carpet-jdbc/parquets -name "*.parquet" 2>/dev/null | wc -l)
    echo "📦 Generated $PARQUET_COUNT Parquet files"
    echo ""

    if [ $PARQUET_COUNT -gt 0 ]; then
        echo "Sample files:"
        find carpet-jdbc/parquets -name "*.parquet" | head -5 | while read -r file; do
            SIZE=$(ls -lh "$file" | awk '{print $5}')
            FILENAME=$(basename "$file")
            echo "  📄 $FILENAME ($SIZE)"
        done

        if [ $PARQUET_COUNT -gt 5 ]; then
            echo "  ... and $(($PARQUET_COUNT - 5)) more files"
        fi
    fi
fi

echo ""
echo "════════════════════════════════════════════════════════════════"
echo "Next Steps:"
echo "════════════════════════════════════════════════════════════════"
echo ""
echo "View exported files:"
echo "  ls -lh carpet-jdbc/parquets/"
echo ""
echo "Read a Parquet file (requires parquet-tools):"
echo "  parquet-tools head carpet-jdbc/parquets/kapp_txn_sys_dt.parquet"
echo ""
echo "Clean up:"
echo "  rm -f carpet-jdbc/parquets/*.parquet"
echo ""
echo "Re-run export:"
echo "  ./gradlew :carpet-jdbc:test --tests MultiDatabaseTableExportTest"
echo ""
