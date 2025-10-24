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
package com.jerolba.carpet.jdbc;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import com.jerolba.carpet.ColumnNamingStrategy;

/**
 * Multi-database table export test for GaussDB.
 *
 * This test exports tables from two different database schemas:
 * - Accounting DB (sit_suncbs_acctdb): tables starting with 'kfa' or 'kgl'
 * - Core DB (sit_suncbs_coredb): all other tables
 *
 * Environment variables required:
 * - GAUSSDB_ACCTDB_URL (default: jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_acctdb?currentSchema=sit_suncbs_acctdb)
 * - GAUSSDB_ACCTDB_USERNAME
 * - GAUSSDB_ACCTDB_PASSWORD
 * - GAUSSDB_COREDB_URL (default: jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb?currentSchema=sit_suncbs_coredb)
 * - GAUSSDB_COREDB_USERNAME
 * - GAUSSDB_COREDB_PASSWORD
 * - GAUSSDB_LGL_PERN_CODE (default: 6666)
 *
 * The table list is read from /carpet-jdbc/scripts/table-list.txt
 * Parquet files are written to /carpet-jdbc/parquets/
 */
class MultiDatabaseTableExportTest {

    private static final String DEFAULT_ACCTDB_URL =
        "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_acctdb?currentSchema=sit_suncbs_acctdb";
    private static final String DEFAULT_COREDB_URL =
        "jdbc:gaussdb://127.0.0.1:8889/sit_suncbs_coredb?currentSchema=sit_suncbs_coredb";
    private static final String DEFAULT_LGL_PERN_CODE = "6666";

    private Connection acctDbConnection;
    private Connection coreDbConnection;
    private boolean acctDbAvailable = false;
    private boolean coreDbAvailable = false;
    private String lglPernCode;
    private Path parquetOutputDir;

    @BeforeEach
    void setUp() throws IOException {
        // Get legal person code from environment
        lglPernCode = System.getenv().getOrDefault("GAUSSDB_LGL_PERN_CODE", DEFAULT_LGL_PERN_CODE);

        // Set up parquet output directory
        String projectRoot = System.getProperty("user.dir");
        parquetOutputDir = Paths.get(projectRoot, "parquets");
        Files.createDirectories(parquetOutputDir);
        System.out.println("📁 Parquet output directory: " + parquetOutputDir);

        // Set up Accounting DB connection
        setupAcctDbConnection();

        // Set up Core DB connection
        setupCoreDbConnection();
    }

    private void setupAcctDbConnection() {
        String url = System.getenv().getOrDefault("GAUSSDB_ACCTDB_URL", DEFAULT_ACCTDB_URL);
        String username = System.getenv("GAUSSDB_ACCTDB_USERNAME");
        String password = System.getenv("GAUSSDB_ACCTDB_PASSWORD");

        if (username == null || password == null) {
            System.out.println("⚠️  Skipping Accounting DB: GAUSSDB_ACCTDB_USERNAME and GAUSSDB_ACCTDB_PASSWORD not set");
            return;
        }

        try {
            acctDbConnection = DriverManager.getConnection(url, username, password);
            acctDbAvailable = true;
            System.out.println("✅ Connected to Accounting DB at: " + url);
        } catch (SQLException e) {
            System.out.println("⚠️  Could not connect to Accounting DB: " + e.getMessage());
            acctDbAvailable = false;
        }
    }

    private void setupCoreDbConnection() {
        String url = System.getenv().getOrDefault("GAUSSDB_COREDB_URL", DEFAULT_COREDB_URL);
        String username = System.getenv("GAUSSDB_COREDB_USERNAME");
        String password = System.getenv("GAUSSDB_COREDB_PASSWORD");

        if (username == null || password == null) {
            System.out.println("⚠️  Skipping Core DB: GAUSSDB_COREDB_USERNAME and GAUSSDB_COREDB_PASSWORD not set");
            return;
        }

        try {
            coreDbConnection = DriverManager.getConnection(url, username, password);
            coreDbAvailable = true;
            System.out.println("✅ Connected to Core DB at: " + url);
        } catch (SQLException e) {
            System.out.println("⚠️  Could not connect to Core DB: " + e.getMessage());
            coreDbAvailable = false;
        }
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (acctDbConnection != null && !acctDbConnection.isClosed()) {
            acctDbConnection.close();
        }
        if (coreDbConnection != null && !coreDbConnection.isClosed()) {
            coreDbConnection.close();
        }
    }

    @Test
    void testExportAllTablesFromTableList() throws IOException, SQLException {
        assumeTrue(acctDbAvailable || coreDbAvailable,
            "At least one database connection must be available");

        // Read table list from scripts/table-list.txt
        List<String> tables = readTableList();
        assertTrue(!tables.isEmpty(), "Table list should not be empty");
        System.out.println("📋 Found " + tables.size() + " tables in table-list.txt");

        // Categorize tables by database
        Map<String, List<String>> tablesByDb = categorizeTables(tables);

        List<String> acctTables = tablesByDb.get("acct");
        List<String> coreTables = tablesByDb.get("core");

        System.out.println("📊 Table distribution:");
        System.out.println("   - Accounting DB (kfa*, kgl*): " + acctTables.size() + " tables");
        System.out.println("   - Core DB (others): " + coreTables.size() + " tables");

        // Export results tracking
        Map<String, ExportResult> results = new HashMap<>();

        // Export Accounting DB tables
        if (acctDbAvailable && !acctTables.isEmpty()) {
            System.out.println("\n🔄 Exporting Accounting DB tables...");
            exportTables(acctDbConnection, acctTables, "acct", results);
        } else if (!acctTables.isEmpty()) {
            System.out.println("\n⏭️  Skipping " + acctTables.size() + " Accounting DB tables (connection not available)");
        }

        // Export Core DB tables
        if (coreDbAvailable && !coreTables.isEmpty()) {
            System.out.println("\n🔄 Exporting Core DB tables...");
            exportTables(coreDbConnection, coreTables, "core", results);
        } else if (!coreTables.isEmpty()) {
            System.out.println("\n⏭️  Skipping " + coreTables.size() + " Core DB tables (connection not available)");
        }

        // Print summary
        printExportSummary(results);
    }

    /**
     * Read table list from scripts/table-list.txt resource or file
     */
    private List<String> readTableList() throws IOException {
        List<String> tables = new ArrayList<>();

        // Try to read from classpath resource first
        try (InputStream is = getClass().getClassLoader()
                .getResourceAsStream("scripts/table-list.txt")) {
            if (is != null) {
                try (BufferedReader reader = new BufferedReader(
                        new InputStreamReader(is, StandardCharsets.UTF_8))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        line = line.trim();
                        if (!line.isEmpty() && !line.startsWith("#")) {
                            tables.add(line);
                        }
                    }
                }
                System.out.println("📖 Read table list from classpath resource");
                return tables;
            }
        }

        // Fallback to file system
        String projectRoot = System.getProperty("user.dir");
        Path tableListPath = Paths.get(projectRoot, "scripts", "table-list.txt");

        if (Files.exists(tableListPath)) {
            tables = Files.readAllLines(tableListPath, StandardCharsets.UTF_8).stream()
                .map(String::trim)
                .filter(line -> !line.isEmpty() && !line.startsWith("#"))
                .collect(Collectors.toList());
            System.out.println("📖 Read table list from: " + tableListPath);
        } else {
            throw new IOException("Table list not found at: " + tableListPath);
        }

        return tables;
    }

    /**
     * Categorize tables by database (acct vs core)
     * Following the same logic as the Python script
     */
    private Map<String, List<String>> categorizeTables(List<String> tables) {
        Map<String, List<String>> result = new HashMap<>();
        result.put("acct", new ArrayList<>());
        result.put("core", new ArrayList<>());

        for (String table : tables) {
            if (table.startsWith("kfa") || table.startsWith("kgl")) {
                result.get("acct").add(table);
            } else {
                result.get("core").add(table);
            }
        }

        return result;
    }

    /**
     * Export tables from a database connection
     */
    private void exportTables(Connection connection, List<String> tables,
                             String dbType, Map<String, ExportResult> results) {
        DynamicExportConfig config = new DynamicExportConfig()
            .withFetchSize(1000)
            .withBatchSize(10000)
            .withCompressionCodec(CompressionCodecName.SNAPPY)
            .withColumnNamingStrategy(ColumnNamingStrategy.FIELD_NAME);

        int successCount = 0;
        int skipCount = 0;
        int errorCount = 0;

        for (String tableName : tables) {
            try {
                ExportResult result = exportTable(connection, tableName, dbType, config);
                results.put(tableName, result);

                if (result.success) {
                    successCount++;
                    System.out.println("  ✅ " + tableName + " → " + result.rowCount + " rows");
                } else if (result.skipped) {
                    skipCount++;
                    System.out.println("  ⏭️  " + tableName + " (empty or no access)");
                } else {
                    errorCount++;
                    System.out.println("  ❌ " + tableName + " → Error: " + result.errorMessage);
                }
            } catch (Exception e) {
                errorCount++;
                results.put(tableName, ExportResult.error(tableName, e.getMessage()));
                System.out.println("  ❌ " + tableName + " → Exception: " + e.getMessage());
            }
        }

        System.out.println("\n📈 " + dbType.toUpperCase() + " DB Summary: " +
            successCount + " success, " + skipCount + " skipped, " + errorCount + " errors");
    }

    /**
     * Export a single table to Parquet
     */
    private ExportResult exportTable(Connection connection, String tableName,
                                    String dbType, DynamicExportConfig config)
            throws SQLException, IOException {

        // Build SQL query with lgl_pern_code filter
        String sql = String.format(
            "SELECT a.* FROM %s a WHERE a.lgl_pern_code = '%s'",
            tableName, lglPernCode
        );

        // Check if table has any rows first
        try (Statement stmt = connection.createStatement()) {
            stmt.setFetchSize(1);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                if (!rs.next()) {
                    // Table is empty or no matching rows
                    return ExportResult.skipped(tableName);
                }
            }
        } catch (SQLException e) {
            // Table might not exist or no access
            return ExportResult.skipped(tableName);
        }

        // Export to Parquet
        File outputFile = parquetOutputDir.resolve(tableName + ".parquet").toFile();
        long rowCount = DynamicJdbcExporter.exportWithConfig(connection, sql, outputFile, config);

        return ExportResult.success(tableName, rowCount, outputFile);
    }

    /**
     * Print export summary
     */
    private void printExportSummary(Map<String, ExportResult> results) {
        System.out.println("\n" + "=".repeat(80));
        System.out.println("📊 EXPORT SUMMARY");
        System.out.println("=".repeat(80));

        long totalSuccess = results.values().stream().filter(r -> r.success).count();
        long totalSkipped = results.values().stream().filter(r -> r.skipped).count();
        long totalErrors = results.values().stream().filter(r -> !r.success && !r.skipped).count();
        long totalRows = results.values().stream().filter(r -> r.success).mapToLong(r -> r.rowCount).sum();

        System.out.println("Total tables processed: " + results.size());
        System.out.println("  ✅ Successful exports: " + totalSuccess);
        System.out.println("  ⏭️  Skipped (empty/no access): " + totalSkipped);
        System.out.println("  ❌ Errors: " + totalErrors);
        System.out.println("  📦 Total rows exported: " + totalRows);
        System.out.println("  📁 Output directory: " + parquetOutputDir);
        System.out.println("=".repeat(80));

        if (totalErrors > 0) {
            System.out.println("\n❌ Failed tables:");
            results.values().stream()
                .filter(r -> !r.success && !r.skipped)
                .forEach(r -> System.out.println("  - " + r.tableName + ": " + r.errorMessage));
        }
    }

    /**
     * Result holder for export operations
     */
    private static class ExportResult {
        final String tableName;
        final boolean success;
        final boolean skipped;
        final long rowCount;
        final String errorMessage;

        private ExportResult(String tableName, boolean success, boolean skipped,
                           long rowCount, String errorMessage) {
            this.tableName = tableName;
            this.success = success;
            this.skipped = skipped;
            this.rowCount = rowCount;
            this.errorMessage = errorMessage;
        }

        static ExportResult success(String tableName, long rowCount, File outputFile) {
            return new ExportResult(tableName, true, false, rowCount, null);
        }

        static ExportResult skipped(String tableName) {
            return new ExportResult(tableName, false, true, 0, "Table empty or no access");
        }

        static ExportResult error(String tableName, String errorMessage) {
            return new ExportResult(tableName, false, false, 0, errorMessage);
        }
    }
}
