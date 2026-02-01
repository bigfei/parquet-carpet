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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import com.jerolba.carpet.CarpetReader;
import com.jerolba.carpet.jdbc.cli.DynamicJdbcExportCli;

/**
 * Unit tests for parallel export API and CLI functionality using PostgreSQL in Testcontainers.
 */
@Testcontainers
class ParallelExportPostgresDBTest {

    @Container
    private static final PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
        .withDatabaseName("testdb")
        .withUsername("test")
        .withPassword("test");

    @BeforeAll
    static void setup() throws SQLException {
        // Create tables once for all tests
        try (Connection conn = postgres.createConnection("");
             Statement stmt = conn.createStatement()) {

            stmt.execute("""
                CREATE TABLE employees (
                    id BIGINT PRIMARY KEY NOT NULL,
                    name VARCHAR(100),
                    department VARCHAR(50),
                    salary DECIMAL(10, 2)
                )
            """);

            stmt.execute("""
                INSERT INTO employees VALUES
                (1, 'Alice', 'Engineering', 90000.00),
                (2, 'Bob', 'Marketing', 75000.00),
                (3, 'Charlie', 'Engineering', 85000.00)
            """);

            stmt.execute("""
                CREATE TABLE departments (
                    id BIGINT PRIMARY KEY NOT NULL,
                    name VARCHAR(100),
                    location VARCHAR(100)
                )
            """);

            stmt.execute("""
                INSERT INTO departments VALUES
                (1, 'Engineering', 'San Francisco'),
                (2, 'Marketing', 'New York')
            """);

            stmt.execute("""
                CREATE TABLE projects (
                    id BIGINT PRIMARY KEY NOT NULL,
                    name VARCHAR(100),
                    budget DECIMAL(15, 2)
                )
            """);

            stmt.execute("""
                INSERT INTO projects VALUES
                (1, 'Project Alpha', 500000.00),
                (2, 'Project Beta', 750000.00)
            """);
        }
    }

    @Test
    void testParallelExportMultipleTables(@TempDir Path tempDir) throws SQLException, IOException {
        List<String> tableNames = Arrays.asList("employees", "departments", "projects");
        String queryPattern = "SELECT * FROM %s";
        File outputBaseDir = tempDir.toFile();
        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(1000)
            .withFetchSize(500);

        ConnectionSupplier connectionSupplier = new ConnectionSupplier(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );

        // When
        Map<String, Long> results = DynamicJdbcExporter.exportParallelWithConfig(
            tableNames,
            queryPattern,
            outputBaseDir,
            config,
            connectionSupplier
        );

        // Then - verify results map
        assertEquals(3, results.size(), "Should have results for all 3 tables");
        assertEquals(3L, results.get("employees"), "Employees table should have 3 rows");
        assertEquals(2L, results.get("departments"), "Departments table should have 2 rows");
        assertEquals(2L, results.get("projects"), "Projects table should have 2 rows");

        // Verify date folder created (yyyyMMdd format)
        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File dateFolderPath = new File(outputBaseDir, dateFolder);
        assertTrue(dateFolderPath.exists(), "Date folder should exist: " + dateFolder);
        assertTrue(dateFolderPath.isDirectory(), "Date folder should be a directory");

        // Verify all output files created
        File employeesFile = new File(dateFolderPath, "employees.parquet");
        File departmentsFile = new File(dateFolderPath, "departments.parquet");
        File projectsFile = new File(dateFolderPath, "projects.parquet");

        assertTrue(employeesFile.exists(), "Employees parquet file should exist");
        assertTrue(departmentsFile.exists(), "Departments parquet file should exist");
        assertTrue(projectsFile.exists(), "Projects parquet file should exist");

        // Verify row counts by reading back the files
        List<Map<String, Object>> employeeRecords = readParquetFile(employeesFile);
        assertEquals(3, employeeRecords.size(), "Employees file should have 3 records");

        List<Map<String, Object>> departmentRecords = readParquetFile(departmentsFile);
        assertEquals(2, departmentRecords.size(), "Departments file should have 2 records");

        List<Map<String, Object>> projectRecords = readParquetFile(projectsFile);
        assertEquals(2, projectRecords.size(), "Projects file should have 2 records");

        // Verify content of first employee record
        Map<String, Object> firstEmployee = employeeRecords.get(0);
        assertEquals(1L, firstEmployee.get("id"));
        assertEquals("Alice", firstEmployee.get("name"));
        assertEquals("Engineering", firstEmployee.get("department"));
    }

    @Test
    void testParallelExportFailFastBehavior(@TempDir Path tempDir) throws SQLException {
        List<String> tableNames = Arrays.asList("employees", "nonexistent_table", "departments");
        String queryPattern = "SELECT * FROM %s";
        File outputBaseDir = tempDir.toFile();
        DynamicExportConfig config = new DynamicExportConfig()
            .withBatchSize(1000);

        ConnectionSupplier connectionSupplier = new ConnectionSupplier(
            postgres.getJdbcUrl(),
            postgres.getUsername(),
            postgres.getPassword()
        );

        // When/Then - should throw IOException due to nonexistent table
        IOException exception = assertThrows(IOException.class, () -> {
            DynamicJdbcExporter.exportParallelWithConfig(
                tableNames,
                queryPattern,
                outputBaseDir,
                config,
                connectionSupplier
            );
        });

        assertTrue(exception.getMessage().contains("nonexistent_table") || 
                   exception.getCause() != null && exception.getCause().getMessage().contains("nonexistent_table"),
            "Exception should mention the failed table name, but got: " + exception.getMessage());

        // Verify date folder exists (created before failure)
        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File dateFolderPath = new File(outputBaseDir, dateFolder);
        assertTrue(dateFolderPath.exists(), "Date folder should exist even after failure");

        File failedFile = new File(dateFolderPath, "nonexistent_table.parquet");
        assertFalse(failedFile.exists(), "Failed table output file should be deleted");
    }

    @Test
    void testCliExport(@TempDir Path tempDir) throws IOException {
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        File propertiesFile = tempDir.resolve("export.properties").toFile();
        Properties props = new Properties();
        props.setProperty("jdbc.url", jdbcUrl);
        props.setProperty("jdbc.user", username);
        props.setProperty("jdbc.password", password);
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.batchSize", "500");
        props.setProperty("export.fetchSize", "500");
        props.setProperty("export.compression", "SNAPPY");
        props.setProperty("export.namingStrategy", "FIELD_NAME");
        props.setProperty("export.queryPattern", "SELECT * FROM %s");

        try (var writer = Files.newBufferedWriter(propertiesFile.toPath(), StandardCharsets.UTF_8)) {
            props.store(writer, "Test export configuration");
        }

        File tablesFile = tempDir.resolve("tables.txt").toFile();
        List<String> tables = Arrays.asList(
            "# Export these tables",
            "",
            "employees"
        );
        Files.write(tablesFile.toPath(), tables, StandardCharsets.UTF_8);

        String[] args = new String[] {
            "--properties", propertiesFile.getAbsolutePath(),
            "--tables", tablesFile.getAbsolutePath()
        };

        int exitCode = DynamicJdbcExportCli.run(args);

        assertEquals(0, exitCode, "CLI should return exit code 0 for success");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = tempDir.resolve("output").resolve(dateFolder).toFile();
        assertTrue(outputDir.exists(), "Output date folder should exist");

        File employeesFile = new File(outputDir, "employees.parquet");
        assertTrue(employeesFile.exists(), "CLI should create employees.parquet");

        List<Map<String, Object>> employeeRecords = readParquetFile(employeesFile);
        assertEquals(3, employeeRecords.size(), "CLI export should have 3 employee records");
    }

    @Test
    void testCliExportAllTables(@TempDir Path tempDir) throws IOException {
        String jdbcUrl = postgres.getJdbcUrl();
        String username = postgres.getUsername();
        String password = postgres.getPassword();

        File propertiesFile = tempDir.resolve("export.properties").toFile();
        Properties props = new Properties();
        props.setProperty("jdbc.url", jdbcUrl);
        props.setProperty("jdbc.user", username);
        props.setProperty("jdbc.password", password);
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.batchSize", "500");
        props.setProperty("export.fetchSize", "500");
        props.setProperty("export.compression", "SNAPPY");
        props.setProperty("export.namingStrategy", "FIELD_NAME");
        props.setProperty("export.queryPattern", "SELECT * FROM %s");

        try (var writer = Files.newBufferedWriter(propertiesFile.toPath(), StandardCharsets.UTF_8)) {
            props.store(writer, "Test export configuration");
        }

        String[] args = new String[] {
            "--properties", propertiesFile.getAbsolutePath()
        };

        int exitCode = DynamicJdbcExportCli.run(args);

        assertEquals(0, exitCode, "CLI should return exit code 0 for success");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = tempDir.resolve("output").resolve(dateFolder).toFile();
        assertTrue(outputDir.exists(), "Output date folder should exist");

        File employeesFile = new File(outputDir, "employees.parquet");
        File departmentsFile = new File(outputDir, "departments.parquet");
        File projectsFile = new File(outputDir, "projects.parquet");

        assertTrue(employeesFile.exists(), "CLI should create employees.parquet");
        assertTrue(departmentsFile.exists(), "CLI should create departments.parquet");
        assertTrue(projectsFile.exists(), "CLI should create projects.parquet");

        List<Map<String, Object>> employeeRecords = readParquetFile(employeesFile);
        assertEquals(3, employeeRecords.size(), "Employees export should have 3 records");

        List<Map<String, Object>> departmentRecords = readParquetFile(departmentsFile);
        assertEquals(2, departmentRecords.size(), "Departments export should have 2 records");

        List<Map<String, Object>> projectRecords = readParquetFile(projectsFile);
        assertEquals(2, projectRecords.size(), "Projects export should have 2 records");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }

    private static class ConnectionSupplier implements java.util.function.Supplier<Connection> {
        private final String jdbcUrl;
        private final String username;
        private final String password;

        ConnectionSupplier(String jdbcUrl, String username, String password) {
            this.jdbcUrl = jdbcUrl;
            this.username = username;
            this.password = password;
        }

        @Override
        public Connection get() {
            try {
                return DriverManager.getConnection(jdbcUrl, username, password);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to create connection", e);
            }
        }
    }
}
