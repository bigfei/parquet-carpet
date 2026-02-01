/*
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
package com.jerolba.carpet.jdbc.cli;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jerolba.carpet.CarpetReader;

/**
 * Tests for DynamicJdbcExportCli auto-discover functionality.
 * Verifies that when no --tables argument is provided, all user tables
 * are automatically discovered and exported.
 */
class DynamicJdbcExportCliAutoDiscoverTest {

    @TempDir
    Path outputTempDir;

    private Connection connection;
    private String DB_URL;

    @BeforeEach
    void setup() throws SQLException {
        // Use in-memory DuckDB for testing
        DB_URL = "jdbc:duckdb:";
        connection = DriverManager.getConnection(DB_URL);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute("""
                CREATE TABLE employees (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(100),
                    department VARCHAR(50)
                )
            """);

            stmt.execute("""
                CREATE TABLE departments (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(100),
                    location VARCHAR(100)
                )
            """);

            stmt.execute("""
                CREATE TABLE projects (
                    id BIGINT PRIMARY KEY,
                    name VARCHAR(100),
                    budget DECIMAL(15, 2)
                )
            """);

            stmt.execute("INSERT INTO employees VALUES (1, 'Alice', 'Engineering'), (2, 'Bob', 'Marketing')");
            stmt.execute("INSERT INTO departments VALUES (1, 'Engineering', 'NYC'), (2, 'Marketing', 'LA')");
            stmt.execute("INSERT INTO projects VALUES (1, 'Project A', 100000.00), (2, 'Project B', 200000.00)");
        }
    }

    @AfterEach
    void tearDown() {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            // Ignore
        }
    }

    @Test
    void testAutoDiscoverAllTables() throws IOException, SQLException {
        File propertiesFile = outputTempDir.resolve("export.properties").toFile();
        Properties props = new Properties();
        props.setProperty("jdbc.url", DB_URL);
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", outputTempDir.resolve("output").toString());
        props.setProperty("export.batchSize", "1000");
        props.store(Files.newOutputStream(propertiesFile.toPath()), "Test properties");

        String[] args = new String[] {
            "--properties", propertiesFile.getAbsolutePath()
        };

        int exitCode = DynamicJdbcExportCli.run(args);

        assertEquals(0, exitCode, "CLI should return exit code 0 for success");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = outputTempDir.resolve("output").resolve(dateFolder).toFile();
        assertTrue(outputDir.exists(), "Output date folder should exist");

        File employeesFile = new File(outputDir, "employees.parquet");
        File departmentsFile = new File(outputDir, "departments.parquet");
        File projectsFile = new File(outputDir, "projects.parquet");

        assertTrue(employeesFile.exists(), "CLI should auto-discover and export employees table");
        assertTrue(departmentsFile.exists(), "CLI should auto-discover and export departments table");
        assertTrue(projectsFile.exists(), "CLI should auto-discover and export projects table");

        List<Map<String, Object>> employees = readParquetFile(employeesFile);
        assertEquals(2, employees.size(), "Employees table should have 2 records");

        List<Map<String, Object>> departments = readParquetFile(departmentsFile);
        assertEquals(2, departments.size(), "Departments table should have 2 records");

        List<Map<String, Object>> projects = readParquetFile(projectsFile);
        assertEquals(2, projects.size(), "Projects table should have 2 records");
    }

    @Test
    void testAutoDiscoverWithSystemTablesExcluded() throws IOException, SQLException {
        File propertiesFile = outputTempDir.resolve("export.properties").toFile();
        Properties props = new Properties();
        props.setProperty("jdbc.url", DB_URL);
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", outputTempDir.resolve("output").toString());
        props.store(Files.newOutputStream(propertiesFile.toPath()), "Test properties");

        String[] args = new String[] {
            "--properties", propertiesFile.getAbsolutePath()
        };

        int exitCode = DynamicJdbcExportCli.run(args);

        assertEquals(0, exitCode, "CLI should return exit code 0 for success");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = outputTempDir.resolve("output").resolve(dateFolder).toFile();

        File[] files = outputDir.listFiles((dir, name) -> name.endsWith(".parquet"));
        assertTrue(files != null && files.length == 3, "Should export exactly 3 user tables, excluding system tables");
    }

    @Test
    void testWithExplicitTablesFile() throws IOException {
        File propertiesFile = outputTempDir.resolve("export.properties").toFile();
        Properties props = new Properties();
        props.setProperty("jdbc.url", DB_URL);
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", outputTempDir.resolve("output").toString());
        props.store(Files.newOutputStream(propertiesFile.toPath()), "Test properties");

        File tablesFile = outputTempDir.resolve("tables.txt").toFile();
        Files.write(tablesFile.toPath(), "employees\n".getBytes(StandardCharsets.UTF_8));

        String[] args = new String[] {
            "--properties", propertiesFile.getAbsolutePath(),
            "--tables", tablesFile.getAbsolutePath()
        };

        int exitCode = DynamicJdbcExportCli.run(args);

        assertEquals(0, exitCode, "CLI should return exit code 0 for success");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File outputDir = outputTempDir.resolve("output").resolve(dateFolder).toFile();

        File employeesFile = new File(outputDir, "employees.parquet");
        File departmentsFile = new File(outputDir, "departments.parquet");

        assertTrue(employeesFile.exists(), "CLI should export employees table from tables file");
        assertTrue(!departmentsFile.exists(), "CLI should NOT export departments table when tables file specifies only employees");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }
}
