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
package com.jerolba.carpet.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.duckdb.DuckDBConnection;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.IOException;
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
import java.util.function.Supplier;

import com.jerolba.carpet.CarpetReader;

/**
 * Unit tests for parallel export API functionality using DuckDB in-memory database.
 */
class ParallelExportDuckDBTest {

    private static DuckDBConnection baseConnection;

    @BeforeAll
    static void setup() throws SQLException {
        baseConnection = (DuckDBConnection) DriverManager.getConnection("jdbc:duckdb:");

        try (Statement stmt = baseConnection.createStatement()) {
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

    @AfterAll
    static void tearDown() throws SQLException {
        if (baseConnection != null) {
            baseConnection.close();
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

        Supplier<Connection> connectionSupplier = new DuckDbConnectionSupplier(baseConnection);

        Map<String, Long> results = DynamicJdbcExporter.exportParallelWithConfig(
            tableNames,
            queryPattern,
            outputBaseDir,
            config,
            connectionSupplier
        );

        assertEquals(3, results.size(), "Should have results for all 3 tables");
        assertEquals(3L, results.get("employees"), "Employees table should have 3 rows");
        assertEquals(2L, results.get("departments"), "Departments table should have 2 rows");
        assertEquals(2L, results.get("projects"), "Projects table should have 2 rows");

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File dateFolderPath = new File(outputBaseDir, dateFolder);
        assertTrue(dateFolderPath.exists(), "Date folder should exist: " + dateFolder);
        assertTrue(dateFolderPath.isDirectory(), "Date folder should be a directory");

        File employeesFile = new File(dateFolderPath, "employees.parquet");
        File departmentsFile = new File(dateFolderPath, "departments.parquet");
        File projectsFile = new File(dateFolderPath, "projects.parquet");

        assertTrue(employeesFile.exists(), "Employees parquet file should exist");
        assertTrue(departmentsFile.exists(), "Departments parquet file should exist");
        assertTrue(projectsFile.exists(), "Projects parquet file should exist");

        List<Map<String, Object>> employeeRecords = readParquetFile(employeesFile);
        assertEquals(3, employeeRecords.size(), "Employees file should have 3 records");

        List<Map<String, Object>> departmentRecords = readParquetFile(departmentsFile);
        assertEquals(2, departmentRecords.size(), "Departments file should have 2 records");

        List<Map<String, Object>> projectRecords = readParquetFile(projectsFile);
        assertEquals(2, projectRecords.size(), "Projects file should have 2 records");

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

        Supplier<Connection> connectionSupplier = new DuckDbConnectionSupplier(baseConnection);

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

        String dateFolder = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        File dateFolderPath = new File(outputBaseDir, dateFolder);
        assertTrue(dateFolderPath.exists(), "Date folder should exist even after failure");

        File failedFile = new File(dateFolderPath, "nonexistent_table.parquet");
        assertFalse(failedFile.exists(), "Failed table output file should be deleted");
    }

    @SuppressWarnings("unchecked")
    private List<Map<String, Object>> readParquetFile(File file) throws IOException {
        CarpetReader<Map<String, Object>> reader = new CarpetReader<>(file, (Class<Map<String, Object>>) (Class<?>) Map.class);
        return reader.toList();
    }

    private static class DuckDbConnectionSupplier implements Supplier<Connection> {
        private final DuckDBConnection connection;

        DuckDbConnectionSupplier(DuckDBConnection connection) {
            this.connection = connection;
        }

        @Override
        public Connection get() {
            // Duplicate the connection for thread-safe concurrent queries.
            try {
                return connection.duplicate();
            } catch (SQLException e) {
                throw new RuntimeException("Failed to create DuckDB connection", e);
            }
        }
    }
}
