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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class DynamicJdbcExportCliValidationTest {

    @TempDir
    Path tempDir;

    @Test
    void helpReturnsSuccess() {
        int exitCode = DynamicJdbcExportCli.run(new String[] {"--help"});
        assertEquals(0, exitCode);
    }

    @Test
    void unknownArgumentReturnsValidationError() {
        int exitCode = DynamicJdbcExportCli.run(new String[] {"--unknown"});
        assertEquals(2, exitCode);
    }

    @Test
    void missingPropertiesValueReturnsValidationError() {
        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties"});
        assertEquals(2, exitCode);
    }

    @Test
    void conflictingKmsFlagsReturnValidationError() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        int exitCode = DynamicJdbcExportCli.run(new String[] {
            "--properties", propsPath.toString(),
            "--kms",
            "--no-kms"
        });
        assertEquals(2, exitCode);
    }

    @Test
    void kmsEnabledRequiresKeyId() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        int exitCode = DynamicJdbcExportCli.run(new String[] {
            "--properties", propsPath.toString(),
            "--kms"
        });
        assertEquals(2, exitCode);
    }

    @Test
    void invalidBatchSizeReturnsValidationError() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        Properties props = new Properties();
        props.setProperty("jdbc.url", "jdbc:duckdb:");
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.batchSize", "not-a-number");
        try (var writer = Files.newBufferedWriter(propsPath, StandardCharsets.UTF_8)) {
            props.store(writer, "Invalid batch size");
        }

        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties", propsPath.toString()});
        assertEquals(2, exitCode);
    }

    @Test
    void invalidCompressionReturnsValidationError() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        Properties props = new Properties();
        props.setProperty("jdbc.url", "jdbc:duckdb:");
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.compression", "BOGUS");
        try (var writer = Files.newBufferedWriter(propsPath, StandardCharsets.UTF_8)) {
            props.store(writer, "Invalid compression");
        }

        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties", propsPath.toString()});
        assertEquals(2, exitCode);
    }

    @Test
    void invalidNamingStrategyReturnsValidationError() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        Properties props = new Properties();
        props.setProperty("jdbc.url", "jdbc:duckdb:");
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.namingStrategy", "BOGUS");
        try (var writer = Files.newBufferedWriter(propsPath, StandardCharsets.UTF_8)) {
            props.store(writer, "Invalid naming strategy");
        }

        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties", propsPath.toString()});
        assertEquals(2, exitCode);
    }

    @Test
    void invalidThreadPoolSizeReturnsValidationError() throws IOException {
        Path propsPath = writeRequiredProperties(tempDir.resolve("props.properties"));
        Properties props = new Properties();
        props.setProperty("jdbc.url", "jdbc:duckdb:");
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        props.setProperty("export.threadPoolSize", "-1");
        try (var writer = Files.newBufferedWriter(propsPath, StandardCharsets.UTF_8)) {
            props.store(writer, "Invalid thread pool size");
        }

        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties", propsPath.toString()});
        assertEquals(2, exitCode);
    }

    @Test
    void missingPropertiesFileReturnsExportFailure() {
        int exitCode = DynamicJdbcExportCli.run(new String[] {"--properties", tempDir.resolve("missing.properties").toString()});
        assertEquals(1, exitCode);
    }

    private Path writeRequiredProperties(Path path) throws IOException {
        Properties props = new Properties();
        props.setProperty("jdbc.url", "jdbc:duckdb:");
        props.setProperty("jdbc.user", "");
        props.setProperty("jdbc.password", "");
        props.setProperty("output.baseDir", tempDir.resolve("output").toString());
        try (var writer = Files.newBufferedWriter(path, StandardCharsets.UTF_8)) {
            props.store(writer, "Required properties");
        }
        return path;
    }
}
