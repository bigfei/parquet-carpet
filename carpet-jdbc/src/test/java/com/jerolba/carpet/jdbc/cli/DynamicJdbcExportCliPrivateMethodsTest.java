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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.lang.reflect.Method;
import java.nio.charset.StandardCharsets;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import org.junit.jupiter.api.Test;

class DynamicJdbcExportCliPrivateMethodsTest {

    @Test
    void parseCurrentSchemasExtractsAndNormalizes() throws Exception {
        @SuppressWarnings("unchecked")
        List<String> schemas = (List<String>) invokeStatic(
            "parseCurrentSchemas",
            new Class<?>[] { String.class },
            "jdbc:postgresql://localhost/db?currentSchema=Foo,Bar&x=1");

        assertEquals(List.of("foo", "bar"), schemas);
    }

    @Test
    void normalizeIdentifierStripsQuotes() throws Exception {
        assertEquals("schema", invokeStatic("normalizeIdentifier", new Class<?>[] { String.class }, "\"Schema\""));
        assertEquals("table", invokeStatic("normalizeIdentifier", new Class<?>[] { String.class }, "`Table`"));
        assertEquals("col", invokeStatic("normalizeIdentifier", new Class<?>[] { String.class }, "[Col]"));
        assertEquals("", invokeStatic("normalizeIdentifier", new Class<?>[] { String.class }, (Object) null));
    }

    @Test
    void confirmContinueWithExportUsesInput() throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream yesIn = new ByteArrayInputStream("y\n".getBytes(StandardCharsets.UTF_8));
        boolean yes = (boolean) invokeStatic(
            "confirmContinueWithExport",
            new Class<?>[] { PrintStream.class, java.io.InputStream.class, int.class, List.class },
            new PrintStream(out),
            yesIn,
            3,
            List.of("missing_table"));
        assertTrue(yes);

        ByteArrayInputStream noIn = new ByteArrayInputStream("\n".getBytes(StandardCharsets.UTF_8));
        boolean no = (boolean) invokeStatic(
            "confirmContinueWithExport",
            new Class<?>[] { PrintStream.class, java.io.InputStream.class, int.class, List.class },
            new PrintStream(out),
            noIn,
            1,
            List.of());
        assertFalse(no);
    }

    @Test
    void formatTableListTruncates() throws Exception {
        @SuppressWarnings("unchecked")
        String formatted = (String) invokeStatic(
            "formatTableList",
            new Class<?>[] { List.class, int.class },
            List.of("a", "b", "c"),
            2);
        assertEquals("a, b, ... (3 total)", formatted);
    }

    @Test
    void inferDriverFromUrlMatchesKnownPrefixes() throws Exception {
        assertEquals("com.huawei.gaussdb.jdbc.Driver",
            invokeStatic("inferDriverFromUrl", new Class<?>[] { String.class }, "jdbc:gaussdb://host/db"));
        assertEquals("org.postgresql.Driver",
            invokeStatic("inferDriverFromUrl", new Class<?>[] { String.class }, "jdbc:postgresql://host/db"));
        assertEquals("com.mysql.cj.jdbc.Driver",
            invokeStatic("inferDriverFromUrl", new Class<?>[] { String.class }, "jdbc:mysql://host/db"));
        assertEquals("org.sqlite.JDBC",
            invokeStatic("inferDriverFromUrl", new Class<?>[] { String.class }, "jdbc:sqlite:/tmp/test.db"));
        assertEquals("org.duckdb.DuckDBDriver",
            invokeStatic("inferDriverFromUrl", new Class<?>[] { String.class }, "jdbc:duckdb:"));
    }

    @Test
    void trimToNullHandlesBlankAndTrimmedValues() throws Exception {
        assertEquals(null, invokeStatic("trimToNull", new Class<?>[] { String.class }, "   "));
        assertEquals("value", invokeStatic("trimToNull", new Class<?>[] { String.class }, "  value  "));
        assertEquals(null, invokeStatic("trimToNull", new Class<?>[] { String.class }, (Object) null));
    }

    @Test
    void resolvePathHandlesRelativeAndAbsoluteValues() throws Exception {
        String absolute = Paths.get("/tmp/cert.pem").toString();
        assertEquals(
            absolute,
            invokeStatic("resolvePath", new Class<?>[] { String.class, String.class }, "/tmp/export.properties", absolute)
        );

        String resolved = (String) invokeStatic(
            "resolvePath",
            new Class<?>[] { String.class, String.class },
            "/tmp/config/export.properties",
            "cert.pem"
        );
        assertEquals(Paths.get("/tmp/config/cert.pem").toString(), resolved);
    }

    @Test
    void buildJdbcPropertiesIncludesSslSettings() throws Exception {
        Properties input = new Properties();
        input.setProperty("jdbc.user", "user");
        input.setProperty("jdbc.password", "pass");
        input.setProperty("ssl.rootcert", "certs/root.pem");
        input.setProperty("ssl.mode", "verify-ca");
        input.setProperty("ssl.factory", "com.example.Factory");

        @SuppressWarnings("unchecked")
        Properties result = (Properties) invokeStatic(
            "buildJdbcProperties",
            new Class<?>[] { Properties.class, String.class },
            input,
            "/tmp/config/export.properties"
        );

        assertEquals("user", result.getProperty("user"));
        assertEquals("pass", result.getProperty("password"));
        assertEquals(Paths.get("/tmp/config/certs/root.pem").toString(), result.getProperty("sslrootcert"));
        assertEquals("verify-ca", result.getProperty("sslmode"));
        assertEquals("com.example.Factory", result.getProperty("sslfactory"));
    }

    @Test
    void systemTableDetectionHandlesKnownPatterns() throws Exception {
        assertEquals(true, invokeStatic("isSystemTable", new Class<?>[] { String.class, String.class }, null, null));
        assertEquals(true, invokeStatic("isSystemTable", new Class<?>[] { String.class, String.class }, "pg_class", null));
        assertEquals(true, invokeStatic("isSystemTable", new Class<?>[] { String.class, String.class }, "users", "information_schema"));
        assertEquals(false, invokeStatic("isSystemTable", new Class<?>[] { String.class, String.class }, "customers", "public"));
    }

    @Test
    void tableTypeAndSchemaHelpersNormalize() throws Exception {
        assertEquals(true, invokeStatic("isUserTableType", new Class<?>[] { String.class }, (Object) null));
        assertEquals(true, invokeStatic("isUserTableType", new Class<?>[] { String.class }, "TABLE"));
        assertEquals(true, invokeStatic("isUserTableType", new Class<?>[] { String.class }, "BASE TABLE"));
        assertEquals(false, invokeStatic("isUserTableType", new Class<?>[] { String.class }, "VIEW"));

        assertEquals(true, invokeStatic("isDefaultSchema", new Class<?>[] { String.class }, "public"));
        assertEquals(true, invokeStatic("isDefaultSchema", new Class<?>[] { String.class }, "main"));
        assertEquals(false, invokeStatic("isDefaultSchema", new Class<?>[] { String.class }, "custom"));
    }

    private static Object invokeStatic(String method, Class<?>[] types, Object... args) throws Exception {
        Method target = DynamicJdbcExportCli.class.getDeclaredMethod(method, types);
        target.setAccessible(true);
        return target.invoke(null, args);
    }
}
