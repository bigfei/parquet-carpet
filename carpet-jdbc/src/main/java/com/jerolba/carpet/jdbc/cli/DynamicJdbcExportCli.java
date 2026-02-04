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
package com.jerolba.carpet.jdbc.cli;

import com.jerolba.carpet.ColumnNamingStrategy;
import com.jerolba.carpet.jdbc.DynamicExportConfig;
import com.jerolba.carpet.jdbc.DynamicJdbcExporter;
import com.jerolba.carpet.jdbc.KmsEncryptionConfig;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

/**
 * Command-line interface for exporting JDBC tables to Parquet files using parallel processing.
 *
 * <p>Usage:
 * <pre>
 * java -jar carpet-jdbc.jar --properties config.properties --tables tables.txt
 * </pre>
 *
 * <p>Properties file format:
 * <pre>
 * # Required properties
 * jdbc.url=jdbc:postgresql://localhost:5432/mydb
 * jdbc.user=username
 * jdbc.password=password
 * output.baseDir=/path/to/output
 *
 * # Optional export configuration
 * export.batchSize=10000
 * export.fetchSize=1000
 * export.compression=SNAPPY
 * export.namingStrategy=SNAKE_CASE
 * export.convertCamelCase=true
 * export.includeSchemaInfo=false
 * export.continueOnFailure=true
 * export.promptBeforeExport=true
 * export.overwriteExistingFiles=false
 * export.threadPoolSize=4
 * export.queryPattern=SELECT * FROM %s
 *
 * # Optional AWS KMS encryption
 * aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/12345678-1234-1234-1234-123456789012
 * aws.kms.region=us-east-1
 * aws.profile=default
 * aws.kms.endpointUrl=https://vpce-xxx.kms.us-east-1.vpce.amazonaws.com
 * </pre>
 *
 * <p>Tables file format (one table per line):
 * <pre>
 * # This is a comment
 * employees
 * departments
 * projects
 * </pre>
 *
 * <p>Exit codes:
 * <ul>
 *   <li>0: Success - all tables exported successfully</li>
 *   <li>1: Export failure - one or more tables failed to export</li>
 *   <li>2: Validation error - invalid arguments or configuration</li>
 * </ul>
 */
public class DynamicJdbcExportCli {

    private static final Logger logger = LoggerFactory.getLogger(DynamicJdbcExportCli.class);

    // Exit codes
    private static final int EXIT_SUCCESS = 0;
    private static final int EXIT_EXPORT_FAILURE = 1;
    private static final int EXIT_VALIDATION_ERROR = 2;

    // Required property keys
    private static final String PROP_JDBC_URL = "jdbc.url";
    private static final String PROP_JDBC_USER = "jdbc.user";
    private static final String PROP_JDBC_PASSWORD = "jdbc.password";
    private static final String PROP_JDBC_DRIVER_CLASS = "jdbc.driverClass";
    private static final String PROP_SSL_ROOTCERT = "ssl.rootcert";
    private static final String PROP_SSL_MODE = "ssl.mode";
    private static final String PROP_SSL_FACTORY = "ssl.factory";
    private static final String PROP_OUTPUT_BASE_DIR = "output.baseDir";

    // Optional export configuration keys
    private static final String PROP_EXPORT_BATCH_SIZE = "export.batchSize";
    private static final String PROP_EXPORT_FETCH_SIZE = "export.fetchSize";
    private static final String PROP_EXPORT_COMPRESSION = "export.compression";
    private static final String PROP_EXPORT_NAMING_STRATEGY = "export.namingStrategy";
    private static final String PROP_EXPORT_CONVERT_CAMEL_CASE = "export.convertCamelCase";
    private static final String PROP_EXPORT_INCLUDE_SCHEMA_INFO = "export.includeSchemaInfo";
    private static final String PROP_EXPORT_CONTINUE_ON_FAILURE = "export.continueOnFailure";
    private static final String PROP_EXPORT_PROMPT_BEFORE_EXPORT = "export.promptBeforeExport";
    private static final String PROP_EXPORT_OVERWRITE_EXISTING = "export.overwriteExistingFiles";
    private static final String PROP_EXPORT_QUERY_PATTERN = "export.queryPattern";
    private static final String PROP_EXPORT_THREAD_POOL_SIZE = "export.threadPoolSize";

    // Optional AWS KMS encryption keys
    private static final String PROP_AWS_KMS_KEY_ID = "aws.kms.keyId";
    private static final String PROP_AWS_KMS_REGION = "aws.kms.region";
    private static final String PROP_AWS_PROFILE = "aws.profile";
    private static final String PROP_AWS_KMS_ENDPOINT_URL = "aws.kms.endpointUrl";

    // Default query pattern
    private static final String DEFAULT_QUERY_PATTERN = "SELECT * FROM %s";

    /**
     * Main entry point for the CLI application.
     *
     * @param args command-line arguments
     */
    public static void main(String[] args) {
        int exitCode = run(args);
        System.exit(exitCode);
    }

    /**
     * Run the CLI application without calling System.exit().
     *
     * @param args command-line arguments
     * @return exit code (0=success, 1=export failure, 2=validation error)
     */
    public static int run(String[] args) {
        try {
            // Parse command-line arguments
            CommandLineArgs parsedArgs = parseArguments(args);

            if (parsedArgs.showHelp) {
                printHelp();
                return EXIT_SUCCESS;
            }

            if (parsedArgs.propertiesFile == null) {
                logger.error("Missing required argument: --properties. Use --help for usage information.");
                return EXIT_VALIDATION_ERROR;
            }

            // Load properties file
            Properties properties = loadProperties(parsedArgs.propertiesFile);

            // Validate required properties
            validateRequiredProperties(properties);

            // Ensure JDBC driver is loaded (fat JARs can drop driver service files)
            loadJdbcDriver(properties);

            // Build export configuration
            DynamicExportConfig config = buildExportConfig(properties, parsedArgs.kmsEnabledOverride);

            // Read table list or get all tables from database
            List<String> tables;
            if (parsedArgs.tablesFile != null) {
                tables = readTableList(parsedArgs.tablesFile);
            } else {
                // Get all tables from database metadata
                String jdbcUrl = properties.getProperty(PROP_JDBC_URL);
                Properties jdbcProperties = buildJdbcProperties(properties, parsedArgs.propertiesFile);
                tables = getAllTablesFromDatabase(jdbcUrl, jdbcProperties);
                logger.info("No tables file provided. Found {} tables in database.", tables.size());
            }

            // Deduplicate tables
            tables = deduplicateTables(tables);

            // Validate table list
            if (tables.isEmpty()) {
                logger.error("Table list is empty. At least one table must be specified.");
                return EXIT_VALIDATION_ERROR;
            }

            // Extract connection properties
            String jdbcUrl = properties.getProperty(PROP_JDBC_URL);
            Properties jdbcProperties = buildJdbcProperties(properties, parsedArgs.propertiesFile);
            File outputBaseDir = new File(properties.getProperty(PROP_OUTPUT_BASE_DIR));
            String queryPattern = properties.getProperty(PROP_EXPORT_QUERY_PATTERN, DEFAULT_QUERY_PATTERN);

            // Validate tables exist before exporting when a list is provided
            List<String> missingTables = new ArrayList<>();
            if (parsedArgs.tablesFile != null) {
                missingTables = findMissingTables(tables, jdbcUrl, jdbcProperties);
                if (!missingTables.isEmpty()) {
                    logger.warn("Missing {} table(s): {}", missingTables.size(), formatTableList(missingTables, 20));
                }
            }

            if (config.isPromptBeforeExport()) {
                if (!confirmContinueWithExport(System.out, System.in, tables.size(), missingTables)) {
                    logger.error("Export aborted by user.");
                    return EXIT_VALIDATION_ERROR;
                }
            }

            // Create connection supplier
            ConnectionSupplier connectionSupplier = new ConnectionSupplier(jdbcUrl, jdbcProperties);

            // Log export configuration
            logger.info("Starting parallel export of {} tables", tables.size());
            logger.info("JDBC URL: {}", jdbcUrl);
            logger.info("Output directory: {}", outputBaseDir.getAbsolutePath());
            logger.info("Query pattern: {}", queryPattern);
            logger.info("Batch size: {}", config.getBatchSize());
            logger.info("Fetch size: {}", config.getFetchSize());
            logger.info("Compression: {}", config.getCompressionCodec());
            logger.info("KMS encryption: {}", config.isKmsEncryptionEnabled() ? "enabled" : "disabled");

            // Execute parallel export
            DynamicJdbcExporter.exportParallelWithConfig(
                tables,
                queryPattern,
                outputBaseDir,
                config,
                connectionSupplier
            );

            logger.info("Export completed successfully!");
            return EXIT_SUCCESS;

        } catch (ValidationException e) {
            logger.error("Validation error: {}", e.getMessage());
            return EXIT_VALIDATION_ERROR;
        } catch (IOException e) {
            logger.error("Export failed: {}", e.getMessage(), e);
            return EXIT_EXPORT_FAILURE;
        } catch (Exception e) {
            logger.error("Unexpected error: {}", e.getMessage(), e);
            return EXIT_EXPORT_FAILURE;
        }
    }

    /**
     * Parse command-line arguments.
     */
    private static CommandLineArgs parseArguments(String[] args) {
        CommandLineArgs parsedArgs = new CommandLineArgs();

        for (int i = 0; i < args.length; i++) {
            String arg = args[i];

            if ("--help".equals(arg) || "-h".equals(arg)) {
                parsedArgs.showHelp = true;
                return parsedArgs;
            }

            if ("--properties".equals(arg)) {
                if (i + 1 >= args.length) {
                    throw new ValidationException("Missing value for --properties argument");
                }
                parsedArgs.propertiesFile = args[++i];
                continue;
            }

            if ("--tables".equals(arg)) {
                if (i + 1 >= args.length) {
                    throw new ValidationException("Missing value for --tables argument");
                }
                parsedArgs.tablesFile = args[++i];
                continue;
            }

            if ("--kms".equals(arg)) {
                parsedArgs.kmsEnabledOverride = mergeKmsOverride(parsedArgs.kmsEnabledOverride, true);
                continue;
            }

            if ("--no-kms".equals(arg)) {
                parsedArgs.kmsEnabledOverride = mergeKmsOverride(parsedArgs.kmsEnabledOverride, false);
                continue;
            }

            throw new ValidationException("Unknown argument: " + arg);
        }

        return parsedArgs;
    }

    /**
     * Load properties from file.
     */
    private static Properties loadProperties(String propertiesFile) throws IOException {
        Properties properties = new Properties();

        try (InputStream input = new FileInputStream(propertiesFile)) {
            properties.load(new InputStreamReader(input, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new IOException("Failed to load properties file: " + propertiesFile, e);
        }

        return properties;
    }

    /**
     * Validate that all required properties are present.
     */
    private static void validateRequiredProperties(Properties properties) {
        List<String> missingProperties = new ArrayList<>();

        if (!properties.containsKey(PROP_JDBC_URL)) {
            missingProperties.add(PROP_JDBC_URL);
        }
        if (!properties.containsKey(PROP_JDBC_USER)) {
            missingProperties.add(PROP_JDBC_USER);
        }
        if (!properties.containsKey(PROP_JDBC_PASSWORD)) {
            missingProperties.add(PROP_JDBC_PASSWORD);
        }
        if (!properties.containsKey(PROP_OUTPUT_BASE_DIR)) {
            missingProperties.add(PROP_OUTPUT_BASE_DIR);
        }

        if (!missingProperties.isEmpty()) {
            throw new ValidationException("Missing required properties: " + String.join(", ", missingProperties));
        }
    }

    /**
     * Build export configuration from properties.
     */
    private static DynamicExportConfig buildExportConfig(Properties properties, Boolean kmsEnabledOverride) {
        DynamicExportConfig config = new DynamicExportConfig();

        // Batch size
        if (properties.containsKey(PROP_EXPORT_BATCH_SIZE)) {
            try {
                int batchSize = Integer.parseInt(properties.getProperty(PROP_EXPORT_BATCH_SIZE));
                config.setBatchSize(batchSize);
            } catch (NumberFormatException e) {
                throw new ValidationException("Invalid batch size: " + properties.getProperty(PROP_EXPORT_BATCH_SIZE));
            }
        }

        // Fetch size
        if (properties.containsKey(PROP_EXPORT_FETCH_SIZE)) {
            try {
                int fetchSize = Integer.parseInt(properties.getProperty(PROP_EXPORT_FETCH_SIZE));
                config.setFetchSize(fetchSize);
            } catch (NumberFormatException e) {
                throw new ValidationException("Invalid fetch size: " + properties.getProperty(PROP_EXPORT_FETCH_SIZE));
            }
        }

        // Compression codec
        if (properties.containsKey(PROP_EXPORT_COMPRESSION)) {
            try {
                CompressionCodecName compression = CompressionCodecName.valueOf(
                    properties.getProperty(PROP_EXPORT_COMPRESSION).toUpperCase()
                );
                config.setCompressionCodec(compression);
            } catch (IllegalArgumentException e) {
                throw new ValidationException("Invalid compression codec: " + properties.getProperty(PROP_EXPORT_COMPRESSION));
            }
        }

        // Column naming strategy
        if (properties.containsKey(PROP_EXPORT_NAMING_STRATEGY)) {
            try {
                ColumnNamingStrategy namingStrategy = ColumnNamingStrategy.valueOf(
                    properties.getProperty(PROP_EXPORT_NAMING_STRATEGY).toUpperCase()
                );
                config.setColumnNamingStrategy(namingStrategy);
            } catch (IllegalArgumentException e) {
                throw new ValidationException("Invalid naming strategy: " + properties.getProperty(PROP_EXPORT_NAMING_STRATEGY));
            }
        }

        // Convert camel case
        if (properties.containsKey(PROP_EXPORT_CONVERT_CAMEL_CASE)) {
            boolean convertCamelCase = Boolean.parseBoolean(properties.getProperty(PROP_EXPORT_CONVERT_CAMEL_CASE));
            config.setConvertCamelCase(convertCamelCase);
        }

        // Include schema info
        if (properties.containsKey(PROP_EXPORT_INCLUDE_SCHEMA_INFO)) {
            boolean includeSchemaInfo = Boolean.parseBoolean(properties.getProperty(PROP_EXPORT_INCLUDE_SCHEMA_INFO));
            config.setIncludeSchemaInfo(includeSchemaInfo);
        }

        // Continue on failure
        if (properties.containsKey(PROP_EXPORT_CONTINUE_ON_FAILURE)) {
            boolean continueOnFailure = Boolean.parseBoolean(properties.getProperty(PROP_EXPORT_CONTINUE_ON_FAILURE));
            config.setContinueOnFailure(continueOnFailure);
        }

        // Prompt before export
        if (properties.containsKey(PROP_EXPORT_PROMPT_BEFORE_EXPORT)) {
            boolean promptBeforeExport = Boolean.parseBoolean(properties.getProperty(PROP_EXPORT_PROMPT_BEFORE_EXPORT));
            config.setPromptBeforeExport(promptBeforeExport);
        }

        // Overwrite existing output files
        if (properties.containsKey(PROP_EXPORT_OVERWRITE_EXISTING)) {
            boolean overwriteExisting = Boolean.parseBoolean(properties.getProperty(PROP_EXPORT_OVERWRITE_EXISTING));
            config.setOverwriteExistingFiles(overwriteExisting);
        }

        // Thread pool size
        if (properties.containsKey(PROP_EXPORT_THREAD_POOL_SIZE)) {
            try {
                int threadPoolSize = Integer.parseInt(properties.getProperty(PROP_EXPORT_THREAD_POOL_SIZE));
                if (threadPoolSize < 0) {
                    throw new ValidationException("Thread pool size must be >= 0");
                }
                config.setThreadPoolSize(threadPoolSize);
            } catch (NumberFormatException e) {
                throw new ValidationException("Invalid thread pool size: " + properties.getProperty(PROP_EXPORT_THREAD_POOL_SIZE));
            }
        }

        // KMS encryption configuration
        if (Boolean.TRUE.equals(kmsEnabledOverride) && !properties.containsKey(PROP_AWS_KMS_KEY_ID)) {
            throw new ValidationException("KMS encryption enabled but aws.kms.keyId is not configured");
        }
        if (!Boolean.FALSE.equals(kmsEnabledOverride) && properties.containsKey(PROP_AWS_KMS_KEY_ID)) {
            KmsEncryptionConfig kmsConfig = buildKmsConfig(properties);
            config.setKmsEncryptionConfig(kmsConfig);
        }
        if (Boolean.FALSE.equals(kmsEnabledOverride) && properties.containsKey(PROP_AWS_KMS_KEY_ID)) {
            logger.info("KMS encryption disabled by CLI flag; ignoring aws.kms.* properties");
        }

        return config;
    }

    /**
     * Build KMS encryption configuration from properties.
     */
    private static KmsEncryptionConfig buildKmsConfig(Properties properties) {
        KmsEncryptionConfig kmsConfig = new KmsEncryptionConfig();

        String kmsKeyId = properties.getProperty(PROP_AWS_KMS_KEY_ID);
        if (kmsKeyId == null || kmsKeyId.trim().isEmpty()) {
            throw new ValidationException("KMS key ID cannot be empty");
        }
        kmsConfig.setKmsKeyId(kmsKeyId);

        if (properties.containsKey(PROP_AWS_KMS_REGION)) {
            kmsConfig.setAwsRegion(properties.getProperty(PROP_AWS_KMS_REGION));
        }

        if (properties.containsKey(PROP_AWS_PROFILE)) {
            kmsConfig.setAwsProfile(properties.getProperty(PROP_AWS_PROFILE));
        }

        if (properties.containsKey(PROP_AWS_KMS_ENDPOINT_URL)) {
            kmsConfig.setKmsEndpointUrl(properties.getProperty(PROP_AWS_KMS_ENDPOINT_URL));
        }

        kmsConfig.validate();
        return kmsConfig;
    }

    private static Boolean mergeKmsOverride(Boolean current, boolean nextValue) {
        if (current != null && current.booleanValue() != nextValue) {
            throw new ValidationException("Conflicting KMS flags: use only one of --kms or --no-kms");
        }
        return nextValue;
    }

    /**
     * Read table list from file.
     * Ignores empty lines and lines starting with '#'.
     */
    private static List<String> readTableList(String tablesFile) throws IOException {
        List<String> tables = new ArrayList<>();

        try {
            List<String> lines = Files.readAllLines(Paths.get(tablesFile), StandardCharsets.UTF_8);
            for (String line : lines) {
                String trimmed = line.trim();
                if (!trimmed.isEmpty() && !trimmed.startsWith("#")) {
                    tables.add(trimmed);
                }
            }
        } catch (IOException e) {
            throw new IOException("Failed to read table list file: " + tablesFile, e);
        }

        return tables;
    }

    /**
     * Deduplicate tables using LinkedHashSet to preserve order.
     * Logs warning for duplicates.
     */
    private static List<String> deduplicateTables(List<String> tables) {
        Set<String> uniqueTables = new LinkedHashSet<>();
        for (String table : tables) {
            if (!uniqueTables.add(table)) {
                logger.warn("Duplicate table name found and ignored: {}", table);
            }
        }
        return new ArrayList<>(uniqueTables);
    }

    /**
     * Retrieve all table names from database metadata.
     * Filters out system tables and views, returning only user tables.
     */
    private static List<String> getAllTablesFromDatabase(String jdbcUrl, Properties jdbcProperties) throws SQLException {
        List<String> tables = new ArrayList<>();

        try (Connection connection = DriverManager.getConnection(jdbcUrl, jdbcProperties)) {
            DatabaseMetaData metaData = connection.getMetaData();
            
            // Prefer TABLE type, fallback to driver-specific types (e.g., BASE TABLE)
            tables.addAll(collectTables(metaData, new String[] {"TABLE"}));
            if (tables.isEmpty()) {
                tables.addAll(collectTables(metaData, null));
            }
        }
        
        if (tables.isEmpty()) {
            throw new ValidationException("No tables found in database.");
        }
        
        return tables;
    }

    private static List<String> collectTables(DatabaseMetaData metaData, String[] types) throws SQLException {
        List<String> tables = new ArrayList<>();
        try (ResultSet rs = metaData.getTables(null, null, "%", types)) {
            while (rs.next()) {
                String tableType = rs.getString("TABLE_TYPE");
                if (!isUserTableType(tableType)) {
                    continue;
                }
                String tableName = rs.getString("TABLE_NAME");
                String schemaName = rs.getString("TABLE_SCHEM");

                // Skip system tables (common patterns)
                if (isSystemTable(tableName, schemaName)) {
                    continue;
                }

                // Include schema prefix if present and not default
                if (schemaName != null && !schemaName.isEmpty() && !isDefaultSchema(schemaName)) {
                    tables.add(schemaName + "." + tableName);
                } else {
                    tables.add(tableName);
                }
            }
        }
        return tables;
    }

    private static boolean isUserTableType(String tableType) {
        if (tableType == null) {
            return true;
        }
        String normalized = tableType.trim().toUpperCase(Locale.ROOT);
        return normalized.equals("TABLE") || normalized.equals("BASE TABLE");
    }

    private static boolean isDefaultSchema(String schemaName) {
        String normalized = schemaName.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("public") || normalized.equals("main");
    }

    private static List<String> findMissingTables(List<String> tables, String jdbcUrl, Properties jdbcProperties) throws SQLException {
        List<String> availableTables = getAllTablesFromDatabase(jdbcUrl, jdbcProperties);
        Map<String, Set<String>> tablesBySchema = new HashMap<>();
        Set<String> allTables = new HashSet<>();

        for (String table : availableTables) {
            String normalized = normalizeIdentifier(table);
            allTables.add(normalized);

            String schema = null;
            String tableName = table;
            int dotIndex = table.indexOf('.');
            if (dotIndex > 0) {
                schema = table.substring(0, dotIndex);
                tableName = table.substring(dotIndex + 1);
            }

            String normalizedSchema = schema != null ? normalizeIdentifier(schema) : null;
            String normalizedTable = normalizeIdentifier(tableName);
            if (normalizedSchema != null) {
                tablesBySchema.computeIfAbsent(normalizedSchema, key -> new HashSet<>()).add(normalizedTable);
            }
        }

        List<String> currentSchemas = parseCurrentSchemas(jdbcUrl);
        List<String> missing = new ArrayList<>();

        for (String table : tables) {
            String trimmed = table.trim();
            String schema = null;
            String tableName = trimmed;
            int dotIndex = trimmed.indexOf('.');
            if (dotIndex > 0) {
                schema = trimmed.substring(0, dotIndex);
                tableName = trimmed.substring(dotIndex + 1);
            }

            String normalizedSchema = schema != null ? normalizeIdentifier(schema) : null;
            String normalizedTable = normalizeIdentifier(tableName);

            boolean exists;
            if (normalizedSchema != null) {
                exists = tablesBySchema.containsKey(normalizedSchema)
                    && tablesBySchema.get(normalizedSchema).contains(normalizedTable);
            } else if (!currentSchemas.isEmpty()) {
                exists = false;
                for (String currentSchema : currentSchemas) {
                    Set<String> schemaTables = tablesBySchema.get(currentSchema);
                    if (schemaTables != null && schemaTables.contains(normalizedTable)) {
                        exists = true;
                        break;
                    }
                }
            } else {
                exists = allTables.contains(normalizedTable);
            }

            if (!exists) {
                missing.add(trimmed);
            }
        }

        return missing;
    }

    private static List<String> parseCurrentSchemas(String jdbcUrl) {
        List<String> schemas = new ArrayList<>();
        if (jdbcUrl == null) {
            return schemas;
        }
        int queryIndex = jdbcUrl.indexOf('?');
        if (queryIndex < 0 || queryIndex + 1 >= jdbcUrl.length()) {
            return schemas;
        }
        String query = jdbcUrl.substring(queryIndex + 1);
        String[] params = query.split("&");
        for (String param : params) {
            int equalsIndex = param.indexOf('=');
            if (equalsIndex <= 0) {
                continue;
            }
            String key = param.substring(0, equalsIndex);
            if (!"currentschema".equalsIgnoreCase(key)) {
                continue;
            }
            String value = param.substring(equalsIndex + 1);
            if (value.isBlank()) {
                continue;
            }
            for (String schema : value.split(",")) {
                String normalized = normalizeIdentifier(schema);
                if (!normalized.isBlank()) {
                    schemas.add(normalized);
                }
            }
        }
        return schemas;
    }

    private static String normalizeIdentifier(String value) {
        if (value == null) {
            return "";
        }
        String trimmed = value.trim();
        if (trimmed.length() >= 2) {
            char first = trimmed.charAt(0);
            char last = trimmed.charAt(trimmed.length() - 1);
            if ((first == '"' && last == '"') || (first == '`' && last == '`') || (first == '[' && last == ']')) {
                trimmed = trimmed.substring(1, trimmed.length() - 1);
            }
        }
        return trimmed.toLowerCase(Locale.ROOT);
    }

    private static boolean confirmContinueWithExport(
        PrintStream out,
        InputStream in,
        int tableCount,
        List<String> missingTables
    ) throws IOException {
        out.println("About to export " + tableCount + " table(s).");
        if (missingTables != null && !missingTables.isEmpty()) {
            out.println("The following tables were not found in the database:");
            out.println(formatTableList(missingTables, 50));
        }
        out.print("Continue exporting anyway? [y/N]: ");
        out.flush();

        BufferedReader reader = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));
        String response = reader.readLine();
        if (response == null) {
            return false;
        }
        String normalized = response.trim().toLowerCase(Locale.ROOT);
        return normalized.equals("y") || normalized.equals("yes");
    }

    private static String formatTableList(List<String> tables, int maxItems) {
        if (tables.size() <= maxItems) {
            return String.join(", ", tables);
        }
        List<String> subset = tables.subList(0, maxItems);
        return String.join(", ", subset) + ", ... (" + tables.size() + " total)";
    }

    /**
     * Check if a table is a system table that should be excluded.
     */
    private static boolean isSystemTable(String tableName, String schemaName) {
        if (tableName == null) return true;
        
        String upperTable = tableName.toUpperCase();
        String upperSchema = schemaName != null ? schemaName.toUpperCase() : "";
        
        // Common system table patterns to exclude
        String[] systemPatterns = {
            "PG_", "SQLITE_", "SYS", "DUAL", "INFORMATION_SCHEMA", "MYSQL", "PERFORMANCE_SCHEMA"
        };
        
        for (String pattern : systemPatterns) {
            if (upperTable.startsWith(pattern) || upperSchema.startsWith(pattern)) {
                return true;
            }
        }
        
        return false;
    }

    /**
     * Print help message.
     */
    private static void printHelp() {
        System.out.println("Usage: DynamicJdbcExportCli --properties <file> [--tables <file>]");
        System.out.println();
        System.out.println("Options:");
        System.out.println("  --properties <file>  Path to properties file (required)");
        System.out.println("  --tables <file>      Path to table list file (optional)");
        System.out.println("                       If not provided, all user tables will be exported");
        System.out.println("  --kms                Force-enable KMS encryption (requires aws.kms.keyId)");
        System.out.println("  --no-kms             Disable KMS encryption even if aws.kms.* is set");
        System.out.println("  --help, -h           Show this help message");
        System.out.println();
        System.out.println("Properties file format:");
        System.out.println("  # Required properties");
        System.out.println("  jdbc.url=jdbc:postgresql://localhost:5432/mydb");
        System.out.println("  jdbc.user=username");
        System.out.println("  jdbc.password=password");
        System.out.println("  output.baseDir=/path/to/output");
        System.out.println("  # Optional JDBC driver override");
        System.out.println("  jdbc.driverClass=org.postgresql.Driver");
        System.out.println("  # Optional SSL settings (driver-dependent)");
        System.out.println("  ssl.rootcert=/path/to/ca.pem");
        System.out.println("  ssl.mode=verify-ca");
        System.out.println("  ssl.factory=com.huawei.gaussdb.jdbc.ssl.LibPQGmtlsFactory");
        System.out.println();
        System.out.println("  # Optional export configuration");
        System.out.println("  export.batchSize=10000");
        System.out.println("  export.fetchSize=1000");
        System.out.println("  export.compression=SNAPPY");
        System.out.println("  export.namingStrategy=SNAKE_CASE");
        System.out.println("  export.convertCamelCase=true");
        System.out.println("  export.includeSchemaInfo=false");
        System.out.println("  export.continueOnFailure=true");
        System.out.println("  export.promptBeforeExport=true");
        System.out.println("  export.overwriteExistingFiles=false");
        System.out.println("  export.threadPoolSize=4");
        System.out.println("  export.queryPattern=SELECT * FROM %s");
        System.out.println();
        System.out.println("  # Optional AWS KMS encryption");
        System.out.println("  aws.kms.keyId=arn:aws:kms:us-east-1:123456789012:key/...");
        System.out.println("  aws.kms.region=us-east-1");
        System.out.println("  aws.profile=default");
        System.out.println("  aws.kms.endpointUrl=https://vpce-xxx.kms.us-east-1.vpce.amazonaws.com");
        System.out.println();
        System.out.println("Tables file format (one table per line):");
        System.out.println("  # This is a comment");
        System.out.println("  employees");
        System.out.println("  departments");
        System.out.println("  projects");
        System.out.println();
        System.out.println("Exit codes:");
        System.out.println("  0 - Success: all tables exported successfully");
        System.out.println("  1 - Export failure: one or more tables failed to export");
        System.out.println("  2 - Validation error: invalid arguments or configuration");
    }

    /**
     * Connection supplier that creates JDBC connections using DriverManager.
     */
    private static class ConnectionSupplier implements java.util.function.Supplier<Connection> {
        private final String jdbcUrl;
        private final Properties jdbcProperties;

        public ConnectionSupplier(String jdbcUrl, Properties jdbcProperties) {
            this.jdbcUrl = jdbcUrl;
            this.jdbcProperties = jdbcProperties;
        }

        @Override
        public Connection get() {
            try {
                return DriverManager.getConnection(jdbcUrl, jdbcProperties);
            } catch (SQLException e) {
                throw new RuntimeException("Failed to create database connection", e);
            }
        }
    }

    /**
     * Command-line arguments holder.
     */
    private static class CommandLineArgs {
        String propertiesFile;
        String tablesFile;
        boolean showHelp;
        Boolean kmsEnabledOverride;
    }

    /**
     * Validation exception for configuration errors.
     */
    private static class ValidationException extends RuntimeException {
        public ValidationException(String message) {
            super(message);
        }
    }

    private static void loadJdbcDriver(Properties properties) {
        String configuredDriver = trimToNull(properties.getProperty(PROP_JDBC_DRIVER_CLASS));
        if (configuredDriver != null) {
            loadDriverClass(configuredDriver, true);
            return;
        }

        String jdbcUrl = properties.getProperty(PROP_JDBC_URL, "");
        String inferredDriver = inferDriverFromUrl(jdbcUrl);
        if (inferredDriver != null) {
            if (loadDriverClass(inferredDriver, false)) {
                return;
            }
        }

        for (String candidate : getKnownDriverClasses()) {
            loadDriverClass(candidate, false);
        }
    }

    private static String inferDriverFromUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        String normalized = jdbcUrl.trim().toLowerCase();
        if (normalized.startsWith("jdbc:gaussdb:")) {
            return "com.huawei.gaussdb.jdbc.Driver";
        }
        if (normalized.startsWith("jdbc:postgresql:")) {
            return "org.postgresql.Driver";
        }
        if (normalized.startsWith("jdbc:mysql:")) {
            return "com.mysql.cj.jdbc.Driver";
        }
        if (normalized.startsWith("jdbc:sqlite:")) {
            return "org.sqlite.JDBC";
        }
        if (normalized.startsWith("jdbc:duckdb:")) {
            return "org.duckdb.DuckDBDriver";
        }
        return null;
    }

    private static boolean loadDriverClass(String className, boolean required) {
        try {
            Class.forName(className);
            logger.info("Loaded JDBC driver: {}", className);
            return true;
        } catch (ClassNotFoundException e) {
            if (required) {
                throw new ValidationException("JDBC driver class not found: " + className);
            }
            logger.debug("JDBC driver class not found: {}", className);
            return false;
        }
    }

    private static List<String> getKnownDriverClasses() {
        List<String> drivers = new ArrayList<>();
        drivers.add("com.huawei.gaussdb.jdbc.Driver");
        drivers.add("org.postgresql.Driver");
        drivers.add("com.mysql.cj.jdbc.Driver");
        drivers.add("org.sqlite.JDBC");
        drivers.add("org.duckdb.DuckDBDriver");
        return drivers;
    }

    private static String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }

    private static Properties buildJdbcProperties(Properties properties, String propertiesFile) {
        Properties jdbcProperties = new Properties();

        String jdbcUser = trimToNull(properties.getProperty(PROP_JDBC_USER));
        String jdbcPassword = trimToNull(properties.getProperty(PROP_JDBC_PASSWORD));
        if (jdbcUser != null) {
            jdbcProperties.setProperty("user", jdbcUser);
        }
        if (jdbcPassword != null) {
            jdbcProperties.setProperty("password", jdbcPassword);
        }

        String sslRootCert = trimToNull(properties.getProperty(PROP_SSL_ROOTCERT));
        if (sslRootCert != null) {
            jdbcProperties.setProperty("sslrootcert", resolvePath(propertiesFile, sslRootCert));
        }
        String sslMode = trimToNull(properties.getProperty(PROP_SSL_MODE));
        if (sslMode != null) {
            jdbcProperties.setProperty("sslmode", sslMode);
        }
        String sslFactory = trimToNull(properties.getProperty(PROP_SSL_FACTORY));
        if (sslFactory != null) {
            jdbcProperties.setProperty("sslfactory", sslFactory);
        }

        return jdbcProperties;
    }

    private static String resolvePath(String propertiesFile, String value) {
        try {
            java.nio.file.Path path = java.nio.file.Paths.get(value);
            if (path.isAbsolute()) {
                return path.toString();
            }
            java.nio.file.Path baseDir = java.nio.file.Paths.get(propertiesFile).toAbsolutePath().getParent();
            if (baseDir == null) {
                return path.toString();
            }
            return baseDir.resolve(path).normalize().toString();
        } catch (Exception e) {
            return value;
        }
    }
}
