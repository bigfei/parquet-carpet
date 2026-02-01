# Multi-Table Parallel Export + CLI

## Context

### Original Request
Export a list of tables from a DB, one Parquet per table, under a date-based folder, using multi-threaded export (one table per thread). Add overall throughput logging via SLF4J. Provide a CLI that takes a properties file and a table-list file. Add a public API in `DynamicJdbcExporter` to accept `List<String>` tables.

### Interview Summary
**Key Decisions**:
- Public API in `DynamicJdbcExporter` accepts `List<String> tableNames`.
- Output folder format: `yyyyMMdd` with system default timezone.
- Fixed thread pool size: `max(1, CPU-1)`; fail-fast on first error.
- KMS in parallel mode uses per-table KMS encryption (no shared DEK).
- CLI uses a properties file for config and `--tables` for list file (reusing `aws.*` KMS keys from `test.properties`; other keys are new).
- Query pattern allowed (default `SELECT * FROM %s`).
- Table list file: one table per line, ignore empty lines and `#` comments.
- Tests added after implementation.

**Research Findings**:
- `DynamicJdbcExporter.exportBatchWithKmsEncryption(...)` is sequential and writes `<table>.parquet` under output directory; it creates the directory if missing. `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/DynamicJdbcExporter.java`.
- Table list file pattern is in `MultiDatabaseTableExportTest` (reads `scripts/table-list.txt`). `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/MultiDatabaseTableExportTest.java`.
- Logging patterns for throughput/rows/sec exist in `DynamicJdbcExporterPostgreSQLTest`. `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/DynamicJdbcExporterPostgreSQLTest.java`.
- No production concurrency patterns exist; README contains only examples. `carpet-jdbc/README.md`.

### Metis Review
**Identified Gaps (addressed)**:
- Fail-fast semantics, thread pool shutdown timeout, and connection lifecycle clarified in plan.
- Properties required/optional keys and defaults specified in plan.
- Edge cases (empty/duplicate tables, nonexistent tables, output collisions) specified in plan.
- Logging frequency and error handling specified in plan.

---

## Work Objectives

### Core Objective
Provide a public API and CLI to export a list of tables to per-table Parquet files under `output.baseDir/yyyyMMdd`, using parallel threads and SLF4J progress/throughput logging, with fail-fast behavior on first error.

### Concrete Deliverables
- New `DynamicJdbcExporter` method that accepts `List<String> tableNames` and performs parallel export; returns `Map<String, Long>` on success and throws `IOException` on failure.
- New CLI class under `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/cli/` supporting properties + table-list inputs.
- Properties file schema documented in CLI usage (and README if needed).
- Tests for parallel export behavior and CLI entrypoint (JUnit 5).

### Definition of Done
- [ ] Given a properties file and tables list, the CLI exports one Parquet file per table to `output.baseDir/yyyyMMdd`.
- [ ] Export uses a fixed thread pool of `max(1, CPU-1)` and fails fast on first error.
- [ ] Logging shows `exported/total` and overall `rows/sec` via SLF4J.
- [ ] Tests pass: `./gradlew :carpet-jdbc:test --tests *Parallel*` (final command in plan).

### Must Have
- Public API in `DynamicJdbcExporter` that takes `List<String>` and a query pattern.
- CLI that consumes a properties file and a table-list file.
- Fail-fast semantics and thread-pool shutdown handling.

### Must NOT Have (Guardrails)
- No connection pooling setup or new DB adapters.
- No resume/checkpoint, retries, or table discovery features.
- No shared-DEK KMS optimization in parallel mode.
- No new UI/TUI or interactive prompts.

---

## Verification Strategy (Tests After)

### Test Decision
- **Infrastructure exists**: YES (JUnit 5, Testcontainers, DuckDB)
- **User wants tests**: YES (Tests after)
- **Framework**: JUnit 5

### Manual QA (always)
- CLI run with DuckDB or local DB:
  - `java -cp ... com.jerolba.carpet.jdbc.cli.<CliClass> --properties /path/to/export.properties --tables /path/to/tables.txt`
  - Verify output folder `output.baseDir/yyyyMMdd` contains one `.parquet` per table.
  - Verify logs include exported/total and rows/sec.

---

## Task Flow

```
Task 1 → Task 2 → Task 3 → Task 4 → Task 5
```

## Parallelization

| Group | Tasks | Reason |
|------|------|--------|
| A | 2, 3 | CLI and export API can be developed in parallel after API signature is set |

| Task | Depends On | Reason |
|------|------------|--------|
| 4 | 1-3 | Tests depend on API + CLI behavior |
| 5 | 4 | Verification runs after tests added |

---

## TODOs

- [x] 1. Define new parallel export API in `DynamicJdbcExporter`

  **What to do**:
  - Add a public method that accepts `List<String> tableNames`, `String queryPattern`, `File outputBaseDir`, `DynamicExportConfig config`, and a `Supplier<Connection>` to create per-thread connections.
  - Return `Map<String, Long>` on full success; on any failure throw `IOException` with root cause and do not return partial results.
  - Build queries using `String.format(queryPattern, tableName)`; validate pattern contains `%s`.
  - Create output folder `outputBaseDir/yyyyMMdd` using system default timezone.
  - Compute thread pool size as `max(1, availableProcessors - 1)`.
  - Choose export path per table: if `config.isKmsEncryptionEnabled()` then call `exportWithKmsEncryption(...)` (per-table KMS call); else call `exportWithConfig(...)`.
  - Fail-fast: on first task failure, cancel remaining tasks, call `shutdownNow()`, and await termination with a fixed timeout (e.g., 60s). In-flight tasks may still finish and produce outputs.
  - Ensure each task closes its own JDBC connection.
  - Delete partial output file for failed table if created; if KMS is enabled, also delete `<output>.metadata` sidecar when present.
  - Overwrite existing target file with a warning.
  - File naming: use table name as base, sanitize path separators to `_` (keep dots).
  - Use SLF4J logger for progress: log completion per table and overall rows/sec.

  **Must NOT do**:
  - No shared JDBC Connection across threads.
  - No shared-DEK KMS context in parallel mode.
  - No retries or table discovery.

  **Parallelizable**: NO (foundational)

  **References**:
  - `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/DynamicJdbcExporter.java` - existing export methods and directory creation behavior.
  - `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/KmsEnvelopeEncryptionOutputStream.java` - `.metadata` sidecar naming (`outputFile + ".metadata"`).
  - `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/MultiDatabaseTableExportTest.java` - table list parsing conventions.
  - `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/DynamicJdbcExporterPostgreSQLTest.java` - rows/sec logging formula.

  **Acceptance Criteria**:
  - [ ] Method validates `queryPattern` contains `%s`; throws `IllegalArgumentException` otherwise.
  - [ ] Method creates `outputBaseDir/yyyyMMdd` if missing.
  - [ ] When one table fails, remaining tasks are cancelled, executor is shutdown, and method throws.
  - [ ] Failed table output file is removed if created; if KMS enabled, the `.metadata` sidecar is also removed.
  - [ ] Other outputs may exist due to in-flight tasks.
  - [ ] Logs include `Exported X/Y tables` and `rows/sec`.

- [x] 2. Add CLI class to expose export functionality

  **What to do**:
  - Create CLI under `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/cli/` (e.g., `DynamicJdbcExportCli`).
  - Required args: `--properties <file>` and `--tables <file>`; support `--help`.
  - Load Java `.properties` file (UTF-8) and map keys (align with `test.properties`):
    - Required: `jdbc.url`, `jdbc.user`, `jdbc.password`, `output.baseDir`
    - Optional: `export.batchSize`, `export.fetchSize`, `export.compression`, `export.namingStrategy`, `export.convertCamelCase`, `export.includeSchemaInfo`, `export.queryPattern`, `aws.kms.keyId`, `aws.kms.region`, `aws.profile`, `aws.kms.endpointUrl`
    - Defaults: missing optional keys use `DynamicExportConfig` defaults.
    - Validation: `export.compression` must map to `CompressionCodecName` enum; `export.namingStrategy` must map to `ColumnNamingStrategy` enum; invalid values -> validation error (exit code 2).
  - Read tables file: one per line, ignore empty lines and `#` comments; dedupe with warning; empty list is a validation error.
  - Build `DynamicExportConfig` from properties (including KMS config when present).
  - Use `DriverManager` to create a new connection per table via `Supplier<Connection>`.
  - Exit codes: `0` success, `2` validation errors, `1` export failure.

  **Must NOT do**:
  - No connection pooling setup.
  - No interactive prompts.

  **Parallelizable**: YES (with Task 3)

  **References**:
  - `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/MultiDatabaseTableExportTest.java` - table list reading.
  - `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/KmsEncryptionConfig.java` - KMS config keys.
  - `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/DynamicExportConfig.java` - defaults and setters for mapping properties.

  **Acceptance Criteria**:
  - [ ] CLI validates properties file and tables file path; exits with code `2` on validation errors.
  - [ ] CLI uses query pattern default `SELECT * FROM %s` when not provided.
  - [ ] CLI rejects invalid `export.compression` or `export.namingStrategy` with exit code `2`.
  - [ ] CLI logs start and summary with exported/total and rows/sec.

- [x] 3. Wire SLF4J logging and fail-fast summary

  **What to do**:
  - Ensure logs are emitted from the coordinator thread only.
  - Use `LoggerFactory.getLogger(DynamicJdbcExporter.class)` for API logs and `LoggerFactory.getLogger(<CliClass>.class)` for CLI logs.
  - Log progress and throughput at INFO level; validation issues at WARN; failures at ERROR.
  - Add SLF4J dependencies in `carpet-jdbc/build.gradle`: `implementation "org.slf4j:slf4j-api:2.0.13"` and `runtimeOnly "org.slf4j:slf4j-simple:2.0.13"` for console output.
  - On each table completion, update totals and log `Exported X/Y tables, totalRows, rows/sec`.
  - On failure, log the failed table name and exception, then cancel remaining tasks.

  **Must NOT do**:
  - No per-row logging.

  **Parallelizable**: YES (with Task 2)

  **References**:
  - `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/DynamicJdbcExporterPostgreSQLTest.java` - throughput calculation pattern.
  - `carpet-jdbc/src/main/java/com/jerolba/carpet/jdbc/DynamicJdbcExporter.java` - existing stdout progress logging.
  - `carpet-jdbc/build.gradle` - add SLF4J dependencies.

  **Acceptance Criteria**:
  - [ ] Logs show progress with exported/total counts and rows/sec after each table.
  - [ ] Final log shows total rows and elapsed time.

- [ ] 4. Add tests (JUnit 5) after implementation

  **What to do**:
  - Add a new test class (DuckDB-based) to export multiple tables in parallel and verify output files.
  - Add a fail-fast test: include one invalid table, assert method throws, and ensure failed table output is removed; allow other outputs due to in-flight completion.
  - Add a CLI test that calls a `run(args)` method (no `System.exit`), using temp properties + table list files.

  **Must NOT do**:
  - No Testcontainers dependency for this feature.

  **Parallelizable**: NO (depends on Tasks 1-3)

  **References**:
  - `carpet-jdbc/src/test/java/com/jerolba/carpet/jdbc/DynamicJdbcExporterDuckDBTest.java` - DuckDB patterns and `@TempDir` usage.

  **Acceptance Criteria**:
  - [ ] `./gradlew :carpet-jdbc:test --tests *Parallel*` passes.
  - [ ] Tests validate date folder output and file count.

- [ ] 5. Update documentation (minimal)

  **What to do**:
  - Add a short CLI usage section to `carpet-jdbc/README.md` with required args and a minimal properties example, e.g.:
    ```properties
    jdbc.url=jdbc:postgresql://localhost:5432/mydb
    jdbc.user=myuser
    jdbc.password=secret
    output.baseDir=output
    export.queryPattern=SELECT * FROM %s
    export.compression=SNAPPY
    export.namingStrategy=SNAKE_CASE
    aws.kms.keyId=arn:aws:kms:ap-southeast-1:123456789012:key/your-key-id
    aws.kms.region=ap-southeast-1
    aws.profile=uat2
    ```

  **Must NOT do**:
  - No extensive documentation or diagrams.

  **Parallelizable**: YES (after Task 2)

  **References**:
  - `carpet-jdbc/README.md` - existing performance and export sections.

  **Acceptance Criteria**:
  - [ ] README includes CLI invocation and minimal properties example.

---

## Commit Strategy

- Commit: NO (only if user explicitly requests)

---

## Success Criteria

### Verification Commands
```bash
./gradlew :carpet-jdbc:test --tests *Parallel*
```

### Final Checklist
- [ ] Parallel export method uses per-table connections and fail-fast cancellation
- [ ] CLI reads properties + table list files and exports to `output.baseDir/yyyyMMdd`
- [ ] SLF4J logs show exported/total and rows/sec
- [ ] Tests pass
