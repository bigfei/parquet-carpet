# Module Rename: carpet-samples → carpet-jdbc

## Summary

The `carpet-samples` module has been renamed to `carpet-jdbc` to better reflect its purpose as a JDBC-to-Parquet adapter library rather than just example code.

## Changes Made

### 1. Directory Rename
- `carpet-samples/` → `carpet-jdbc/`

### 2. Gradle Configuration
- **`settings.gradle`**: Updated module reference from `carpet-samples` to `carpet-jdbc`
- **Build tasks**: Now use `:carpet-jdbc:` prefix instead of `:carpet-samples:`

### 3. Documentation Updates

#### README.md (carpet-jdbc/README.md)
- Updated title from "Carpet Samples" to "Carpet JDBC"
- Changed description to emphasize JDBC adapter functionality
- Updated all Gradle task references:
  - `./gradlew :carpet-samples:test` → `./gradlew :carpet-jdbc:test`
- **Added comprehensive "Adding Support for New Databases" section** with:
  - Type mapping patterns
  - Value conversion strategies
  - Database-specific exporter templates
  - Testing patterns with Testcontainers
  - Integration checklist
  - Common patterns and best practices
  - Performance optimization tips
  - Contribution guidelines

#### .github/copilot-instructions.md
- Updated module description from "Example implementations" to "JDBC-to-Parquet export adapters"
- **Added new "JDBC Adapters" section** with:
  - Purpose and core components
  - Detailed guide for adding new database adapters
  - Database-specific considerations
  - Example Oracle adapter pattern
  - Reference to carpet-jdbc/README.md

## Migration Guide for Developers

### For Build Commands
Replace all occurrences of `carpet-samples` with `carpet-jdbc`:

**Before:**
```bash
./gradlew :carpet-samples:test
./gradlew :carpet-samples:build
```

**After:**
```bash
./gradlew :carpet-jdbc:test
./gradlew :carpet-jdbc:build
```

### For Dependencies
If you have local projects depending on this module:

**Before:**
```gradle
dependencies {
    implementation project(':carpet-samples')
}
```

**After:**
```gradle
dependencies {
    implementation project(':carpet-jdbc')
}
```

### For Import Statements
No changes needed - package names remain `com.jerolba.carpet.samples`

## New Features

### Database Adapter Development Guide
The updated documentation now includes comprehensive patterns for:

1. **Type Mapping**: How to map database-specific SQL types to Parquet types
2. **Value Conversion**: Safe extraction and conversion of database values
3. **Testing**: Using Testcontainers for integration testing
4. **Best Practices**: Common patterns and performance optimization

### Example Templates
Ready-to-use code templates for:
- Oracle adapter (type mapper, value converter, exporter)
- Test suite structure
- Performance optimization patterns

## Testing

Verified that module rename is successful:
```bash
$ ./gradlew projects

Root project 'parquet-carpet'
+--- Project ':carpet-jdbc'
\--- Project ':carpet-record'

BUILD SUCCESSFUL
```

## Next Steps

1. Update any CI/CD pipelines referencing `carpet-samples`
2. Update documentation site if it references the old module name
3. Consider adding Oracle, SQL Server, or other database adapters using the new patterns
4. Review and potentially refactor package names from `com.jerolba.carpet.samples` to `com.jerolba.carpet.jdbc`
