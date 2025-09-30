# Module Rename: carpet-samples → carpet-jdbc

## Summary

The `carpet-samples` module has been renamed to `carpet-jdbc` to better reflect its purpose as a JDBC-to-Parquet adapter library rather than just example code.

## Changes Made

### 1. Directory Rename
- `carpet-samples/` → `carpet-jdbc/`

### 2. Gradle Configuration
- **`settings.gradle`**: Updated module reference from `carpet-samples` to `carpet-jdbc`
- **Build tasks**: Now use `:carpet-jdbc:` prefix instead of `:carpet-samples:`

### 3. Package Rename
- **Package structure**: `com.jerolba.carpet.samples` → `com.jerolba.carpet.jdbc`
- **Directory structure**:
  - `src/main/java/com/jerolba/carpet/samples/` → `src/main/java/com/jerolba/carpet/jdbc/`
  - `src/test/java/com/jerolba/carpet/samples/` → `src/test/java/com/jerolba/carpet/jdbc/`
- **Files updated**:
  - Main source files: `DynamicJdbcExporter.java`, `DynamicExportConfig.java`
  - Test files: All test classes (8 files)
  - Documentation: README.md import examples

### 4. Documentation Updates

#### README.md (carpet-jdbc/README.md)
- Updated title from "Carpet Samples" to "Carpet JDBC"
- Changed description to emphasize JDBC adapter functionality
- Updated all Gradle task references:
  - `./gradlew :carpet-samples:test` → `./gradlew :carpet-jdbc:test`
- Updated package import examples from `com.jerolba.carpet.samples` to `com.jerolba.carpet.jdbc`
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

### 5. Build Configuration
- **Sources JAR**: Added `withSourcesJar()` to java configuration
- **JAR Task**: Configured to automatically build sources jar alongside regular jar
- Result: Running `./gradlew :carpet-jdbc:jar` now produces both:
  - `carpet-jdbc-0.5.0-SNAPSHOT.jar`
  - `carpet-jdbc-0.5.0-SNAPSHOT-sources.jar`

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

**Before:**
```java
import com.jerolba.carpet.samples.*;
```

**After:**
```java
import com.jerolba.carpet.jdbc.*;
```

All package names have been updated from `com.jerolba.carpet.samples` to `com.jerolba.carpet.jdbc`.

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
4. ✅ **Package names refactored from `com.jerolba.carpet.samples` to `com.jerolba.carpet.jdbc`**
