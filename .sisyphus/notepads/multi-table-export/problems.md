
## Test Execution Issue - DuckDB File Locking (RESOLVED)

**Date**: 2026-01-28
**Status**: ✅ RESOLVED

### Original Problem
When running all `*Parallel*` tests together with DuckDB file-based databases, the `testCliExport` test would hang indefinitely during parallel export execution.

### Root Cause
- DuckDB file-based databases don't handle concurrent access well
- Multiple tests creating separate DB files but test framework tries to run in parallel
- Connection hangs when trying to acquire locks

### Solution
**Replaced DuckDB with PostgreSQL using Testcontainers** (2026-01-28)

**Changes made**:
1. Updated `ParallelExportTest.java` to use `@Testcontainers` with PostgreSQL
2. Used `PostgreSQLContainer` from Testcontainers with `postgres:15-alpine` image
3. Setup tables once in `@BeforeAll` method
4. All tests now share the same PostgreSQL container instance
5. Connection supplier creates new connections per thread (proper parallel isolation)

### Verification
✅ All tests now pass when running together:
```bash
./gradlew :carpet-jdbc:test --tests '*Parallel*'
# Result: 3/3 tests passed
```

Individual test results:
- ✅ `testParallelExportMultipleTables` - PASSES
- ✅ `testParallelExportFailFastBehavior` - PASSES  
- ✅ `testCliExport` - PASSES

### Benefits of PostgreSQL Solution
1. **Production-like testing**: PostgreSQL is more representative of real-world usage
2. **Better concurrency**: Proper multi-connection handling
3. **No file locks**: Network-based connections eliminate file-locking issues
4. **Testcontainers integration**: Automatic cleanup and isolation
5. **Consistent with other tests**: Matches PostgreSQL/MySQL test patterns in the project

**Issue fully resolved** - no workarounds needed.
