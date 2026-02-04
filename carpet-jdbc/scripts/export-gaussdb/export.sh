#!/usr/bin/env bash
# =============================================================================
# Database to Parquet Export Script
# =============================================================================
# Exports database tables to Parquet files using the carpet-jdbc CLI.
#
# Prerequisites:
#   - Java 17 or higher
#   - carpet-jdbc-0.5.0-full.jar (fat JAR with JDBC drivers)
#
# Usage:
#   ./export.sh --properties export.properties --tables tables.txt
#   ./export.sh -p export.properties -t tables.txt
#   ./export.sh --help
#
# Exit codes:
#   0 - Success: all tables exported
#   1 - Export failure: one or more tables failed
#   2 - Validation error: invalid arguments or configuration
# =============================================================================

set -euo pipefail

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
JAR_NAME="carpet-jdbc-0.5.0-full.jar"

# Search paths for the JAR file (in order of preference)
# Script is in carpet-jdbc/scripts/export-gaussdb/
# JAR is typically in carpet-jdbc/build/libs/
JAR_SEARCH_PATHS=(
    "${SCRIPT_DIR}/${JAR_NAME}"                                    # Same directory
    "${SCRIPT_DIR}/../../../build/libs/${JAR_NAME}"               # From scripts/export-gaussdb/ to project root build/libs/
    "${SCRIPT_DIR}/../../../carpet-jdbc/build/libs/${JAR_NAME}"   # Alternative path
    "/opt/carpet-jdbc/${JAR_NAME}"
    "${HOME}/lib/${JAR_NAME}"
)

# Java options (can be overridden via JAVA_OPTS environment variable)
DEFAULT_JAVA_OPTS="-Xms2048m -Xmx16g"

# -----------------------------------------------------------------------------
# Functions
# -----------------------------------------------------------------------------
log_info() {
    echo "[INFO] $(date '+%Y-%m-%d %H:%M:%S') $*"
}

log_error() {
    echo "[ERROR] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

log_warn() {
    echo "[WARN] $(date '+%Y-%m-%d %H:%M:%S') $*" >&2
}

show_help() {
    cat << 'EOF'
Database to Parquet Export Script

Usage:
  ./export.sh --properties <file> [--tables <file>] [options]
  ./export.sh -p <file> [-t <file>] [options]

Required Arguments:
  -p, --properties <file>   Path to properties configuration file

Optional Arguments:
  -t, --tables <file>       Path to table list file (one table per line)
                            If not provided, all user tables will be exported
  -j, --jar <file>          Path to carpet-jdbc JAR file (auto-detected if not specified)
  --kms                     Force-enable KMS encryption (requires aws.kms.keyId)
  --no-kms                  Disable KMS encryption even if aws.kms.* is set
  -h, --help                Show this help message

Environment Variables:
  JAVA_OPTS                 JVM options (default: -Xms512m -Xmx4g)
  JAVA_HOME                 Java installation directory

Examples:
  # Export all tables from database
  ./export.sh -p export.properties

  # Export specific tables only
  ./export.sh -p export.properties -t tables.txt

  # With custom JAR location
  ./export.sh -p export.properties -t tables.txt -j /opt/lib/carpet-jdbc-0.5.0-full.jar

  # With custom JVM options
  JAVA_OPTS="-Xms1g -Xmx8g" ./export.sh -p export.properties -t tables.txt

Configuration Files:
  See export.properties.template and tables.txt.template for examples.

Exit Codes:
  0 - Success: all tables exported successfully
  1 - Export failure: one or more tables failed to export
  2 - Validation error: invalid arguments or configuration
EOF
}

find_jar() {
    for path in "${JAR_SEARCH_PATHS[@]}"; do
        if [[ -f "$path" ]]; then
            echo "$path"
            return 0
        fi
    done
    return 1
}

find_java() {
    if [[ -n "${JAVA_HOME:-}" ]] && [[ -x "${JAVA_HOME}/bin/java" ]]; then
        echo "${JAVA_HOME}/bin/java"
    elif command -v java &> /dev/null; then
        command -v java
    else
        return 1
    fi
}

check_java_version() {
    local java_cmd="$1"
    local version
    version=$("$java_cmd" -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f1)
    
    # Handle version strings like "17.0.1" or "1.8.0"
    if [[ "$version" == "1" ]]; then
        version=$("$java_cmd" -version 2>&1 | head -1 | cut -d'"' -f2 | cut -d'.' -f2)
    fi
    
    if [[ "$version" -lt 17 ]]; then
        log_error "Java 17 or higher is required. Found: Java $version"
        return 1
    fi
    
    log_info "Using Java $version"
    return 0
}

validate_file() {
    local file="$1"
    local description="$2"
    
    if [[ ! -f "$file" ]]; then
        log_error "$description not found: $file"
        return 1
    fi
    
    if [[ ! -r "$file" ]]; then
        log_error "$description is not readable: $file"
        return 1
    fi
    
    return 0
}

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------
main() {
    local properties_file=""
    local tables_file=""
    local jar_file=""
    local kms_flag=""
    
    # Parse arguments
    while [[ $# -gt 0 ]]; do
        case "$1" in
            -p|--properties)
                properties_file="$2"
                shift 2
                ;;
            -t|--tables)
                tables_file="$2"
                shift 2
                ;;
            -j|--jar)
                jar_file="$2"
                shift 2
                ;;
            --kms)
                kms_flag="--kms"
                shift
                ;;
            --no-kms)
                kms_flag="--no-kms"
                shift
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            *)
                log_error "Unknown argument: $1"
                echo "Use --help for usage information."
                exit 2
                ;;
        esac
    done
    
    # Validate required arguments
    if [[ -z "$properties_file" ]]; then
        log_error "Missing required argument: --properties"
        echo "Use --help for usage information."
        exit 2
    fi
    
    # Validate input files
    validate_file "$properties_file" "Properties file" || exit 2
    
    # Validate tables file if provided
    if [[ -n "$tables_file" ]]; then
        validate_file "$tables_file" "Tables file" || exit 2
        # Count tables
        local table_count
        table_count=$(grep -v '^#' "$tables_file" | grep -v '^[[:space:]]*$' | wc -l | tr -d ' ')
        log_info "Exporting $table_count table(s) from $tables_file"
    else
        log_info "No tables file provided - will export all user tables from database"
    fi
    
    # Find JAR file
    if [[ -n "$jar_file" ]]; then
        validate_file "$jar_file" "JAR file" || exit 2
    else
        jar_file=$(find_jar) || {
            log_error "Could not find ${JAR_NAME}"
            log_error "Searched in:"
            for path in "${JAR_SEARCH_PATHS[@]}"; do
                log_error "  - $path"
            done
            log_error "Use --jar to specify the JAR file location, or build with:"
            log_error "  ./gradlew :carpet-jdbc:fatJarWithDrivers"
            exit 2
        }
    fi
    
    log_info "Using JAR: $jar_file"
    
    # Find Java
    local java_cmd
    java_cmd=$(find_java) || {
        log_error "Java not found. Please install Java 17+ or set JAVA_HOME."
        exit 2
    }
    
    # Check Java version
    check_java_version "$java_cmd" || exit 2
    
    # Set Java options
    local java_opts="${JAVA_OPTS:-$DEFAULT_JAVA_OPTS}"
    
    # Run export
    log_info "Starting export..."
    log_info "Properties: $properties_file"
    if [[ -n "$tables_file" ]]; then
        log_info "Tables: $tables_file"
    fi
    log_info "Java options: $java_opts"
    
    # Build command arguments
    local cmd_args="--properties \"$properties_file\""
    if [[ -n "$tables_file" ]]; then
        cmd_args="$cmd_args --tables \"$tables_file\""
    fi
    if [[ -n "$kms_flag" ]]; then
        cmd_args="$cmd_args $kms_flag"
    fi
    
    # shellcheck disable=SC2086
    eval "$java_cmd" $java_opts -jar "$jar_file" $cmd_args
    
    local exit_code=$?
    
    if [[ $exit_code -eq 0 ]]; then
        log_info "Export completed successfully!"
    else
        log_error "Export failed with exit code: $exit_code"
    fi
    
    return $exit_code
}

# Run main function
main "$@"
