#!/bin/bash
# setup.sh - Download dependencies required by the demo
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"
LIB_DIR="$PROJECT_DIR/lib"

SQLITE_JDBC_VERSION="3.45.3.0"
SQLITE_JDBC_URL="https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/${SQLITE_JDBC_VERSION}/sqlite-jdbc-${SQLITE_JDBC_VERSION}.jar"
SQLITE_JDBC_JAR="$LIB_DIR/sqlite-jdbc.jar"

mkdir -p "$LIB_DIR"

if [ -f "$SQLITE_JDBC_JAR" ]; then
    echo "SQLite JDBC driver already exists at $SQLITE_JDBC_JAR"
else
    echo "Downloading SQLite JDBC driver v${SQLITE_JDBC_VERSION}..."
    curl -fSL -o "$SQLITE_JDBC_JAR" "$SQLITE_JDBC_URL"
    echo "Downloaded to $SQLITE_JDBC_JAR"
fi

echo "Setup complete."
