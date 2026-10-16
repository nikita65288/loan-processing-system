#!/bin/sh
# Maven Wrapper 3.3.4-compatible bootstrap launcher.
set -e

BASE_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
WRAPPER_DIR="$BASE_DIR/.mvn/wrapper"
WRAPPER_JAR="$WRAPPER_DIR/maven-wrapper.jar"
WRAPPER_URL="https://repo.maven.apache.org/maven2/org/apache/maven/wrapper/maven-wrapper/3.3.4/maven-wrapper-3.3.4.jar"

if [ ! -f "$WRAPPER_JAR" ]; then
  echo "Downloading Maven Wrapper 3.3.4..."
  mkdir -p "$WRAPPER_DIR"
  if command -v curl >/dev/null 2>&1; then
    curl --fail --location --silent --show-error "$WRAPPER_URL" --output "$WRAPPER_JAR"
  elif command -v wget >/dev/null 2>&1; then
    wget --quiet "$WRAPPER_URL" --output-document="$WRAPPER_JAR"
  else
    echo "Neither curl nor wget is available; cannot bootstrap Maven Wrapper." >&2
    exit 1
  fi
fi

exec java -classpath "$WRAPPER_JAR" org.apache.maven.wrapper.MavenWrapperMain "$@"
