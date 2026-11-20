#!/usr/bin/env bash
#
# Check Groovy unit tests for field/property/method accesses that bypass Java visibility
# and rely on dynamic Groovy reflection at runtime.
#
# Usage:
#   scripts/test_field_access_check.sh [target] [options]
#   scripts/test_field_access_check.sh
#   scripts/test_field_access_check.sh io.velo.command --src
#   scripts/test_field_access_check.sh BGroupTest --by-file
#   scripts/test_field_access_check.sh --target-class BigStringFiles
#   scripts/test_field_access_check.sh --cross-package-only
#   scripts/test_field_access_check.sh --include-methods
#   scripts/test_field_access_check.sh --compile
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

FORCE_COMPILE=false
HAS_DIR_ARG=false
HAS_COLOR_ARG=false
FORWARD_ARGS=()

while (($# > 0)); do
    case "$1" in
        --compile)
            FORCE_COMPILE=true
            shift
            ;;
        --dir)
            HAS_DIR_ARG=true
            FORWARD_ARGS+=("$1")
            shift
            if (($# > 0)); then
                PROJECT_DIR="$(cd "$1" && pwd)"
                FORWARD_ARGS+=("$PROJECT_DIR")
                shift
            fi
            ;;
        --color|--no-color)
            HAS_COLOR_ARG=true
            FORWARD_ARGS+=("$1")
            shift
            ;;
        *)
            FORWARD_ARGS+=("$1")
            shift
            ;;
    esac
done

TOOL_SRC="$PROJECT_DIR/src/main/java/io/velo/tools/TestFieldAccessCheck.java"
TOOL_CLS="$PROJECT_DIR/build/classes/java/main/io/velo/tools/TestFieldAccessCheck.class"

if [[ "$FORCE_COMPILE" == "true" ]]; then
    (cd "$PROJECT_DIR" && ./gradlew :compileJava :compileGroovy -q)
elif [[ ! -f "$TOOL_CLS" ]] || { [[ -f "$TOOL_SRC" ]] && [[ "$TOOL_SRC" -nt "$TOOL_CLS" ]]; }; then
    (cd "$PROJECT_DIR" && ./gradlew :compileJava -q)
fi

find_groovy_jar() {
    local jar=""
    if [[ -d "$PROJECT_DIR/build/libs/lib" ]]; then
        jar="$(find "$PROJECT_DIR/build/libs/lib" -maxdepth 1 -type f -name 'groovy-[0-9]*.jar' \
            ! -name '*sources*' ! -name '*javadoc*' ! -name '*dateutil*' \
            2>/dev/null | sort -V | tail -n 1 || true)"
    fi
    if [[ -z "$jar" ]]; then
        local gradle_cache="${GRADLE_USER_HOME:-$HOME/.gradle}/caches"
        if [[ -d "$gradle_cache" ]]; then
            jar="$(find "$gradle_cache" -type f -name 'groovy-[0-9]*.jar' \
                ! -name '*sources*' ! -name '*javadoc*' ! -name '*dateutil*' \
                2>/dev/null | sort -V | tail -n 1 || true)"
        fi
    fi
    printf '%s' "$jar"
}

GROOVY_JAR="$(find_groovy_jar)"
if [[ -z "$GROOVY_JAR" ]]; then
    echo "Error: groovy-*.jar not found in build/libs/lib or Gradle cache. Run './gradlew :compileJava' first." >&2
    exit 2
fi

JAVA_ARGS=()
if [[ "$HAS_DIR_ARG" == "false" ]]; then
    JAVA_ARGS+=(--dir "$PROJECT_DIR")
fi
if [[ "$HAS_COLOR_ARG" == "false" ]]; then
    if [[ -t 1 ]]; then
        JAVA_ARGS+=(--color)
    else
        JAVA_ARGS+=(--no-color)
    fi
fi

exec java -cp "$PROJECT_DIR/build/classes/java/main:$GROOVY_JAR" \
    io.velo.tools.TestFieldAccessCheck \
    "${JAVA_ARGS[@]}" \
    "${FORWARD_ARGS[@]}"
