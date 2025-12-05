#!/usr/bin/env bash
set -euo pipefail

# Wait for ArangoDB to be ready
echo "[run-app] Waiting for ArangoDB to start..."
max_attempts=30
attempt=1

while [ $attempt -le $max_attempts ]; do
    if curl -f http://localhost:8529/_api/version >/dev/null 2>&1; then
        echo "[run-app] ✅ ArangoDB is ready!"
        break
    fi
    echo "[run-app] ⏳ Waiting for ArangoDB... (attempt $attempt/$max_attempts)"
    sleep 2
    attempt=$((attempt + 1))
done

if [ $attempt -gt $max_attempts ]; then
    echo "[run-app] ❌ WARNING: ArangoDB not ready after $max_attempts attempts, starting app anyway..."
fi

# Launch the application
JAR="${APP_JAR_PATH:-/opt/app/onemcp.jar}"
echo "[run-app] launching $JAR"
exec java ${JAVA_OPTS:-} -jar "$JAR" ${APP_ARGS:-}