#!/bin/bash

echo "Checking if ArangoDB is enabled: ${ARANGODB_ENABLED}"

if [ "$ARANGODB_ENABLED" = "false" ]; then
    echo "ArangoDB is disabled. Skipping startup."
    exit 0
fi

echo "Starting ArangoDB on ${ARANGODB_HOST}:${ARANGODB_PORT}"

# Ensure directories exist and have proper permissions
mkdir -p /var/lib/arangodb3 /var/lib/arangodb3-apps /var/log/arangodb3
chown -R arangodb:arangodb /var/lib/arangodb3 /var/lib/arangodb3-apps /var/log/arangodb3

# Set root password if provided (SIMPLIFIED and RELIABLE approach)
if [ -n "$ARANGODB_PASSWORD" ] && [ ! -f /var/lib/arangodb3/.password-set ]; then
    echo "Setting ArangoDB root password to: $ARANGODB_PASSWORD"
    
    # Start ArangoDB without authentication temporarily
    echo "Starting temporary ArangoDB instance for initial setup..."
    /usr/sbin/arangod \
        --server.endpoint tcp://127.0.0.1:8529 \
        --database.directory /var/lib/arangodb3 \
        --log.file /var/log/arangodb3/setup.log \
        --server.authentication false &
    
    TEMP_PID=$!
    
    # Wait for ArangoDB to be ready
    echo "Waiting for ArangoDB to start..."
    for i in {1..30}; do
        if curl -f -s http://127.0.0.1:8529/_api/version > /dev/null 2>&1; then
            echo "ArangoDB is ready for configuration"
            break
        fi
        sleep 1
    done
    
    # Set the password
    echo "Setting root password..."
    if curl -f -X PUT "http://127.0.0.1:8529/_api/user/root" \
        -H "Content-Type: application/json" \
        -d "{\"passwd\":\"$ARANGODB_PASSWORD\",\"active\":true}" > /dev/null 2>&1; then
        echo "✅ Password set successfully"
        touch /var/lib/arangodb3/.password-set
    else
        echo "❌ Failed to set password"
    fi
    
    # Stop temporary instance
    echo "Stopping temporary instance..."
    kill -TERM $TEMP_PID 2>/dev/null || true
    sleep 3
    kill -KILL $TEMP_PID 2>/dev/null || true
    wait $TEMP_PID 2>/dev/null || true
    echo "Temporary instance stopped"
fi

# Determine authentication setting
AUTH_SETTING="false"
if [ -n "$ARANGODB_PASSWORD" ] && [ -f /var/lib/arangodb3/.password-set ]; then
    AUTH_SETTING="true"
    echo "Starting with authentication enabled"
else
    echo "Starting with authentication disabled"
fi

echo "Starting ArangoDB server..."

# Start ArangoDB with final configuration
exec /usr/sbin/arangod \
    --server.endpoint tcp://${ARANGODB_HOST}:${ARANGODB_PORT} \
    --database.directory /var/lib/arangodb3 \
    --log.file /var/log/arangodb3/arangod.log \
    --server.authentication "$AUTH_SETTING" \
    --javascript.app-path /var/lib/arangodb3-apps