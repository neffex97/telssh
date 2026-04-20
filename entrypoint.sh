#!/bin/sh
set -e

CONFIG="/app/data/config.yaml"
SEED="/app/config.seed.yaml"

# If writable config doesn't exist yet, copy from the seed (mounted read-only)
if [ ! -f "$CONFIG" ] && [ -f "$SEED" ]; then
    echo "Initializing config from seed..."
    cp "$SEED" "$CONFIG"
fi

exec ./telssh "$@"
