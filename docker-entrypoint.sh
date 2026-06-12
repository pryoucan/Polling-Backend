#!/bin/sh
set -e

echo "[entrypoint] applying database schema..."
node src/scripts/migrate.js

if [ "${AUTO_SEED:-true}" = "true" ]; then
  echo "[entrypoint] seeding questions if the database is empty..."
  node src/scripts/seed-if-empty.js
fi

echo "[entrypoint] starting: $*"
exec "$@"
