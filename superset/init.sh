#!/usr/bin/env bash
set -e

echo ">>> Superset DB upgrade"
superset db upgrade

echo ">>> Create admin user (idempotent)"
superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@superset.com \
  --password admin || true

echo ">>> Superset init"
superset init

echo ">>> Superset init done"