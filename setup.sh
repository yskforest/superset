#!/bin/bash

SCTIPR_DIR=$(cd $(dirname $0);pwd)
cd ${SCTIPR_DIR}

mkdir -p docker_volumes/superset_home
mkdir -p docker_volumes/postgres_data
chmod -R 755 ./docker_volumes
chown -R 1000:1000 ./docker_volumes

docker compose up -d
# 初期化
docker compose exec superset superset db upgrade
docker compose exec superset superset init
docker compose exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin
docker compose down

# mkdir postgres_data
# chmod -R 700 ./postgres_data
# chown -R 999:999 ./postgres_data
