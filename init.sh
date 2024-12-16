#!/bin/bash

docker compose up -d
docker compose exec superset superset db upgrade
docker compose exec superset superset init
docker compose exec superset superset fab create-admin \
  --username admin \
  --firstname Admin \
  --lastname User \
  --email admin@example.com \
  --password admin
