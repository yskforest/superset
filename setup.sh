#!/bin/bash

SCTIPR_DIR=$(cd $(dirname $0);pwd)
cd ${SCTIPR_DIR}

mkdir superset_home
chmod -R 755 ./superset_home
chown -R 1000:1000 ./superset_home

mkdir postgres_data
chmod -R 700 ./postgres_data
chown -R 999:999 ./postgres_data


