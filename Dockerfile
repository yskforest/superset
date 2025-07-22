ARG BASE_IMAGE=apache/superset:latest
FROM ${BASE_IMAGE}

USER root
RUN uv pip install --no-cache-dir \
    psycopg2-binary pillow
    
USER superset
