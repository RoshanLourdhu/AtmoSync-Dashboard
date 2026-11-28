FROM apache/superset:latest

USER root
COPY requirements-local.txt /app/requirements-local.txt
RUN uv pip install --python /app/.venv/bin/python --no-cache -r /app/requirements-local.txt
USER superset
