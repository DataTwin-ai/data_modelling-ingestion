# Data Modelling & Ingestion API
# Pulled from ECR Public to avoid Docker Hub rate limits in CodeBuild.
FROM public.ecr.aws/docker/library/python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

WORKDIR /app

COPY requirements.txt .
RUN pip install -r requirements.txt

# Application code only. config.ini is NOT baked in: it is mounted at
# /app/config.ini at runtime (see deploy/deploy.sh).
COPY app.py api.py ./

# Non-root user. /app must be writable because app.py writes log_<ts>.txt
# next to itself; /app/exports is mounted from the host.
RUN useradd --uid 10001 --no-create-home --shell /usr/sbin/nologin appuser \
    && mkdir -p /app/exports \
    && chown -R appuser:appuser /app
USER appuser

EXPOSE 8000

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
    CMD python -c "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://127.0.0.1:8000/api/health', timeout=4).status == 200 else 1)"

# ROOT_PATH: URL prefix when served behind a reverse proxy
# (e.g. /data-model-ingestion), so /docs loads the right openapi.json.
ENV ROOT_PATH=""
CMD ["sh", "-c", "exec uvicorn api:app --host 0.0.0.0 --port 8000 --root-path \"$ROOT_PATH\" --proxy-headers --forwarded-allow-ips=127.0.0.1,172.16.0.0/12"]
