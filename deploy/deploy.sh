#!/usr/bin/env bash
# Deploys the data-modelling-ingestion container on a SHARED EC2 host.
#
# Safety rules (other services run on this host):
#   * Only touches the container named $CONTAINER and images of $IMAGE_REPO.
#   * Never runs docker system/volume/network prune, never edits nginx,
#     systemd units, firewall rules or other containers.
#   * Aborts (exit 1, no changes) if $HOST_PORT is held by anything else.
#   * Rolls back to the previous image if the new one fails its health check.
#
# Required env: AWS_REGION, IMAGE_REPO (ECR repo URI, no tag), IMAGE_TAG
# Optional env: HOST_PORT (8010), BIND_ADDR (127.0.0.1), CONTAINER,
#               APP_DIR (/opt/data-modelling-ingestion), CONFIG_PARAM, KEEP_IMAGES (3)
set -euo pipefail

: "${AWS_REGION:?AWS_REGION is required}"
: "${IMAGE_REPO:?IMAGE_REPO is required}"
: "${IMAGE_TAG:?IMAGE_TAG is required}"
HOST_PORT="${HOST_PORT:-8010}"
BIND_ADDR="${BIND_ADDR:-127.0.0.1}"
CONTAINER="${CONTAINER:-data-modelling-ingestion}"
APP_DIR="${APP_DIR:-/opt/data-modelling-ingestion}"
CONFIG_PARAM="${CONFIG_PARAM:-/data-modelling-ingestion/config.ini}"
KEEP_IMAGES="${KEEP_IMAGES:-3}"
APP_UID=10001
NEW_IMAGE="${IMAGE_REPO}:${IMAGE_TAG}"
REGISTRY="${IMAGE_REPO%%/*}"

log() { echo "[deploy $(date -u +%H:%M:%S)] $*"; }
fail() { echo "[deploy] ERROR: $*" >&2; exit 1; }

# Use the host AWS CLI if present, otherwise the official CLI image
# (host network so it can reach the instance-role metadata endpoint).
aws_cli() {
  if command -v aws >/dev/null 2>&1; then
    aws "$@"
  else
    docker run --rm --network host -e AWS_REGION="$AWS_REGION" \
      public.ecr.aws/aws-cli/aws-cli:2.34.41 "$@"
  fi
}

# ---------------------------------------------------------------- pre-checks
command -v docker >/dev/null 2>&1 || fail "docker is not installed on this host"
docker info >/dev/null 2>&1 || fail "docker daemon is not reachable"

avail_kb=$(df --output=avail -k /var/lib/docker 2>/dev/null | tail -1 || df --output=avail -k / | tail -1)
[ "${avail_kb:-0}" -gt 2097152 ] || fail "less than 2 GB free disk; refusing to pull"

container_exists=false
if docker container inspect "$CONTAINER" >/dev/null 2>&1; then
  container_exists=true
fi

# Port guard: the port must be free, or published by OUR container.
if ss -ltnH "sport = :${HOST_PORT}" | grep -q .; then
  owner=$(docker ps --filter "publish=${HOST_PORT}" --format '{{.Names}}' | head -1)
  if [ "$owner" != "$CONTAINER" ]; then
    ss -ltnp "sport = :${HOST_PORT}" || true
    fail "port ${HOST_PORT} is in use by something other than '${CONTAINER}' (owner: '${owner:-non-docker process}'). Not touching it."
  fi
fi

PREV_IMAGE=""
if $container_exists; then
  PREV_IMAGE=$(docker container inspect --format '{{.Config.Image}}' "$CONTAINER")
  log "current image: ${PREV_IMAGE}"
fi

# ---------------------------------------------------------------- pull
log "logging in to ${REGISTRY}"
aws_cli ecr get-login-password --region "$AWS_REGION" | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null
log "pulling ${NEW_IMAGE}"
docker pull "$NEW_IMAGE" >/dev/null

# ---------------------------------------------------------------- config
mkdir -p "$APP_DIR/exports"
tmp_cfg=$(mktemp "$APP_DIR/.config.ini.XXXXXX")
aws_cli ssm get-parameter --region "$AWS_REGION" --name "$CONFIG_PARAM" \
  --with-decryption --query Parameter.Value --output text > "$tmp_cfg"
[ -s "$tmp_cfg" ] || { rm -f "$tmp_cfg"; fail "config parameter ${CONFIG_PARAM} is empty"; }
chmod 600 "$tmp_cfg"
mv -f "$tmp_cfg" "$APP_DIR/config.ini"
chown "$APP_UID:$APP_UID" "$APP_DIR/config.ini" "$APP_DIR/exports"

# ---------------------------------------------------------------- run
run_container() {
  docker run -d \
    --name "$CONTAINER" \
    --restart unless-stopped \
    -p "${BIND_ADDR}:${HOST_PORT}:8000" \
    -v "$APP_DIR/config.ini:/app/config.ini:ro" \
    -v "$APP_DIR/exports:/app/exports" \
    --log-driver json-file --log-opt max-size=20m --log-opt max-file=5 \
    --label "app=${CONTAINER}" \
    "$1" >/dev/null
}

health_ok() {
  for _ in $(seq 1 30); do
    if curl -fsS --max-time 4 "http://127.0.0.1:${HOST_PORT}/api/health" >/dev/null 2>&1; then
      return 0
    fi
    sleep 2
  done
  return 1
}

if $container_exists; then
  log "stopping old ${CONTAINER}"
  docker rm -f "$CONTAINER" >/dev/null
fi

log "starting ${NEW_IMAGE} on ${BIND_ADDR}:${HOST_PORT}"
run_container "$NEW_IMAGE"

if health_ok; then
  log "health check passed"
else
  log "health check FAILED; last logs:"
  docker logs --tail 50 "$CONTAINER" 2>&1 || true
  docker rm -f "$CONTAINER" >/dev/null || true
  if [ -n "$PREV_IMAGE" ]; then
    log "rolling back to ${PREV_IMAGE}"
    run_container "$PREV_IMAGE"
    health_ok && log "rollback healthy" || log "rollback ALSO unhealthy"
  fi
  fail "deployment of ${NEW_IMAGE} failed"
fi

# ---------------------------------------------------------------- cleanup
# Only images of THIS repository; keep the newest $KEEP_IMAGES, never the running one.
running_id=$(docker container inspect --format '{{.Image}}' "$CONTAINER")
docker image ls "$IMAGE_REPO" --format '{{.ID}}' | awk '!seen[$0]++' \
  | tail -n +"$((KEEP_IMAGES + 1))" | while read -r img; do
    [ -n "$img" ] || continue
    [[ "$running_id" == *"$img"* ]] && continue
    docker image rm "$img" >/dev/null 2>&1 && log "removed old image ${img}" || true
  done

log "deployed ${NEW_IMAGE}"
