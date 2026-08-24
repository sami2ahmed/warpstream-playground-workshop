#!/usr/bin/env bash
#
# Renders docker/prometheus/prometheus.yml (a template) into prometheus.yml.tmp,
# which docker-compose-lab7.yml expects to mount. The workshop repo references
# this script in a comment but never ships it, so `docker-compose up` fails with
# "not a directory: Are you trying to mount a directory onto a file".
#
# Run from the workshop repo root.

set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE="docker/prometheus/prometheus.yml"
RENDERED="docker/prometheus/prometheus.yml.tmp"

if [[ ! -f "$TEMPLATE" ]]; then
  echo "error: $TEMPLATE not found; run this from the workshop repo root" >&2
  exit 1
fi

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  source .env
  set +a
fi

: "${VCI_ID:?VCI_ID not set — add it to .env or export it}"
: "${WARPSTREAM_APP_KEY:?WARPSTREAM_APP_KEY not set — add it to .env or export it}"

if [[ "$VCI_ID" != vci_* ]]; then
  echo "error: VCI_ID must start with 'vci_' (got: $VCI_ID)" >&2
  exit 1
fi

if [[ "$WARPSTREAM_APP_KEY" != aks_* ]]; then
  echo "error: WARPSTREAM_APP_KEY must start with 'aks_'. A playground session" >&2
  echo "       key (sks_) will not work; create an app key in the console." >&2
  exit 1
fi

# Docker creates a directory here if the mount target is missing, so clear it.
rm -rf "$RENDERED"

sed -e "s|VCI_ID_PLACEHOLDER|${VCI_ID}|g" \
    -e "s|APP_KEY_PLACEHOLDER|${WARPSTREAM_APP_KEY}|g" \
    "$TEMPLATE" > "$RENDERED"

echo "rendered $RENDERED for cluster $VCI_ID"

# `down -v` drops the grafana-data volume. Without this, GF_SECURITY_ADMIN_PASSWORD
# is ignored on re-runs and the documented admin/admin login fails.
docker-compose -p warpstream-lab7 -f docker/docker-compose-lab7.yml down -v
docker-compose -p warpstream-lab7 -f docker/docker-compose-lab7.yml up -d

cat <<'EOF'

Prometheus  http://localhost:9090   (Status -> Targets)
Grafana     http://localhost:3001   (admin / admin)

Agent metrics come from the internal HTTP port 8080, not 9092:
  curl -s http://localhost:8080/metrics | grep '^warpstream_'
EOF
