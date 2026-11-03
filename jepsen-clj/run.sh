#!/usr/bin/env bash
# Build and run a real Jepsen linearizability test against douchdb.
#
# The cluster nodes and the Jepsen runner share a user-defined Docker network.
# The runner gets the Docker socket so the custom nemesis can kill, restart,
# disconnect and reconnect node containers.
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
HERE="$ROOT/jepsen-clj"
IMAGE="${DOUCHDB_IMAGE:-douchdb:jepsen}"
RUNNER="${DOUCHDB_JEPSEN_RUNNER:-douchdb-jepsen-runner:latest}"
NETWORK="${DOUCHDB_NETWORK:-douchdb-jepsen-net}"
SECRET="${DOUCHDB_CLUSTER_SECRET:-jepsen-cluster-secret-please-change}"
Q="${DOUCHDB_Q:-4}"
REPLICAS="${DOUCHDB_N:-3}"
SYNC_SECS="${DOUCHDB_SYNC_SECS:-1}"
DB="${DOUCHDB_DB:-jepsen}"
HOST_PORT_BASE="${DOUCHDB_HOST_PORT_BASE:-15984}"
KEEP="${DOUCHDB_KEEP_CLUSTER:-0}"

IFS=',' read -r -a NODES <<< "${DOUCHDB_NODES:-douchdb-jepsen-0,douchdb-jepsen-1,douchdb-jepsen-2}"
NODES_CSV="$(IFS=,; echo "${NODES[*]}")"
NODE_URLS=()
for node in "${NODES[@]}"; do
  NODE_URLS+=("http://${node}:5984")
done
NODE_URLS_CSV="$(IFS=,; echo "${NODE_URLS[*]}")"

if [[ "${DOUCHDB_SKIP_BUILD:-0}" != "1" ]]; then
  echo "==> Building douchdb image ${IMAGE}"
  docker build -t "${IMAGE}" "${ROOT}"
  echo "==> Building Jepsen runner image ${RUNNER}"
  docker build -t "${RUNNER}" "${HERE}"
fi

if ! docker network inspect "${NETWORK}" >/dev/null 2>&1; then
  echo "==> Creating Docker network ${NETWORK}"
  docker network create "${NETWORK}" >/dev/null
fi

cleanup() {
  local status=$?
  mkdir -p "${HERE}/logs"
  for node in "${NODES[@]}"; do
    docker logs "${node}" > "${HERE}/logs/${node}.log" 2>&1 || true
  done
  if [[ "${KEEP}" != "1" ]]; then
    for node in "${NODES[@]}"; do
      docker rm -f "${node}" >/dev/null 2>&1 || true
    done
  else
    echo "==> Keeping node containers; logs in ${HERE}/logs"
  fi
  exit "${status}"
}
trap cleanup EXIT

echo "==> Starting ${#NODES[@]} douchdb nodes on ${NETWORK}"
for index in "${!NODES[@]}"; do
  node="${NODES[$index]}"
  host_port=$((HOST_PORT_BASE + index))
  docker rm -f "${node}" >/dev/null 2>&1 || true
  docker volume rm "${node}-data" >/dev/null 2>&1 || true
  docker run -d \
    --name "${node}" \
    --network "${NETWORK}" \
    -p "127.0.0.1:${host_port}:5984" \
    -v "${node}-data:/data" \
    "${IMAGE}" \
    --listen 0.0.0.0:5984 \
    --data /data \
    --q "${Q}" \
    --n "${REPLICAS}" \
    --nodes "${NODE_URLS_CSV}" \
    --me "${index}" \
    --sync-secs "${SYNC_SECS}" \
    --cluster-secret "${SECRET}" >/dev/null
done

echo "==> Waiting for nodes"
for index in "${!NODES[@]}"; do
  host_port=$((HOST_PORT_BASE + index))
  for _ in $(seq 1 240); do
    if curl -fsS "http://127.0.0.1:${host_port}/" >/dev/null 2>&1; then
      break
    fi
    sleep 0.25
  done
  if ! curl -fsS "http://127.0.0.1:${host_port}/" >/dev/null 2>&1; then
    echo "node ${NODES[$index]} did not start" >&2
    exit 1
  fi
done

echo "==> Running Jepsen"
docker run --rm \
  --network "${NETWORK}" \
  -v /var/run/docker.sock:/var/run/docker.sock \
  -v "${HERE}:/work" \
  -v "${HOME}/.m2:/root/.m2" \
  -e "DOUCHDB_NODES=${NODES_CSV}" \
  -e "DOUCHDB_DB=${DB}" \
  -e "DOUCHDB_NETWORK=${NETWORK}" \
  -e "DOUCHDB_CLUSTER_SECRET=${SECRET}" \
  -w /work \
  "${RUNNER}" \
  clojure -M -m douchdb.core \
    --nodes "${NODES_CSV}" \
    --network "${NETWORK}" \
    --db "${DB}" \
    "$@"
