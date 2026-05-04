#!/usr/bin/env bash
#
# Generate PHP classes (protobuf messages + gRPC service stubs) for the OTLP
# wire format from the upstream open-telemetry/opentelemetry-proto repository.
#
# Output:
#   src/Opentelemetry/Proto/...
#   src/GPBMetadata/Opentelemetry/...
#
# Run from anywhere; paths are anchored to the package root.
#
# Requires: protoc, grpc_php_plugin, git
# In this repo: nix-shell --arg with-protoc true

set -euo pipefail

PROTO_REPO_URL="https://github.com/open-telemetry/opentelemetry-proto.git"
PROTO_REPO_REF="${PROTO_REPO_REF:-v1.10.0}"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PACKAGE_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
CLONE_DIR="${PACKAGE_ROOT}/.opentelemetry-proto"
OUT_DIR="${PACKAGE_ROOT}/src"

require_tool() {
    if ! command -v "$1" >/dev/null 2>&1; then
        echo "error: required tool '$1' not found on PATH" >&2
        echo "       enter the dev shell first: nix-shell --arg with-protoc true" >&2
        exit 1
    fi
}

require_tool protoc
require_tool grpc_php_plugin
require_tool git

echo "==> opentelemetry-proto ref: ${PROTO_REPO_REF}"

if [ -d "${CLONE_DIR}/.git" ]; then
    echo "==> refreshing existing clone at ${CLONE_DIR}"
    git -C "${CLONE_DIR}" fetch --tags --depth 1 origin "${PROTO_REPO_REF}"
    git -C "${CLONE_DIR}" -c advice.detachedHead=false checkout "${PROTO_REPO_REF}"
else
    echo "==> cloning ${PROTO_REPO_URL} into ${CLONE_DIR}"
    git clone --depth 1 --branch "${PROTO_REPO_REF}" "${PROTO_REPO_URL}" "${CLONE_DIR}"
fi

echo "==> wiping previously generated trees"
rm -rf "${OUT_DIR}/Opentelemetry" "${OUT_DIR}/GPBMetadata"
mkdir -p "${OUT_DIR}"

mapfile -t PROTO_FILES < <(find "${CLONE_DIR}/opentelemetry/proto" -type f -name '*.proto' | sort)

if [ "${#PROTO_FILES[@]}" -eq 0 ]; then
    echo "error: no .proto files found under ${CLONE_DIR}/opentelemetry/proto" >&2
    exit 1
fi

echo "==> compiling ${#PROTO_FILES[@]} .proto files"
protoc \
    --proto_path="${CLONE_DIR}" \
    --php_out="${OUT_DIR}" \
    --grpc_out="${OUT_DIR}" \
    --plugin=protoc-gen-grpc="$(command -v grpc_php_plugin)" \
    "${PROTO_FILES[@]}"

PHP_COUNT=$(find "${OUT_DIR}/Opentelemetry" "${OUT_DIR}/GPBMetadata" -type f -name '*.php' | wc -l | tr -d ' ')

echo
echo "==> done"
echo "    ref:           ${PROTO_REPO_REF}"
echo "    php files:     ${PHP_COUNT}"
echo "    output root:   ${OUT_DIR}"
echo "    top-level:"
find "${OUT_DIR}/Opentelemetry" "${OUT_DIR}/GPBMetadata" -mindepth 0 -maxdepth 1 -type d | sed 's|^|      |'
