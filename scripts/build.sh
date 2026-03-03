#!/usr/bin/env bash
set -euo pipefail

GOOS_VAL=${GOOS:-$(go env GOOS)}
GOARCH_VAL=${GOARCH:-$(go env GOARCH)}
TAGS_VAL=${TAGS:-}

export CGO_ENABLED=1

build_sdk() {
  local tags="${1:-}"

  if [[ -n "${tags}" ]]; then
    echo "Building (GOOS=${GOOS_VAL} GOARCH=${GOARCH_VAL} TAGS=${tags})"
    go build -tags "${tags}" ./...
    return
  fi

  echo "Building (GOOS=${GOOS_VAL} GOARCH=${GOARCH_VAL})"
  go build ./...
}

has_dynamic_tag() {
  local tags_normalized
  tags_normalized=",${1// /,},"

  [[ "${tags_normalized}" == *",dynamic,"* ]]
}

ensure_dynamic_requirements() {
  if ! command -v pkg-config >/dev/null 2>&1; then
    echo "dynamic build requested, but pkg-config is not installed."
    echo "Install pkg-config and librdkafka, then retry."
    return 1
  fi

  if ! pkg-config --exists rdkafka; then
    echo "dynamic build requested, but librdkafka was not found via pkg-config."
    echo "Ensure rdkafka.pc is discoverable (set PKG_CONFIG_PATH when needed)."
    return 1
  fi
}

if [[ -n "${TAGS_VAL}" ]]; then
  if has_dynamic_tag "${TAGS_VAL}"; then
    ensure_dynamic_requirements
  fi

  build_sdk "${TAGS_VAL}"
  exit 0
fi

if [[ "${GOARCH_VAL}" == "arm" || "${GOARCH_VAL}" == "arm64" ]]; then
  echo "ARM build detected: trying default build first."
  if build_sdk; then
    exit 0
  fi

  echo "Default ARM build failed; retrying with TAGS=dynamic."
  ensure_dynamic_requirements
  build_sdk "dynamic"
  exit 0
fi

build_sdk
