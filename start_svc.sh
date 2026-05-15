#!/bin/sh
set -eu

if [ "$#" -ne 3 ]; then
  echo "usage: $0 <key-file> <namespace> <image-name>" >&2
  exit 1
fi

KEY_FILE=$1
NAMESPACE=$2
IMAGE_NAME=$3
RELEASE=sr
CHART_DIR="$(dirname "$0")/charts/sr"

if [ ! -f "$KEY_FILE" ]; then
  echo "key file not found: $KEY_FILE" >&2
  exit 1
fi

helm upgrade --install "$RELEASE" "$CHART_DIR" \
  --namespace "$NAMESPACE" \
  --create-namespace \
  --set image.repository="$IMAGE_NAME" \
  --set-file serverKey="$KEY_FILE"
