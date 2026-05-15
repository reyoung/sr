#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
  echo "usage: $0 <namespace>" >&2
  exit 1
fi

NAMESPACE=$1
RELEASE=sr

helm uninstall "$RELEASE" --namespace "$NAMESPACE"
