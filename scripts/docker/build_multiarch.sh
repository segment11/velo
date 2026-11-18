#!/usr/bin/env bash
# Build the multi-arch (linux/amd64 + linux/arm64) velo docker image.
#
#   ./scripts/docker/build_multiarch.sh            # build only, output a local oci tar
#   ./scripts/docker/build_multiarch.sh --push     # build and push to the registry
#   ./scripts/docker/build_multiarch.sh --load     # build and load into the local docker (single arch only)
#
# Environment overrides: IMAGE, TAG, PLATFORMS, BUILDER, OUTPUT.
set -euo pipefail

SCRIPT_DIR=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
VELO_DIR=$(cd "$SCRIPT_DIR/../.." && pwd)

IMAGE=${IMAGE:-registry.cn-guangzhou.aliyuncs.com/montplex/velo}
VELO_VERSION=$(grep -oP 'VELO_VERSION\s*=\s*"\K[^"]+' "$VELO_DIR/src/main/java/io/velo/ConfForGlobal.java")
TAG=${TAG:-$VELO_VERSION}
PLATFORMS=${PLATFORMS:-linux/amd64,linux/arm64}
BUILDER=${BUILDER:-multiarch-builder}
OUTPUT=${OUTPUT:-$VELO_DIR/build/velo-$VELO_VERSION-multiarch.oci.tar}

PUSH=false
LOAD=false

usage() {
    sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
    echo "Options:"
    echo "  --push            push the image to the registry"
    echo "  --load            load the image into the local docker daemon (single arch)"
    echo "  --image <name>    image repository, default $IMAGE"
    echo "  --tag <tag>       image tag, default $TAG"
    echo "  --platforms <p>   comma separated platforms, default $PLATFORMS"
    echo "  --builder <name>  buildx builder, default $BUILDER"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --push) PUSH=true; shift ;;
        --load) LOAD=true; shift ;;
        --image) IMAGE=$2; shift 2 ;;
        --tag) TAG=$2; shift 2 ;;
        --platforms) PLATFORMS=$2; shift 2 ;;
        --builder) BUILDER=$2; shift 2 ;;
        -h|--help) usage; exit 0 ;;
        *) echo "unknown option: $1"; usage; exit 1 ;;
    esac
done

cd "$VELO_DIR"

echo "==> building the jar (version $VELO_VERSION)"
./gradlew jar

echo "==> building the image for $PLATFORMS"
BUILD_ARGS=(
    --builder "$BUILDER"
    --platform "$PLATFORMS"
    --build-arg "VELO_VERSION=$VELO_VERSION"
    --tag "$IMAGE:$TAG"
)

if [[ "$PUSH" == true ]]; then
    BUILD_ARGS+=(--tag "$IMAGE:latest" --push)
elif [[ "$LOAD" == true ]]; then
    BUILD_ARGS+=(--load)
else
    mkdir -p "$(dirname "$OUTPUT")"
    BUILD_ARGS+=(--output "type=oci,dest=$OUTPUT")
fi

docker buildx build "${BUILD_ARGS[@]}" "$VELO_DIR"

if [[ "$PUSH" == true ]]; then
    echo "==> pushed $IMAGE:$TAG and $IMAGE:latest"
elif [[ "$LOAD" == true ]]; then
    echo "==> loaded $IMAGE:$TAG into the local docker daemon"
else
    echo "==> wrote the multi-arch image to $OUTPUT"
    echo "    load it with: docker buildx imagetools create -f $OUTPUT registry.cn-guangzhou.aliyuncs.com/montplex/velo:local"
fi
