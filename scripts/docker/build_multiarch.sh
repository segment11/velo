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
PUSH_PER_ARCH=false

usage() {
    sed -n '2,10p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
    echo
    echo "Options:"
    echo "  --push            push the image to the registry (one push of the whole manifest list)"
    echo "  --push-per-arch   push every platform on its own, then create the manifest list"
    echo "  --load            load the image into the local docker daemon (single arch)"
    echo "  --image <name>    image repository, default $IMAGE"
    echo "  --tag <tag>       image tag, default $TAG"
    echo "  --platforms <p>   comma separated platforms, default $PLATFORMS"
    echo "  --builder <name>  buildx builder, default $BUILDER"
}

while [[ $# -gt 0 ]]; do
    case "$1" in
        --push) PUSH=true; shift ;;
        --push-per-arch) PUSH_PER_ARCH=true; shift ;;
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

REGISTRY=${IMAGE%%/*}

login() {
    # `docker login` without --password-stdin prompts on stdin, it blocks forever
    # when the script runs without a tty, so feed the password from the env.
    if [[ -n "${DOCKER_PASSWORD:-}" ]]; then
        printf '%s' "$DOCKER_PASSWORD" | docker login --username "${DOCKER_USERNAME:-}" --password-stdin "$REGISTRY"
    elif [[ -t 0 ]]; then
        docker login "$REGISTRY"
    else
        echo "==> no tty and DOCKER_PASSWORD is not set, using the stored credentials"
    fi
}

if [[ "$PUSH" == true || "$PUSH_PER_ARCH" == true ]]; then
    login
fi

echo "==> building the image for $PLATFORMS"
BUILD_ARGS=(
    --builder "$BUILDER"
    --platform "$PLATFORMS"
    --build-arg "VELO_VERSION=$VELO_VERSION"
    --tag "$IMAGE:$TAG"
)

if [[ "$PUSH" == true ]]; then
    # One single push of the whole manifest list. The image is ~400 MB per
    # platform, so the upload takes a few minutes, the `pushing layers` step is
    # the slow one (the `[auth]` step right above it finishes in 0.0s).
    BUILD_ARGS+=(--tag "$IMAGE:latest" --push)
    docker buildx build "${BUILD_ARGS[@]}" "$VELO_DIR"
    echo "==> pushed $IMAGE:$TAG and $IMAGE:latest"
elif [[ "$PUSH_PER_ARCH" == true ]]; then
    # Two steps: push every platform on its own, each push is independent and
    # can be retried alone, then assemble the manifest list.
    IFS=',' read -ra PLATFORM_LIST <<< "$PLATFORMS"
    ARCH_REFS=()
    for platform in "${PLATFORM_LIST[@]}"; do
        arch=${platform##*/}
        arch_ref="$IMAGE:$TAG-$arch"
        echo "==> pushing $platform as $arch_ref"
        docker buildx build \
            --builder "$BUILDER" \
            --platform "$platform" \
            --build-arg "VELO_VERSION=$VELO_VERSION" \
            --tag "$arch_ref" \
            --push \
            "$VELO_DIR"
        ARCH_REFS+=("$arch_ref")
    done
    echo "==> creating the manifest list $IMAGE:$TAG"
    docker buildx imagetools create --tag "$IMAGE:$TAG" "${ARCH_REFS[@]}"
    docker buildx imagetools create --tag "$IMAGE:latest" "$IMAGE:$TAG"
    echo "==> pushed $IMAGE:$TAG and $IMAGE:latest"
elif [[ "$LOAD" == true ]]; then
    BUILD_ARGS+=(--load)
    docker buildx build "${BUILD_ARGS[@]}" "$VELO_DIR"
    echo "==> loaded $IMAGE:$TAG into the local docker daemon"
else
    mkdir -p "$(dirname "$OUTPUT")"
    BUILD_ARGS+=(--output "type=oci,dest=$OUTPUT")
    docker buildx build "${BUILD_ARGS[@]}" "$VELO_DIR"
    echo "==> wrote the multi-arch image to $OUTPUT"
    echo "    publish it with --push or --push-per-arch"
fi

