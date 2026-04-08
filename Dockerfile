# ARG DIST_IMG=gcr.io/distroless/cc:nonroot
ARG DIST_IMG=gcr.io/distroless/cc:debug

ARG RUST_VERSION=1.85
# ARG RUST_IMG_ALT=-slim-bullseye
ARG RUST_IMG_ALT=-bullseye

FROM --platform=${BUILDPLATFORM} rust:${RUST_VERSION}${RUST_IMG_ALT} as builder

RUN apt-get update && apt-get -y upgrade && apt-get install -y cmake libclang-dev

ARG TARGETOS
ARG TARGETARCH
ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG LIBC=gnu # gnu or musl, switch to musl if using alpine distritubtion image
ARG APT_MIRROR=mirrors.ustc.edu.cn
ARG RUSTUP_DIST_SERVER=https://mirrors.ustc.edu.cn/rust-static
ARG BUILD_ARGS
ARG MODULE_NAME="dt-main"


ENV RUSTUP_DIST_SERVER=${RUSTUP_DIST_SERVER}

RUN --mount=type=cache,target=$CARGO_HOME/git,rw \
    --mount=type=cache,target=$CARGO_HOME/registry,rw \
    rustup target add \
        aarch64-unknown-linux-${LIBC} \
        x86_64-unknown-linux-${LIBC}

# # Add following RUN cmd if using slim image 
# RUN --mount=type=cache,target=/var/cache/apt,rw \
#     sed -i "s/deb.debian.org/${APT_MIRROR}/g" /etc/apt/sources.list && \
#     apt update && \
#     apt install --no-install-recommends -y \
#         pkg-config \
#         libssl-dev \
#         libcrypto++-dev

WORKDIR /app
COPY . ./

# RUN --mount=type=cache,target=$CARGO_HOME/git,rw \
#     --mount=type=cache,target=$CARGO_HOME/registry,rw \
#     cargo update

# RUN --mount=type=cache,target=$CARGO_HOME/git,rw \
#     --mount=type=cache,target=$CARGO_HOME/registry,rw \
#     --mount=type=cache,target=/app/target,rw \
#     cargo build --release ${BUILD_ARGS} && \
#     mkdir -p bin/ && \
#     cp /app/target/release/${MODULE_NAME} bin/

RUN --mount=type=cache,target=$CARGO_HOME/git,rw \
    --mount=type=cache,target=$CARGO_HOME/registry,rw \
    --mount=type=cache,target=/app/target,rw \
    bash -c export BUILD_ARGS="${BUILD_ARGS} --bin ${MODULE_NAME}" ; \
    if [ "${TARGETPLATFORM}" != "${BUILDPLATFORM}" ] || [ "${LIBC}" == "musl" ]; then \
        if [ "${TARGETPLATFORM}" == "linux/amd64" ]; then \
            BUILD_TARGET="x86_64-unknown-${LIBC}" ; \
        elif [ "${TARGETOS}/${TARGETARCH}" == "linux/arm64" ]; then \
            BUILD_TARGET="aarch64-unknown-${LIBC}" ; \
        fi ; \
    fi ; \
    set -x ; \
    if [ -n "${BUILD_TARGET}" ]; then \
        BUILD_ARGS="${BUILD_ARGS} --target ${BUILD_TARGET}" ; \
    fi ; \
    RUSTFLAGS="--cfg tokio_unstable" cargo build --release ${BUILD_ARGS} && \
    mkdir -p bin/ && \
    cp /app/target/release/${MODULE_NAME} bin/

######
# Use distroless as minimal base image to package the binary
# Refer to https://github.com/GoogleContainerTools/distroless for more details
FROM ${DIST_IMG} as dist

#TARGETOS - OS component from --platform, e.g. linux
#TARGETARCH - Architecture from --platform, e.g. arm64
ARG MODULE_NAME="dt-main"
ARG APT_MIRROR=mirrors.ustc.edu.cn

COPY log4rs.yaml /log4rs.yaml
COPY --from=builder /app/bin/${MODULE_NAME} /ape-dts

ENTRYPOINT [ "/ape-dts" ]