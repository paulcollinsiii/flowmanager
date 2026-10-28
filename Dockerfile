# syntax=docker/dockerfile:1

# Build stage: the release binary is compiled inside the image build, so nothing from the
# host's target/ directory is used. protoc is needed by crates/sched-keda/build.rs; the C
# toolchain for aws-lc-sys is already in the rust image.
FROM rust:1-bookworm AS build
RUN apt-get update \
 && apt-get install -y --no-install-recommends protobuf-compiler \
 && rm -rf /var/lib/apt/lists/*
WORKDIR /src
COPY . .
# Size-oriented release settings, given as environment so Cargo.toml is left alone.
ENV CARGO_PROFILE_RELEASE_STRIP=symbols \
    CARGO_PROFILE_RELEASE_LTO=fat \
    CARGO_PROFILE_RELEASE_CODEGEN_UNITS=1 \
    CARGO_PROFILE_RELEASE_PANIC=abort
# The cache mounts keep the registry and build artifacts between image builds; the binary
# is copied out because target/ is not part of the layer.
RUN --mount=type=cache,target=/usr/local/cargo/registry \
    --mount=type=cache,target=/src/target \
    cargo build --release --locked -p sched-service \
 && cp target/release/sched-service /sched-service

# Runtime stage: distroless "cc" is glibc, libgcc, CA certificates and tzdata, with no shell
# or package manager. The binary needs the CA store (TLS to the cluster and the router) and
# tzdata (job time zones); `static` would lack both. The nonroot variant runs as 65532.
FROM gcr.io/distroless/cc-debian12:nonroot
COPY --from=build /sched-service /usr/local/bin/sched-service
USER 65532:65532
# 9080 Restate endpoint (HTTP/2), 9090 metrics and /healthz, 9095 KEDA scaler (gRPC),
# 9096 toggle listener and 9097 webhook listener (both off unless their flags are set).
EXPOSE 9080 9090 9095 9096 9097
ENTRYPOINT ["/usr/local/bin/sched-service"]
