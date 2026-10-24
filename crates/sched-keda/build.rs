//! Generates the KEDA external scaler protocol (server and client) from the vendored proto.
//! Needs `protoc` on `PATH` (or `PROTOC`); `mise.toml` lists it.

fn main() -> std::io::Result<()> {
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        // The proto's comment on StreamMetricSpec is a numbered list that rustdoc and
        // clippy read as badly indented Markdown; it is not needed in the generated docs.
        .disable_comments(["externalscaler.ExternalScaler.StreamMetricSpec"])
        .compile_protos(&["proto/externalscaler.proto"], &["proto"])
}
