fn main() {
    prost_build::compile_protos(&["proto/format_v2.proto"], &["proto/"]).unwrap();
}