fn main() {
    println!("cargo:rerun-if-changed=src/server_commands/extensions/call.c");
    cc::Build::new()
        .cargo_metadata(false)
        .file("src/server_commands/extensions/call.c")
        .flag_if_supported("-std=c11")
        .compile("ffkv_module_call");
    // The C ABI belongs to the server binary, not the shared RESP client library.
    // Once a package has a library, rustc-link-lib would target that library only.
    let archive = std::path::PathBuf::from(std::env::var_os("OUT_DIR").expect("Cargo OUT_DIR"))
        .join("libffkv_module_call.a");
    println!("cargo:rustc-link-arg-bin=ffkv={}", archive.display());
}
