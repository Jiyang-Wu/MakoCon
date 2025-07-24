fn main() -> miette::Result<()> {
    let path = std::path::PathBuf::from("src"); // include path

    let mut b = autocxx_build::Builder::new("src/main.rs", &[&path])
        .extra_clang_args(&["-std=c++17"])
        .build()
        .unwrap();
    b.flag_if_supported("-std=c++17")// use "-std:c++17" here if using msvc on windows
        .file("src/kv_store.cpp") 
        .compile("simpleKV"); // arbitrary library name, pick anything
    println!("cargo:rerun-if-changed=src/main.rs");
    // Add instructions to link to any C++ libraries you need.

    Ok(())
}