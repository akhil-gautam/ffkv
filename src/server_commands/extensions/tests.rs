use super::*;

#[test]
fn binary_extended_load_and_bounded_key_metadata() {
    let args = vec![
        Bytes::from_static(b"CONFIG"),
        Bytes::from_static(b"prefix"),
        Bytes::from_static(b"\0\xff"),
        Bytes::from_static(b"ARGS"),
        Bytes::from_static(b"CONFIG"),
        Bytes::from_static(b"\xff\0"),
    ];
    let (config, argv) = loadex(&args).unwrap();
    assert_eq!(
        config,
        vec![(Bytes::from_static(b"prefix"), Bytes::from_static(b"\0\xff"))]
    );
    assert_eq!(argv, &args[4..]);
    let metadata = CommandMetadata {
        name: "fixture.keys".into(),
        module_name: "fixture".into(),
        arity: -1,
        flags: vec!["readonly"],
        categories: vec!["read"],
        first_key: 1,
        last_key: i32::MAX as i64,
        key_step: 2,
    };
    assert_eq!(
        metadata.keys(&args),
        vec![args[1].clone(), args[3].clone(), args[5].clone()]
    );
}

#[cfg(unix)]
#[tokio::test]
async fn compiled_public_abi_fixture_has_real_lifecycle_config_and_pinning() {
    use std::{path::PathBuf, process::Command};
    static NEXT: AtomicUsize = AtomicUsize::new(0);
    let directory = std::env::temp_dir().join(format!(
        "ffkv-native-abi-{}-{}",
        std::process::id(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    ));
    std::fs::create_dir(&directory).unwrap();
    struct Cleanup(PathBuf);
    impl Drop for Cleanup {
        fn drop(&mut self) {
            let _ = std::fs::remove_dir_all(&self.0);
        }
    }
    let _cleanup = Cleanup(directory.clone());
    let library = directory.join(if cfg!(target_os = "macos") {
        "fixture.dylib"
    } else {
        "fixture.so"
    });
    let mut cc = Command::new(std::env::var("CC").unwrap_or_else(|_| "cc".into()));
    cc.args(["-std=c11", "-D_POSIX_C_SOURCE=200809L", "-fPIC"]);
    cc.arg(if cfg!(target_os = "macos") {
        "-dynamiclib"
    } else {
        "-shared"
    });
    let result = cc
        .arg(PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/bdd/fixtures/module_fixture.c"))
        .arg("-o")
        .arg(&library)
        .output()
        .unwrap();
    assert!(
        result.status.success(),
        "{}",
        String::from_utf8_lossy(&result.stderr)
    );
    let path = Bytes::copy_from_slice(library.as_os_str().as_encoded_bytes());
    let extensions = Extensions::new();
    assert!(matches!(extensions.list().await,Resp::Array(Some(v)) if v.is_empty()));
    for mode in ["--partial-fail", "--unknown-api"] {
        assert!(matches!(
            extensions
                .load(&path, &[Bytes::copy_from_slice(mode.as_bytes())], &[])
                .await,
            Resp::Error(_)
        ));
        assert!(
            extensions
                .prepare_sync(&[
                    Bytes::from_static(b"fixture.echo"),
                    Bytes::from_static(b"x")
                ])
                .is_none()
        );
    }
    assert_eq!(
        extensions
            .load(
                &path,
                &[Bytes::from_static(b"\xff\0")],
                &[(Bytes::from_static(b"prefix"), Bytes::from_static(b"\0\xff"))]
            )
            .await,
        Resp::Simple("OK")
    );
    let invoke = |args: &[Bytes]| extensions.prepare_sync(args).unwrap().unwrap();
    let echo = invoke(&[
        Bytes::from_static(b"fixture.echo"),
        Bytes::from_static(b"hi\0"),
    ]);
    assert!(echo.flags().contains(&"readonly"));
    assert!(echo.categories().contains(&"admin"));
    assert!(
        matches!(
            extensions.unload(&Bytes::from_static(b"fixture")).await,
            Resp::Error(_)
        ),
        "a prepared callback must pin its library"
    );
    assert_eq!(
        echo.execute(|_| panic!("echo unexpectedly called engine")),
        Resp::Bulk(Some(Bytes::from_static(b"\0\xffhi\0")))
    );
    assert_eq!(
        invoke(&[Bytes::from_static(b"fixture.initialization")]).execute(|_| panic!()),
        Resp::Array(Some(vec![Resp::Bulk(Some(Bytes::from_static(b"\xff\0")))]))
    );
    let get = invoke(&[
        Bytes::from_static(b"fixture.get"),
        Bytes::from_static(b"key"),
    ]);
    assert_eq!(get.keys(), &[Bytes::from_static(b"key")]);
    assert_eq!(get.metadata().arity, 2);
    assert!(get.script_allowed());
    assert!(get.transaction_allowed());
    assert_eq!(get.metadata().module_name, "fixture");
    assert!(
        extensions
            .command_metadata_sync()
            .iter()
            .all(|m| m.module_name == "fixture")
    );
    assert_eq!(
        get.execute(|args| {
            assert_eq!(
                args,
                &[Bytes::from_static(b"GET"), Bytes::from_static(b"key")]
            );
            Resp::Bulk(Some(Bytes::from_static(b"actual engine reply")))
        }),
        Resp::Bulk(Some(Bytes::from_static(b"actual engine reply")))
    );
    assert_eq!(
        invoke(&[
            Bytes::from_static(b"fixture.unload"),
            Bytes::from_static(b"1")
        ])
        .execute(|_| panic!()),
        Resp::Status(Bytes::from_static(b"OK"))
    );
    assert!(matches!(
        extensions.unload(&Bytes::from_static(b"fixture")).await,
        Resp::Error(_)
    ));
    assert_eq!(
        invoke(&[
            Bytes::from_static(b"fixture.sum"),
            Bytes::from_static(b"4"),
            Bytes::from_static(b"5")
        ])
        .execute(|_| panic!()),
        Resp::Integer(9)
    );
    assert_eq!(
        invoke(&[
            Bytes::from_static(b"fixture.unload"),
            Bytes::from_static(b"0")
        ])
        .execute(|_| panic!()),
        Resp::Status(Bytes::from_static(b"OK"))
    );
    assert_eq!(
        extensions.unload(&Bytes::from_static(b"fixture")).await,
        Resp::Simple("OK")
    );
    assert!(matches!(extensions.list().await,Resp::Array(Some(v)) if v.is_empty()));
    assert!(
        extensions
            .prepare_sync(&[Bytes::from_static(b"fixture.sum"), Bytes::from_static(b"1")])
            .is_none()
    );
}
