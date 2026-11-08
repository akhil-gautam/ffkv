use super::super::Execution;
use super::*;

fn run(engine: &mut Engine, now: i64, args: &[&[u8]]) -> Resp {
    let args = args
        .iter()
        .map(|v| Bytes::copy_from_slice(v))
        .collect::<Vec<_>>();
    match engine
        .execute_at_time(&mut crate::engine::BoundCommand::bind(&args), now)
        .unwrap()
    {
        Execution::Reply(reply) => reply,
        Execution::Pushed { reply, .. } => reply,
        _ => panic!("unexpected blocked command"),
    }
}
fn bulk(reply: Resp) -> Option<Bytes> {
    match reply {
        Resp::Bulk(value) => value,
        _ => panic!("expected bulk"),
    }
}
fn rows(reply: Resp) -> Vec<Option<Bytes>> {
    match reply {
        Resp::Array(Some(values)) => values.into_iter().map(bulk).collect(),
        _ => panic!("expected array"),
    }
}
fn int(reply: Resp) -> i64 {
    match reply {
        Resp::Integer(v) => v,
        _ => panic!("expected integer"),
    }
}

fn canonical(mut object: Object) -> Object {
    match &mut object {
        Object::Set(values) => values.sort(),
        Object::Hash(values) => values.sort_by(|a, b| a.0.cmp(&b.0)),
        Object::HashWithExpiry(values) => values.sort_by(|a, b| a.0.cmp(&b.0)),
        Object::SortedSet(values) => values.sort_by(|a, b| a.0.cmp(&b.0)),
        _ => {}
    }
    object
}

#[test]
fn binary_core_and_extension_values_roundtrip_independently() {
    let commands: Vec<Vec<&[u8]>> = vec![
        vec![b"SET", b"key", b"\x00\xff\r\n"],
        vec![b"RPUSH", b"key", b"\x00", b"\xff", b"tail"],
        vec![b"SADD", b"key", b"\x00", b"\xff"],
        vec![b"HSET", b"key", b"\x00", b"\xff", b"field", b"value"],
        vec![b"ZADD", b"key", b"1.25", b"\xff", b"-2", b"\x00"],
        vec![b"JSON.SET", b"key", b"$", b"{\"binary\": [1,2,3]}"],
        vec![b"XADD", b"key", b"1-0", b"f", b"v"],
    ];
    for command in commands {
        let mut source = Engine::new();
        let reply = run(&mut source, 1000, &command);
        assert!(!matches!(reply, Resp::Error(_)));
        run(&mut source, 1000, &[b"PEXPIREAT", b"key", b"9000"]);
        let key = Bytes::from_static(b"key");
        let payload = source.export_key(&key, 1001).unwrap().unwrap();
        let mut target = Engine::new();
        assert!(matches!(
            target.import_key(&key, &payload, 1001),
            Resp::Simple("OK")
        ));
        assert_eq!(int(run(&mut target, 1001, &[b"PTTL", b"key"])), 7999);
        let original = source.dump_key(&key, 1001).unwrap().unwrap();
        let restored = target.dump_key(&key, 1001).unwrap().unwrap();
        assert_eq!(
            canonical(rdb::decode_dump(&original, 1 << 20).unwrap()),
            canonical(rdb::decode_dump(&restored, 1 << 20).unwrap())
        );
        run(&mut target, 1002, &[b"DEL", b"key"]);
        assert!(source.dump_key(&key, 1002).unwrap().is_some());
    }
}

#[test]
fn corrupt_restore_never_replaces_or_changes_destination_ttl() {
    let mut engine = Engine::new();
    run(&mut engine, 1000, &[b"SET", b"source", b"data"]);
    run(
        &mut engine,
        1000,
        &[b"SET", b"target", b"old", b"PX", b"5000"],
    );
    let mut dump = bulk(run(&mut engine, 1000, &[b"DUMP", b"source"]))
        .unwrap()
        .to_vec();
    dump[1] ^= 1;
    assert!(matches!(
        engine.restore_key(&Bytes::from_static(b"target"), &dump, None, true, 1001),
        Resp::Error(_)
    ));
    assert_eq!(
        bulk(run(&mut engine, 1001, &[b"GET", b"target"])).unwrap(),
        b"old".as_slice()
    );
    assert_eq!(int(run(&mut engine, 1001, &[b"PTTL", b"target"])), 4999);
}

#[test]
fn sort_pattern_projection_preflights_before_store() {
    let mut engine = Engine::new();
    for command in [
        vec![b"RPUSH".as_slice(), b"ids", b"a", b"b"],
        vec![b"SET", b"weight:a", b"20"],
        vec![b"SET", b"weight:b", b"10"],
        vec![b"HSET", b"row:a", b"name", b"Alice"],
        vec![b"HSET", b"row:b", b"name", b"Bob"],
    ] {
        run(&mut engine, 1000, &command);
    }
    let reply = run(
        &mut engine,
        1000,
        &[
            b"SORT_RO",
            b"ids",
            b"BY",
            b"weight:*",
            b"GET",
            b"#",
            b"GET",
            b"row:*->name",
        ],
    );
    assert_eq!(
        rows(reply),
        vec![
            Some(Bytes::from_static(b"b")),
            Some(Bytes::from_static(b"Bob")),
            Some(Bytes::from_static(b"a")),
            Some(Bytes::from_static(b"Alice"))
        ]
    );
    run(&mut engine, 1000, &[b"SET", b"destination", b"keep"]);
    run(&mut engine, 1000, &[b"SET", b"weight:a", b"not-a-number"]);
    assert!(matches!(
        run(
            &mut engine,
            1000,
            &[
                b"SORT",
                b"ids",
                b"BY",
                b"weight:*",
                b"STORE",
                b"destination"
            ]
        ),
        Resp::Error(_)
    ));
    assert_eq!(
        bulk(run(&mut engine, 1000, &[b"GET", b"destination"])).unwrap(),
        b"keep".as_slice()
    );
}

#[test]
fn copy_and_move_resolve_database_and_field_expirations() {
    let mut engine = Engine::new();
    run(&mut engine, 1000, &[b"HSET", b"original", b"field", b"old"]);
    run(
        &mut engine,
        1000,
        &[
            b"HPEXPIREAT",
            b"original",
            b"5000",
            b"FIELDS",
            b"1",
            b"field",
        ],
    );
    run(&mut engine, 1000, &[b"PEXPIREAT", b"original", b"9000"]);
    assert_eq!(
        int(run(
            &mut engine,
            1001,
            &[b"COPY", b"original", b"copy", b"DB", b"1"]
        )),
        1
    );
    engine.switch_database(1);
    assert_eq!(int(run(&mut engine, 1001, &[b"PTTL", b"copy"])), 7999);
    assert_eq!(
        bulk(run(&mut engine, 1001, &[b"HGET", b"copy", b"field"])).unwrap(),
        b"old".as_slice()
    );
    run(&mut engine, 1001, &[b"HSET", b"copy", b"field", b"new"]);
    engine.switch_database(0);
    assert_eq!(
        bulk(run(&mut engine, 1001, &[b"HGET", b"original", b"field"])).unwrap(),
        b"old".as_slice()
    );
    assert_eq!(
        int(run(&mut engine, 1002, &[b"MOVE", b"original", b"2"])),
        1
    );
    assert_eq!(int(run(&mut engine, 1002, &[b"EXISTS", b"original"])), 0);
    engine.switch_database(2);
    assert_eq!(int(run(&mut engine, 1002, &[b"PTTL", b"original"])), 7998);
    let reply = run(
        &mut engine,
        1002,
        &[b"HPEXPIRETIME", b"original", b"FIELDS", b"1", b"field"],
    );
    assert!(matches!(&reply,Resp::Array(Some(v))if matches!(v.as_slice(),[Resp::Integer(5000)])));
}

#[test]
fn streaming_multidatabase_rdb_and_metadata_install_is_atomic() {
    let mut source = Engine::new();
    run(
        &mut source,
        1000,
        &[b"SET", b"db0", b"zero", b"PX", b"5000"],
    );
    source.switch_database(2);
    run(&mut source, 1000, &[b"RPUSH", b"db2", b"a", b"b"]);
    let metadata = rdb_snapshot::RdbMetadata {
        function_sources: vec![Bytes::from_static(
            b"#!lua name=lib\nredis.register_function('f', function() return 1 end)",
        )],
        search_effects: vec![Bytes::from_static(b"resolved-index-state")],
        auxiliary: vec![],
    };
    let mut file = Vec::new();
    source
        .write_rdb_with_metadata(&mut file, 1001, &metadata)
        .unwrap();
    let mut target = Engine::new();
    let installed = target.install_rdb(&mut file.as_slice(), 1001).unwrap();
    assert_eq!(installed.function_sources, metadata.function_sources);
    assert_eq!(installed.search_effects, metadata.search_effects);
    assert_eq!(
        bulk(run(&mut target, 1001, &[b"GET", b"db0"])).unwrap(),
        b"zero".as_slice()
    );
    target.switch_database(2);
    assert_eq!(
        rows(run(&mut target, 1001, &[b"LRANGE", b"db2", b"0", b"-1"])),
        vec![
            Some(Bytes::from_static(b"a")),
            Some(Bytes::from_static(b"b"))
        ]
    );
    file[10] ^= 1;
    assert!(target.install_rdb(&mut file.as_slice(), 1001).is_err());
    assert_eq!(int(run(&mut target, 1001, &[b"LLEN", b"db2"])), 2);
}

#[test]
fn copy_move_database_context_survives_journal_replay() {
    let mut source = Engine::new();
    source.set_recording(true);
    let mut recovered = Engine::new();
    for command in [
        vec![b"RPUSH".as_slice(), b"list", b"first", b"second"],
        vec![b"PEXPIREAT", b"list", b"9000"],
        vec![b"COPY", b"list", b"copy", b"DB", b"1"],
        vec![b"MOVE", b"list", b"2"],
    ] {
        assert!(!matches!(run(&mut source, 1000, &command), Resp::Error(_)));
        recovered
            .replay(source.take_batch().expect("durable command"))
            .unwrap();
    }
    for database in [0, 1, 2] {
        source.switch_database(database);
        recovered.switch_database(database);
        for key in [Bytes::from_static(b"list"), Bytes::from_static(b"copy")] {
            assert_eq!(
                source.export_key(&key, 1001).unwrap(),
                recovered.export_key(&key, 1001).unwrap()
            );
        }
    }
}

#[test]
fn replica_snapshot_keeps_in_transit_expirations_and_real_access_clock() {
    let mut source = Engine::new();
    run(
        &mut source,
        1000,
        &[b"SET", b"key", b"value", b"PXAT", b"1500"],
    );
    run(&mut source, 1000, &[b"HSET", b"hash", b"field", b"value"]);
    run(
        &mut source,
        1000,
        &[b"HPEXPIREAT", b"hash", b"1500", b"FIELDS", b"1", b"field"],
    );
    let key = Bytes::from_static(b"key");
    let hash = Bytes::from_static(b"hash");
    source.entries.get_mut(&key).unwrap().access.last_access_ms = 100;
    source.entries.get_mut(&key).unwrap().access.frequency = 42;
    let mut file = Vec::new();
    source.write_rdb(&mut file, 1100).unwrap();
    let mut normal = Engine::new();
    normal.install_rdb(&mut file.as_slice(), 5000).unwrap();
    assert!(!normal.entries.contains_key(&key));
    assert!(!normal.entries.contains_key(&hash));
    let mut replica = Engine::new();
    replica
        .install_replica_rdb(&mut file.as_slice(), 5000)
        .unwrap();
    let entry = replica.entries.get(&key).unwrap();
    assert_eq!(entry.expires_at, Some(1500));
    assert_eq!(entry.access.last_access_ms, 4000);
    assert_eq!(entry.access.frequency, 42);
    let Value::Hash(value) = &replica.entries.get(&hash).unwrap().value else {
        panic!("expected hash")
    };
    assert_eq!(value.snapshot(1000).next().unwrap().2, Some(1500));
}

#[test]
fn rdb_roundtrip_preserves_database_above_default_sixteen() {
    let mut source = Engine::new();
    source.switch_database(65_535);
    run(&mut source, 1000, &[b"SET", b"high-database", b"retained"]);
    let mut file = Vec::new();
    source.write_rdb(&mut file, 1001).unwrap();
    let mut target = Engine::new();
    target
        .install_replica_rdb(&mut file.as_slice(), 1002)
        .unwrap();
    target.switch_database(65_535);
    assert_eq!(
        bulk(run(&mut target, 1002, &[b"GET", b"high-database"])).unwrap(),
        b"retained".as_slice()
    );
}

#[test]
fn copy_move_use_configured_database_count() {
    use std::sync::{Arc, atomic::AtomicU32};
    let mut engine = Engine::new();
    engine.set_database_limit(Arc::new(AtomicU32::new(32)));
    run(&mut engine, 1000, &[b"SET", b"source", b"value"]);
    assert_eq!(
        int(run(
            &mut engine,
            1000,
            &[b"COPY", b"source", b"copied", b"DB", b"31"]
        )),
        1
    );
    assert_eq!(int(run(&mut engine, 1000, &[b"MOVE", b"source", b"30"])), 1);
    assert!(matches!(
        run(
            &mut engine,
            1000,
            &[b"COPY", b"copied", b"invalid", b"DB", b"32"]
        ),
        Resp::Error(_)
    ));
    engine.switch_database(31);
    assert!(matches!(
        run(&mut engine, 1000, &[b"MOVE", b"copied", b"32"]),
        Resp::Error(_)
    ));
    assert_eq!(
        bulk(run(&mut engine, 1000, &[b"GET", b"copied"])).unwrap(),
        b"value".as_slice()
    );
}
