use super::{
    model::*,
    schema,
    state::{Effect, State},
};
use bytes::Bytes;
use std::collections::BTreeSet;
fn args(values: &[&str]) -> Vec<Bytes> {
    values
        .iter()
        .map(|v| Bytes::copy_from_slice(v.as_bytes()))
        .collect()
}
fn catalog() -> State {
    let (schema, _) = schema::create(
        &args(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SKIPINITIALSCAN",
            "STOPWORDS",
            "0",
            "SCHEMA",
            "title",
            "TEXT",
            "WITHSUFFIXTRIE",
            "price",
            "NUMERIC",
        ]),
        0,
    )
    .unwrap();
    let mut state = State::default();
    state.effect(Effect::Create(schema)).unwrap();
    state
}
fn document(state: &mut State, term: &str) {
    state
        .effect(Effect::Header {
            index: "idx".into(),
            id: 1,
            key: b"doc:1".to_vec(),
            score: 1.,
            expires_at: Some(100),
            field_deadline: Some(50),
            length: 1,
            synonym_generation: 0,
        })
        .unwrap();
    field(state, term);
}
fn field(state: &mut State, term: &str) {
    state
        .effect(Effect::Field {
            index: "idx".into(),
            id: 1,
            name: "title".into(),
            value: IndexedValue::Text {
                strings: vec![term.into()],
                tokens: vec![Token {
                    term: term.into(),
                    position: 0,
                    start: 0,
                    end: term.len(),
                }],
            },
        })
        .unwrap();
}
#[test]
fn snapshot_preserves_skipped_coverage_and_synonym_generations() {
    let mut state = catalog();
    assert!(state.indexes["idx"].docs.is_empty());
    document(&mut state, "boy");
    for terms in [["boy", "child"], ["boy", "child"], ["boy", "child"]] {
        state
            .effect(Effect::Synonyms {
                index: "idx".into(),
                group: "family".into(),
                terms: terms.into_iter().map(str::to_owned).collect(),
            })
            .unwrap();
    }
    let mut altered = state.indexes["idx"].schema.clone();
    altered
        .fields
        .extend(schema::fields(&args(&["tag", "TAG"]), false).unwrap());
    state.effect(Effect::Alter(altered)).unwrap();
    state
        .effect(Effect::Alias {
            alias: "view".into(),
            index: Some("idx".into()),
        })
        .unwrap();
    let mut restored = State::default();
    for effect in state.snapshot() {
        restored.apply_bytes(effect.unwrap()).unwrap();
    }
    let index = &restored.indexes["idx"];
    assert_eq!(index.synonym_generation, 3);
    assert_eq!(index.docs[&1].synonym_generation, 0);
    assert!(!index.docs[&1].fields.contains_key("price"));
    assert!(!index.docs[&1].fields.contains_key("tag"));
    assert_eq!(restored.aliases["view"], "idx");
    assert!(restored.deadlines.contains(&(50, "idx".into(), 1)));
    assert_eq!(index.suffixes["title"].terms("oy", true, 10), vec!["boy"]);
}
#[test]
fn pinned_snapshot_is_immutable_during_field_replacement() {
    let mut state = catalog();
    document(&mut state, "before");
    let snapshot = state.snapshot();
    field(&mut state, "after");
    let mut restored = State::default();
    for effect in snapshot {
        restored.apply_bytes(effect.unwrap()).unwrap();
    }
    let FieldIndex::Text(current) = &state.indexes["idx"].fields["title"] else {
        panic!("text schema")
    };
    assert!(current.contains_key("after"));
    assert!(!current.contains_key("before"));
    let FieldIndex::Text(pinned) = &restored.indexes["idx"].fields["title"] else {
        panic!("text schema")
    };
    assert!(pinned.contains_key("before"));
    assert!(!pinned.contains_key("after"));
}
#[test]
fn per_term_dictionary_deletion_and_document_removal_do_not_resurrect() {
    let mut state = catalog();
    document(&mut state, "removed");
    state
        .effect(Effect::DictionaryAdd {
            name: "words".into(),
            terms: BTreeSet::from(["one".into(), "two".into()]),
        })
        .unwrap();
    state
        .effect(Effect::DictionaryRemove {
            name: "words".into(),
            terms: BTreeSet::from(["one".into(), "two".into()]),
        })
        .unwrap();
    state
        .effect(Effect::Remove {
            index: "idx".into(),
            id: 1,
        })
        .unwrap();
    let mut restored = State::default();
    for effect in state.snapshot() {
        restored.apply_bytes(effect.unwrap()).unwrap();
    }
    assert!(restored.dictionaries.contains_key("words"));
    assert!(restored.dictionaries["words"].is_empty());
    assert!(restored.indexes["idx"].docs.is_empty());
    assert!(restored.deadlines.is_empty());
    let FieldIndex::Text(terms) = &restored.indexes["idx"].fields["title"] else {
        panic!("text schema")
    };
    assert!(terms.is_empty());
}

fn records(state: &State) -> Vec<Bytes> {
    let mut records = state.snapshot().map(Result::unwrap).collect::<Vec<_>>();
    records.sort();
    records
}
#[test]
fn disposable_catalog_restores_documents_schema_cursors_and_metadata() {
    use super::state::Cursor;
    use std::collections::{BTreeMap, VecDeque};
    let mut state = catalog();
    document(&mut state, "before");
    state
        .effect(Effect::Payload {
            index: "idx".into(),
            id: 1,
            value: Some(vec![7; 4096]),
        })
        .unwrap();
    state
        .effect(Effect::Alias {
            alias: "view".into(),
            index: Some("idx".into()),
        })
        .unwrap();
    state
        .effect(Effect::DictionaryAdd {
            name: "words".into(),
            terms: BTreeSet::from(["before".into()]),
        })
        .unwrap();
    state.cursor_insert(
        7,
        Cursor {
            index: "idx".into(),
            rows: VecDeque::from([BTreeMap::from([(
                "value".into(),
                serde_json::json!("original"),
            )])]),
            count: 1,
            max_idle: 1000,
            deadline: 1000,
            total: 1,
        },
    );
    state.next_cursor = 8;
    state.indexes.get_mut("idx").unwrap().uses = 9;
    let baseline = records(&state);
    let revision = state.metadata_revision;
    state.begin_rollback().unwrap();
    field(&mut state, "after");
    state
        .effect(Effect::Header {
            index: "idx".into(),
            id: 2,
            key: b"doc:2".to_vec(),
            score: 1.,
            expires_at: None,
            field_deadline: None,
            length: 0,
            synonym_generation: 0,
        })
        .unwrap();
    state
        .effect(Effect::Payload {
            index: "idx".into(),
            id: 1,
            value: None,
        })
        .unwrap();
    state
        .effect(Effect::Synonyms {
            index: "idx".into(),
            group: "g".into(),
            terms: BTreeSet::from(["after".into()]),
        })
        .unwrap();
    state
        .effect(Effect::Alias {
            alias: "view".into(),
            index: None,
        })
        .unwrap();
    state
        .effect(Effect::DictionaryRemove {
            name: "words".into(),
            terms: BTreeSet::from(["before".into()]),
        })
        .unwrap();
    state
        .effect(Effect::DictionaryAdd {
            name: "words".into(),
            terms: BTreeSet::from(["after".into()]),
        })
        .unwrap();
    state
        .effect(Effect::Rejection {
            index: "idx".into(),
            count: 3,
            message: "bad".into(),
            key: b"doc:2".to_vec(),
        })
        .unwrap();
    state
        .effect(Effect::FieldError {
            index: "idx".into(),
            field: "title".into(),
            count: 3,
            message: "bad".into(),
            key: b"doc:2".to_vec(),
        })
        .unwrap();
    state.save_config();
    state.config.min_prefix = 5;
    state.save_index_metadata("idx");
    state.indexes.get_mut("idx").unwrap().uses += 1;
    let mut cursor = state.cursor_remove(7).unwrap();
    cursor.rows.pop_front();
    state.cursor_insert(7, cursor);
    state.next_cursor = 99;
    assert_eq!(
        super::query::execute(
            &state.indexes["idx"],
            "after",
            &super::query::Options::default(),
            &state.config,
            1
        )
        .unwrap()
        .hits
        .len(),
        1
    );
    state.rollback().unwrap();
    assert_eq!(records(&state), baseline);
    assert_eq!(state.metadata_revision, revision);
    assert_eq!(state.next_cursor, 8);
    assert_eq!(state.config.min_prefix, 2);
    assert_eq!(
        state.cursors[&7].rows[0]["value"],
        serde_json::json!("original")
    );
    let index = &state.indexes["idx"];
    assert_eq!(index.uses, 9);
    assert_eq!(index.next_id, 2);
    assert_eq!(index.errors, 0);
    assert!(index.field_errors.is_empty());
    assert_eq!(index.docs[&1].payload.as_ref().unwrap().len(), 4096);
    assert_eq!(
        super::query::execute(
            index,
            "before",
            &super::query::Options::default(),
            &state.config,
            1
        )
        .unwrap()
        .hits
        .len(),
        1
    );
    assert!(
        super::query::execute(
            index,
            "after",
            &super::query::Options::default(),
            &state.config,
            1
        )
        .unwrap()
        .hits
        .is_empty()
    );
}
#[test]
fn disposable_clear_and_drop_recreation_restore_the_original_catalog() {
    let mut state = catalog();
    document(&mut state, "before");
    state
        .effect(Effect::Alias {
            alias: "view".into(),
            index: Some("idx".into()),
        })
        .unwrap();
    let baseline = records(&state);
    state.begin_rollback().unwrap();
    field(&mut state, "staged");
    state.effect(Effect::Drop("idx".into())).unwrap();
    let schema = catalog().indexes.remove("idx").unwrap().schema;
    state.effect(Effect::Create(schema.clone())).unwrap();
    document(&mut state, "recreated");
    state.effect(Effect::Clear).unwrap();
    state.effect(Effect::Create(schema)).unwrap();
    document(&mut state, "cleared");
    state.rollback().unwrap();
    assert_eq!(records(&state), baseline);
    assert_eq!(state.aliases["view"], "idx");
    assert_eq!(state.indexes["idx"].docs.len(), 1);
    assert_eq!(
        super::query::execute(
            &state.indexes["idx"],
            "before",
            &super::query::Options::default(),
            &state.config,
            1
        )
        .unwrap()
        .hits
        .len(),
        1
    );
}
#[test]
fn disposable_search_restores_lazy_engine_attachments_and_drop_is_fail_safe() {
    use super::{Engine, SearchRollback};
    use std::sync::Arc;
    let mut engines = [Engine::new(), Engine::new()];
    let rollback = SearchRollback::begin(&mut engines).unwrap();
    assert!(Arc::ptr_eq(
        engines[0].search.as_ref().unwrap(),
        engines[1].search.as_ref().unwrap()
    ));
    {
        let catalog = engines[0].search.as_ref().unwrap();
        catalog
            .lock()
            .effect(Effect::Create(catalog_schema()))
            .unwrap();
    }
    rollback.rollback(&mut engines).unwrap();
    assert!(engines.iter().all(|e| e.search.is_none()));
    let rollback = SearchRollback::begin(&mut engines).unwrap();
    let catalog = engines[0].search.clone().unwrap();
    catalog
        .lock()
        .effect(Effect::Create(catalog_schema()))
        .unwrap();
    drop(rollback);
    assert!(catalog.lock().indexes.is_empty());
}
fn catalog_schema() -> Schema {
    catalog().indexes.remove("idx").unwrap().schema
}

#[test]
fn vector_queries_are_identical_after_disposable_graph_updates() {
    let (schema, _) = schema::create(
        &args(&[
            "FT.CREATE",
            "vectors",
            "SKIPINITIALSCAN",
            "SCHEMA",
            "v",
            "VECTOR",
            "HNSW",
            "6",
            "TYPE",
            "FLOAT32",
            "DIM",
            "2",
            "DISTANCE_METRIC",
            "L2",
        ]),
        0,
    )
    .unwrap();
    let mut state = State::default();
    state.effect(Effect::Create(schema)).unwrap();
    for id in 1..40 {
        state
            .effect(Effect::Header {
                index: "vectors".into(),
                id,
                key: format!("doc:{id}").into_bytes(),
                score: 1.,
                expires_at: None,
                field_deadline: None,
                length: 0,
                synonym_generation: 0,
            })
            .unwrap();
        state
            .effect(Effect::Field {
                index: "vectors".into(),
                id,
                name: "v".into(),
                value: IndexedValue::Vector(vec![vec![id as f64, 0.]]),
            })
            .unwrap();
    }
    let results = |state: &State| {
        let FieldIndex::Vector(v) = &state.indexes["vectors"].fields["v"] else {
            panic!("vector schema")
        };
        v.query(&[5., 0.], 10, None, &std::collections::BTreeMap::new())
            .unwrap()
    };
    let before = results(&state);
    state.begin_rollback().unwrap();
    state
        .effect(Effect::Remove {
            index: "vectors".into(),
            id: 5,
        })
        .unwrap();
    state
        .effect(Effect::Field {
            index: "vectors".into(),
            id: 6,
            name: "v".into(),
            value: IndexedValue::Vector(vec![vec![0., 0.]]),
        })
        .unwrap();
    assert_ne!(results(&state), before);
    state.rollback().unwrap();
    assert_eq!(results(&state), before);
}

#[test]
fn strict_timeout_stops_postprocessing_instead_of_aliasing_return() {
    use std::{
        collections::BTreeMap,
        time::{Duration, Instant},
    };
    let state = catalog();
    let index = &state.indexes["idx"];
    let stages = args(&["APPLY", "@value + 1", "AS", "incremented"]);
    let rows = || vec![(1, BTreeMap::from([("value".into(), serde_json::json!(2))]))];
    let available = BTreeSet::from(["value".into()]);
    let expired = Some(Instant::now() - Duration::from_secs(1));
    let mut config = Config::default();
    let result = super::pipeline::execute_rows(
        index,
        rows(),
        &stages,
        BTreeMap::new(),
        &config,
        available.clone(),
        500,
        expired,
    )
    .unwrap();
    assert_eq!(result.rows[0]["incremented"].as_f64(), Some(3.0));
    assert!(result.warnings.is_empty());
    config.set_workers(1).unwrap();
    config.on_timeout_strict = true;
    let result = super::pipeline::execute_rows(
        index,
        rows(),
        &stages,
        BTreeMap::new(),
        &config,
        available.clone(),
        500,
        expired,
    )
    .unwrap();
    assert!(result.rows.is_empty());
    assert!(!result.warnings.is_empty());
    config.on_timeout_strict = false;
    config.on_timeout_fail = true;
    assert!(
        super::pipeline::execute_rows(
            index,
            rows(),
            &stages,
            BTreeMap::new(),
            &config,
            available,
            500,
            expired
        )
        .is_err()
    );
}

#[test]
fn disposable_configuration_restores_the_actual_pool_and_collector_schedule() {
    use std::sync::Arc;
    for workers in [0, 1] {
        let mut state = catalog();
        state.config.set_workers(workers).unwrap();
        let pool = state.config.pool.clone();
        let next = state.gc.next;
        state.begin_rollback().unwrap();
        state.save_config();
        state.config.set_workers(2).unwrap();
        state.config.gc.interval = 1;
        state.config.gc.threshold = 1;
        state.gc.next = 77;
        assert_eq!(state.config.pool.as_ref().unwrap().current_num_threads(), 2);
        state.rollback().unwrap();
        assert_eq!(state.config.workers, workers);
        assert_eq!(state.config.gc.interval, 30);
        assert_eq!(state.config.gc.threshold, 100);
        assert_eq!(state.gc.next, next);
        match (pool, state.config.pool.as_ref()) {
            (Some(before), Some(after)) => assert!(Arc::ptr_eq(&before, after)),
            (None, None) => {}
            _ => panic!("pool attachment changed"),
        }
    }
}

#[test]
fn swapdb_from_a_nonzero_database_reconciles_db0_and_preserves_the_catalog() {
    use super::Engine;
    fn run(engine: &mut Engine, command: &[&str]) -> super::Resp {
        let reply = match engine
            .execute_at_time(&mut crate::engine::BoundCommand::bind(&args(command)), 1000)
            .unwrap()
        {
            super::super::Execution::Reply(reply)
            | super::super::Execution::Pushed { reply, .. } => reply,
            _ => panic!("unexpected blocking search command"),
        };
        if let super::Resp::Error(message) = &reply {
            panic!("command {command:?}: {message}")
        }
        reply
    }
    fn total(reply: super::Resp) -> u64 {
        match reply {
            super::Resp::Array(Some(values)) => match values.first() {
                Some(super::Resp::Unsigned(n)) => *n,
                Some(super::Resp::Integer(n)) => *n as u64,
                _ => panic!("missing search count"),
            },
            _ => panic!("expected search array"),
        }
    }
    let mut engine = Engine::new();
    for command in [
        vec![
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ],
        vec!["FT.ALIASADD", "view", "idx"],
        vec!["HSET", "doc:old", "title", "before"],
        vec!["HSET", "doc:shared", "title", "before"],
        vec!["SELECT", "1"],
        vec!["HSET", "doc:new", "title", "after"],
        vec!["HSET", "doc:shared", "title", "after"],
        vec!["SELECT", "2"],
        vec!["SWAPDB", "0", "1"],
    ] {
        run(&mut engine, &command);
    }
    assert_eq!(engine.database(), 2);
    run(&mut engine, &["SELECT", "0"]);
    assert_eq!(
        total(run(
            &mut engine,
            &["FT.SEARCH", "view", "after", "NOCONTENT", "LIMIT", "0", "0"]
        )),
        2
    );
    assert_eq!(
        total(run(
            &mut engine,
            &["FT.SEARCH", "view", "before", "NOCONTENT"]
        )),
        0
    );
    run(&mut engine, &["SWAPDB", "1", "2"]);
    assert_eq!(
        total(run(
            &mut engine,
            &["FT.SEARCH", "view", "after", "NOCONTENT", "LIMIT", "0", "0"]
        )),
        2
    );
    let catalog = engine.search.as_ref().unwrap();
    let state = catalog.lock();
    assert_eq!(state.indexes.len(), 1);
    assert_eq!(state.aliases["view"], "idx");
    assert_eq!(state.indexes["idx"].schema.fields.len(), 1);
    assert_eq!(state.indexes["idx"].docs.len(), 2);
}

#[test]
fn coherent_swap_reconciliation_keeps_all_four_shards_indexed_without_moving_sources() {
    use super::{Catalog, Engine};
    use std::sync::Arc;
    fn run(engine: &mut Engine, command: &[Bytes]) {
        match engine
            .execute_at_time(&mut crate::engine::BoundCommand::bind(command), 1000)
            .unwrap()
        {
            super::super::Execution::Reply(super::Resp::Error(message)) => panic!("{message}"),
            super::super::Execution::Reply(_) | super::super::Execution::Pushed { .. } => {}
            _ => panic!("unexpected blocking execution"),
        }
    }
    fn key(role: &str, shard: usize) -> Bytes {
        (0..10000)
            .map(|n| Bytes::from(format!("doc:{role}:{shard}:{n}")))
            .find(|key| Engine::shard_for(key, 4) == shard)
            .unwrap()
    }
    fn set(engine: &mut Engine, key: Bytes, title: &'static str) {
        run(
            engine,
            &[
                Bytes::from_static(b"HSET"),
                key,
                Bytes::from_static(b"title"),
                Bytes::from_static(title.as_bytes()),
            ],
        );
    }
    let catalog = Arc::new(Catalog::new());
    let mut engines = (0..4).map(|_| Engine::new()).collect::<Vec<_>>();
    for engine in &mut engines {
        engine.search = Some(catalog.clone());
    }
    run(
        &mut engines[0],
        &args(&[
            "FT.CREATE",
            "idx",
            "PREFIX",
            "1",
            "doc:",
            "SCHEMA",
            "title",
            "TEXT",
        ]),
    );
    run(&mut engines[0], &args(&["FT.ALIASADD", "view", "idx"]));
    for (shard, engine) in engines.iter_mut().enumerate() {
        set(engine, key("shared", shard), "before");
        set(engine, key("old", shard), "before");
        engine.switch_database(1);
        set(engine, key("shared", shard), "after");
        set(engine, key("new", shard), "after");
        engine.switch_database(shard as u32 + 2);
    }
    for (shard, engine) in engines.iter_mut().enumerate() {
        engine.set_recording(shard == 0);
        engine.set_search_reindexing(false);
        run(engine, &args(&["SWAPDB", "0", "1"]));
        engine.finish_pending_search();
        if let Some(batch) = engine.take_batch() {
            assert!(
                !batch
                    .mutations
                    .iter()
                    .any(|mutation| matches!(mutation, super::super::Mutation::Search { .. })),
                "physical shard emitted an incoherent search effect"
            );
        }
        engine.set_search_reindexing(true);
    }
    let effects = Engine::search_reconcile_swapped(&mut engines, 0, 1, 1000).unwrap();
    assert!(!effects.is_empty());
    for (shard, engine) in engines.iter_mut().enumerate() {
        assert_eq!(engine.database(), shard as u32 + 2);
        engine.switch_database(0);
        assert_eq!(engine.entries.len(), 2);
        assert!(engine.entries.contains_key(&key("shared", shard)));
        assert!(engine.entries.contains_key(&key("new", shard)));
        engine.switch_database(shard as u32 + 2);
    }
    {
        let state = catalog.lock();
        let name = state.resolve("view").unwrap();
        let index = &state.indexes[name];
        assert_eq!(index.docs.len(), 8);
        assert_eq!(
            super::query::execute(
                index,
                "after",
                &super::query::Options::default(),
                &state.config,
                1000
            )
            .unwrap()
            .hits
            .len(),
            8
        );
        assert!(
            super::query::execute(
                index,
                "before",
                &super::query::Options::default(),
                &state.config,
                1000
            )
            .unwrap()
            .hits
            .is_empty()
        );
    }
    assert!(
        Engine::search_reconcile_swapped(&mut engines, 0, 1, 1000)
            .unwrap()
            .is_empty()
    );
    assert!(
        Engine::search_reconcile_swapped(&mut engines, 1, 2, 1000)
            .unwrap()
            .is_empty()
    );
}

#[test]
fn a_shard_that_wrote_before_any_index_indexes_its_writes_once_one_exists() {
    use super::{Catalog, Engine};
    use std::sync::Arc;
    fn run(engine: &mut Engine, command: &[&str]) {
        match engine
            .execute_at_time(&mut crate::engine::BoundCommand::bind(&args(command)), 1000)
            .unwrap()
        {
            super::super::Execution::Reply(super::Resp::Error(message)) => panic!("{message}"),
            super::super::Execution::Reply(_) | super::super::Execution::Pushed { .. } => {}
            _ => panic!("unexpected blocking execution"),
        }
        // As a commit stages it.
        engine.finish_pending_search();
        engine.take_batch();
    }
    for journaled in [false, true] {
        let catalog = Arc::new(Catalog::new());
        let mut creator = Engine::new();
        let mut writer = Engine::new();
        for engine in [&mut creator, &mut writer] {
            engine.search = Some(catalog.clone());
            engine.set_recording(journaled);
        }
        run(&mut writer, &["HSET", "doc:early", "title", "before"]);
        run(
            &mut creator,
            &[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "title",
                "TEXT",
            ],
        );
        run(&mut writer, &["HSET", "doc:late", "title", "after"]);
        let state = catalog.lock();
        let index = &state.indexes["idx"];
        assert_eq!(index.docs.len(), 1, "journaled: {journaled}");
        let hits = |text: &str| {
            super::query::execute(
                index,
                text,
                &super::query::Options::default(),
                &state.config,
                1000,
            )
            .unwrap()
            .hits
            .len()
        };
        assert_eq!(hits("after"), 1, "journaled: {journaled}");
        assert_eq!(hits("before"), 0, "journaled: {journaled}");
    }
}
