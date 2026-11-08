//! Global search metadata and incrementally maintained document indexes.
//! Catalog records are independent from Redis keys and snapshot in pinned chunks.
#[cfg(test)]
mod catalog_tests;
mod document;
pub(super) mod expression;
pub(super) mod hybrid;
mod indexing;
mod isolation;
mod lifecycle;
mod memory;
pub(crate) mod migration;
mod model;
mod options;
pub(super) mod pipeline;
pub(super) mod query;
mod response;
mod routing;
mod schema;
pub(super) mod spatial;
mod state;
pub(super) mod text;
pub(super) mod value;
pub(super) mod vector;
pub(crate) use isolation::SearchRollback;
mod runtime;
use super::{CommandSpec, Engine, Mutation, Resp, error};
use bytes::Bytes;
pub(crate) use routing::SearchPlan;
pub(crate) use state::SearchSnapshot;
use state::gc;
use state::{Effect, State};
use std::{
    io,
    sync::{
        Mutex, MutexGuard,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(crate) struct Catalog {
    state: Mutex<State>,
    count: AtomicUsize,
    gc_running: std::sync::atomic::AtomicBool,
}
impl Catalog {
    pub(super) fn new() -> Self {
        Self {
            state: Mutex::new(State::default()),
            count: AtomicUsize::new(0),
            gc_running: std::sync::atomic::AtomicBool::new(false),
        }
    }
    #[inline]
    pub(super) fn has_indexes(&self) -> bool {
        self.count.load(Ordering::Acquire) != 0
    }
    pub(crate) fn has_due(&self, now: i64) -> bool {
        let state = self.lock();
        state.deadlines.first().is_some_and(|(d, _, _)| *d <= now)
            || state.temp_deadlines.first().is_some_and(|(d, _)| *d <= now)
            || state
                .cursor_deadlines
                .first()
                .is_some_and(|(d, _)| *d <= now)
            || (!self.gc_running() && state.gc_due(now))
    }
    fn lock(&self) -> MutexGuard<'_, State> {
        self.state.lock().unwrap_or_else(|e| e.into_inner())
    }
    fn update_count(&self, s: &State) {
        self.count
            .store(s.indexes.len() + s.dictionaries.len(), Ordering::Release)
    }
    pub(super) fn apply_effect(&self, effect: &[u8]) -> io::Result<()> {
        let mut s = self.lock();
        s.apply_bytes(Bytes::copy_from_slice(effect))?;
        self.update_count(&s);
        Ok(())
    }
    pub(super) fn snapshot(&self) -> SearchSnapshot {
        self.lock().snapshot()
    }
}
pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "ft.create",
        -5,
        &["write", "denyoom"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.alter",
        -5,
        &["write", "denyoom"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.dropindex",
        -2,
        &["write"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.search",
        -3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.aggregate",
        -3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.hybrid",
        -6,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.profile",
        -5,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler("ft.info", 2, &["readonly"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler("ft._list", 1, &["readonly"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler("ft.aliasadd", 3, &["write"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler("ft.aliasdel", 2, &["write"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler(
        "ft.aliasupdate",
        3,
        &["write"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.aliaslist",
        2,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler("ft.dictadd", -3, &["write"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler("ft.dictdel", -3, &["write"], (0, 0, 0), Engine::cmd_search),
    CommandSpec::handler(
        "ft.dictdump",
        2,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.synupdate",
        -4,
        &["write"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.syndump",
        2,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.spellcheck",
        -3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.tagvals",
        3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.explain",
        -3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.explaincli",
        -3,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler(
        "ft.cursor",
        -4,
        &["readonly"],
        (0, 0, 0),
        Engine::cmd_search,
    ),
    CommandSpec::handler("ft.config", -3, &["admin"], (0, 0, 0), Engine::cmd_search),
];
fn string(b: &[u8]) -> Result<&str, String> {
    std::str::from_utf8(b).map_err(|_| "Invalid UTF-8 argument".into())
}
fn failure(s: String) -> Resp {
    if s.starts_with("NOPERM ") {
        Resp::Error(s)
    } else {
        Resp::Error(format!("ERR {s}"))
    }
}
fn bulk(s: impl Into<Bytes>) -> Resp {
    Resp::Bulk(Some(s.into()))
}
fn text(s: impl AsRef<str>) -> Resp {
    bulk(Bytes::copy_from_slice(s.as_ref().as_bytes()))
}
impl Engine {
    pub(super) fn search_catalog(&mut self) -> std::sync::Arc<Catalog> {
        self.search
            .get_or_insert_with(|| std::sync::Arc::new(Catalog::new()))
            .clone()
    }
    pub(crate) fn search_apply_effect(&mut self, effect: &[u8]) -> io::Result<()> {
        self.search_catalog().apply_effect(effect)
    }
    pub(crate) fn search_snapshot(&self) -> SearchSnapshot {
        self.search
            .as_ref()
            .map(|c| c.snapshot())
            .unwrap_or_else(state::SearchSnapshot::empty)
    }
    fn search_record_effects(&mut self, effects: Vec<Bytes>, now: i64) {
        for effect in effects {
            self.record(now, || Mutation::Search { effect });
        }
    }
    fn cmd_search(&mut self, a: &[Bytes], now: i64) -> Resp {
        if self.database() != 0 {
            return error("ERR Cannot access search indexes on db != 0");
        }
        self.search_catalog();
        self.search_reap(now);
        let mut command_bytes = [0_u8; 32];
        let command_len = a[0].len().min(command_bytes.len());
        for (out, input) in command_bytes.iter_mut().zip(a[0].iter()) {
            *out = input.to_ascii_lowercase();
        }
        let command = &command_bytes[..command_len];
        let result = match command {
            b"ft.create" => self.search_create(a, now),
            b"ft.alter" => self.search_alter(a, now),
            b"ft.dropindex" => self.search_drop(a, now),
            b"ft.aliasadd" | b"ft.aliasdel" | b"ft.aliasupdate" | b"ft.aliaslist" => {
                self.search_alias(a, now)
            }
            b"ft.dictadd" | b"ft.dictdel" | b"ft.dictdump" => self.search_dictionary(a, now),
            b"ft.synupdate" | b"ft.syndump" => self.search_synonym(a, now),
            b"ft.config" => self.search_config(a),
            b"ft._list" => {
                let catalog = self.search_catalog();
                let s = catalog.lock();
                Ok(self.search_set(
                    s.indexes
                        .values()
                        .filter(|i| self.search_authorize(i).is_ok())
                        .map(|i| text(&i.schema.name))
                        .collect(),
                ))
            }
            b"ft.tagvals" => self.search_tagvals(a),
            b"ft.info" => self.search_info(a, now),
            b"ft.spellcheck" => self.search_spellcheck(a, now),
            b"ft.explain" | b"ft.explaincli" => self.search_explain(a, now),
            b"ft.cursor" => self.search_cursor(a, now),
            b"ft.search" => self.search_query(a, now),
            b"ft.aggregate" => self.search_aggregate(a, now),
            b"ft.hybrid" => self.search_hybrid(a, now),
            b"ft.profile" => self.search_profile(a, now),
            _ => Err("Unknown search command".into()),
        };
        let reply = match result {
            Ok(reply) => reply,
            Err(e) => return failure(e),
        };
        let offset = match command {
            b"ft.aliasadd" | b"ft.aliasupdate" | b"ft.cursor" => Some(2),
            b"ft.create" | b"ft.alter" | b"ft.dropindex" | b"ft.aliaslist" | b"ft.synupdate"
            | b"ft.syndump" | b"ft.tagvals" | b"ft.info" | b"ft.spellcheck" | b"ft.explain"
            | b"ft.explaincli" | b"ft.search" | b"ft.aggregate" | b"ft.hybrid" | b"ft.profile" => {
                Some(1)
            }
            _ => None,
        };
        if let Some(name) = offset
            .and_then(|offset| a.get(offset))
            .and_then(|b| std::str::from_utf8(b).ok())
        {
            let catalog = self.search_catalog();
            let mut s = catalog.lock();
            let mut effects = Vec::new();
            if let Ok(name) = s.resolve(name).map(str::to_owned) {
                s.save_index_metadata(&name);
                let index = s.indexes.get_mut(&name).expect("resolved index");
                index.uses = index.uses.saturating_add(1);
                let response_owned = matches!(
                    command,
                    b"ft.search"
                        | b"ft.aggregate"
                        | b"ft.hybrid"
                        | b"ft.profile"
                        | b"ft.explain"
                        | b"ft.explaincli"
                        | b"ft.cursor"
                );
                if !response_owned && let Some(ttl) = index.schema.temporary_ms {
                    let deadline = now.saturating_add(ttl);
                    if index.schema.deadline != Some(deadline) {
                        match s.effect(Effect::Touch {
                            index: name,
                            deadline,
                        }) {
                            Ok(effect) => effects.push(effect),
                            Err(e) => {
                                drop(s);
                                return self.fail_io(io::Error::other(e));
                            }
                        }
                    }
                }
            }
            drop(s);
            self.search_record_effects(effects, now);
        }
        reply
    }
    fn search_authorize(&self, index: &model::Index) -> Result<(), String> {
        if self.command_prefixes_readable(&index.schema.prefixes) {
            Ok(())
        } else {
            Err("NOPERM User does not have permissions to access the index".into())
        }
    }
    fn search_set(&self, values: Vec<Resp>) -> Resp {
        if matches!(self.command_version(), crate::protocol::Version::Resp3) {
            Resp::Set(values)
        } else {
            Resp::Array(Some(values))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn run(engine: &mut Engine, command: &[&str]) -> Resp {
        let args = command
            .iter()
            .map(|arg| Bytes::copy_from_slice(arg.as_bytes()))
            .collect::<Vec<_>>();
        let reply = match engine
            .execute_at_time(&mut crate::engine::BoundCommand::bind(&args), 1000)
            .unwrap()
        {
            super::super::Execution::Reply(reply)
            | super::super::Execution::Pushed { reply, .. } => reply,
            _ => panic!("unexpected blocking search command"),
        };
        if let Resp::Error(message) = &reply {
            panic!("command {command:?}: {message}");
        }
        reply
    }

    fn search(engine: &mut Engine, query: &str, expected: &[&str]) {
        let reply = run(
            engine,
            &[
                "FT.SEARCH",
                "idx",
                query,
                "NOCONTENT",
                "LIMIT",
                "0",
                "100",
                "DIALECT",
                "2",
            ],
        );
        let Resp::Array(Some(values)) = reply else {
            panic!("expected search array");
        };
        assert_eq!(values.len(), expected.len() + 1, "query {query}");
        match &values[0] {
            Resp::Unsigned(count) => assert_eq!(*count, expected.len() as u64, "query {query}"),
            Resp::Integer(count) => assert_eq!(*count, expected.len() as i64, "query {query}"),
            _ => panic!("missing search count"),
        }
        let mut keys = values[1..]
            .iter()
            .map(|value| match value {
                Resp::Bulk(Some(value)) => value.as_ref(),
                _ => panic!("expected document key"),
            })
            .collect::<Vec<_>>();
        keys.sort();
        let mut expected = expected
            .iter()
            .map(|key| key.as_bytes())
            .collect::<Vec<_>>();
        expected.sort();
        assert_eq!(keys, expected, "query {query}");
    }

    #[test]
    fn numeric_signed_zero_ranges_share_postings_through_updates_and_deletes() {
        let mut engine = Engine::new();
        run(&mut engine, &["HSET", "doc:negative", "n", "-0"]);
        run(
            &mut engine,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "n",
                "NUMERIC",
            ],
        );
        run(&mut engine, &["HSET", "doc:positive", "n", "0"]);
        run(&mut engine, &["HSET", "doc:one", "n", "1"]);
        for range in ["@n:[0 -0]", "@n:[-0 0]", "@n:[0 0]", "@n:[-0 -0]"] {
            search(&mut engine, range, &["doc:negative", "doc:positive"]);
        }
        for range in ["@n:[(0 -0]", "@n:[0 (-0]", "@n:[(-0 0]", "@n:[-0 (0]"] {
            search(&mut engine, range, &[]);
        }
        search(&mut engine, "@n:[(0 +inf]", &["doc:one"]);
        search(&mut engine, "@n:[-inf (-0]", &[]);
        run(&mut engine, &["HSET", "doc:negative", "n", "0"]);
        run(&mut engine, &["HSET", "doc:positive", "n", "-0"]);
        search(&mut engine, "@n:[0 -0]", &["doc:negative", "doc:positive"]);
        run(&mut engine, &["HSET", "doc:negative", "n", "2"]);
        search(&mut engine, "@n:[0 0]", &["doc:positive"]);
        run(&mut engine, &["DEL", "doc:positive"]);
        search(&mut engine, "@n:[-0 -0]", &[]);
    }

    #[test]
    fn empty_tag_postings_follow_initial_scan_mutations_and_rebuild() {
        let mut engine = Engine::new();
        run(&mut engine, &["HSET", "doc:empty", "tag", ""]);
        run(
            &mut engine,
            &[
                "FT.CREATE",
                "idx",
                "ON",
                "HASH",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "tag",
                "TAG",
                "INDEXEMPTY",
                "INDEXMISSING",
            ],
        );
        search(&mut engine, "@tag:{\"\"}", &["doc:empty"]);
        run(
            &mut engine,
            &["HSET", "doc:mixed", "tag", "red,,blue", "other", "retained"],
        );
        search(&mut engine, "@tag:{\"\"}", &["doc:empty", "doc:mixed"]);
        run(&mut engine, &["HSET", "doc:empty", "tag", "red"]);
        search(&mut engine, "@tag:{\"\"}", &["doc:mixed"]);
        run(&mut engine, &["HSET", "doc:empty", "tag", ""]);
        run(&mut engine, &["HDEL", "doc:mixed", "tag"]);
        search(&mut engine, "@tag:{\"\"}", &["doc:empty"]);
        search(&mut engine, "ismissing(@tag)", &["doc:mixed"]);
        run(&mut engine, &["FT.DROPINDEX", "idx"]);
        run(
            &mut engine,
            &[
                "FT.CREATE",
                "idx",
                "PREFIX",
                "1",
                "doc:",
                "SCHEMA",
                "tag",
                "TAG",
                "INDEXEMPTY",
            ],
        );
        search(&mut engine, "@tag:{\"\"}", &["doc:empty"]);
        run(&mut engine, &["DEL", "doc:empty"]);
        search(&mut engine, "@tag:{\"\"}", &[]);
    }
}
