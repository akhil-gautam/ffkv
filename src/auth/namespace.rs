//! Exact inclusion of `literal_prefix · byte*` in a selector's glob union.
//!
//! Each glob is a linear epsilon NFA: `*` loops and skips, other atoms consume
//! one byte. Subset construction searches for a reachable nonaccepting state
//! after consuming the fixed prefix. Exploring only byte-equivalence classes
//! avoids building a 256-way transition table for every pattern or subset.
use super::{Atom, KeyAccess, MAX_GLOB_WORK, Selector, User, charge};
use bytes::Bytes;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

const MAX_NFA_STATES: usize = 16 * 1024;
const MAX_SUBSETS: usize = 4096;
const MAX_SUBSET_ITEMS: usize = 1024 * 1024;
const MAX_CACHE_ENTRIES: usize = 256;
const MAX_CACHE_BYTES: usize = 1024 * 1024;
const MAX_PREFIX_BYTES: usize = 64 * 1024;
const COMPLEXITY: &str = "namespace key pattern complexity limit exceeded";
type Proof = Result<bool, ()>;
type ProofKey = (usize, bool, Bytes);
type Mask = [u64; 4];

#[derive(Default)]
pub(super) struct Cache {
    automata: HashMap<(usize, bool), Result<Arc<Automaton>, ()>>,
    proofs: HashMap<ProofKey, Proof>,
    order: VecDeque<ProofKey>,
    prefix_bytes: usize,
}

pub(super) fn authorize(
    user: &User,
    command: &[u8],
    categories: &[&str],
    prefixes: &[Bytes],
    access: KeyAccess,
    keys: &[(&Bytes, KeyAccess)],
    audit: Option<&super::Audit<'_>>,
) -> Result<(), String> {
    let reject = |reason: &str, object: &[u8], message: &str| {
        if let Some(audit) = audit {
            audit.record(user, reason, object);
        }
        Err(message.to_owned())
    };
    let mut work = MAX_GLOB_WORK;
    let mut command_granted = false;
    let mut complexity = false;
    let mut denied_resource: Option<&Bytes> = None;
    let mut cache = user
        .namespace_cache
        .lock()
        .unwrap_or_else(|error| error.into_inner());
    for (index, selector) in std::iter::once(&user.root)
        .chain(&user.selectors)
        .enumerate()
    {
        if charge(&mut work, 1).is_err() {
            return reject("key", b"*", COMPLEXITY);
        }
        if !selector.command_allowed(command, categories) {
            continue;
        }
        command_granted = true;
        let mut allowed = true;
        for prefix in prefixes {
            if prefix.len() > MAX_PREFIX_BYTES {
                return reject("key", prefix, COMPLEXITY);
            }
            for read in [true, false] {
                if (read && access == KeyAccess::Write) || (!read && access == KeyAccess::Read) {
                    continue;
                }
                match cache.covers(index, selector, prefix, read, &mut work) {
                    Ok(true) => {}
                    Ok(false) => {
                        denied_resource = Some(prefix);
                        allowed = false;
                        break;
                    }
                    Err(()) => {
                        denied_resource = Some(prefix);
                        complexity = true;
                        allowed = false;
                        break;
                    }
                }
            }
            if !allowed {
                break;
            }
        }
        if !allowed {
            continue;
        }
        for &(key, mode) in keys {
            if charge(&mut work, 1).is_err() {
                return reject("key", key, COMPLEXITY);
            }
            match selector.key_allowed(key, mode, &mut work) {
                Ok(true) => {}
                Ok(false) => {
                    denied_resource = Some(key);
                    allowed = false;
                    break;
                }
                Err(()) => {
                    denied_resource = Some(key);
                    complexity = true;
                    allowed = false;
                    break;
                }
            }
        }
        if allowed {
            return Ok(());
        }
    }
    drop(cache);
    if !command_granted {
        reject("command", command, "command is not permitted")
    } else {
        reject(
            "key",
            denied_resource.map_or(b"*".as_slice(), Bytes::as_ref),
            if complexity {
                COMPLEXITY
            } else {
                "key access is not permitted"
            },
        )
    }
}

impl Cache {
    fn covers(
        &mut self,
        index: usize,
        selector: &Selector,
        prefix: &Bytes,
        read: bool,
        work: &mut usize,
    ) -> Proof {
        charge(work, prefix.len() + 1)?;
        let key = (index, read, prefix.clone());
        if let Some(result) = self.proofs.get(&key) {
            return *result;
        }
        // A proof's own budget is independent of preceding prefixes/selectors:
        // caching complexity failures must not make policy depend on query order.
        // The request budget charges actual work afterwards. A final miss can
        // exceed its remaining allowance by at most one fixed proof budget.
        let mut proof_work = MAX_GLOB_WORK;
        let result = self.prove(index, selector, prefix, read, &mut proof_work);
        while self.proofs.len() >= MAX_CACHE_ENTRIES
            || self.prefix_bytes + prefix.len() > MAX_CACHE_BYTES
        {
            let Some(old) = self.order.pop_front() else {
                break;
            };
            self.prefix_bytes -= old.2.len();
            self.proofs.remove(&old);
        }
        // Do not retain a tiny Bytes view's potentially enormous input buffer.
        let key = (index, read, Bytes::copy_from_slice(prefix));
        self.prefix_bytes += prefix.len();
        self.order.push_back(key.clone());
        self.proofs.insert(key, result);
        if charge(work, MAX_GLOB_WORK - proof_work).is_err() {
            *work = 0;
            return Err(());
        }
        result
    }

    fn prove(
        &mut self,
        index: usize,
        selector: &Selector,
        prefix: &Bytes,
        read: bool,
        work: &mut usize,
    ) -> Proof {
        // Literal-prefix stars are overwhelmingly common. This is a language
        // proof, not a match against a synthetic key ending in an asterisk.
        for pattern in &selector.key_patterns {
            if !compatible(pattern.access, read) {
                continue;
            }
            charge(work, pattern.glob.atoms.len() + 1)?;
            if let Some((Atom::Many, leading)) = pattern.glob.atoms.split_last()
                && leading.len() <= prefix.len()
                && leading
                    .iter()
                    .zip(prefix.iter())
                    .all(|(atom, byte)| matches!(atom, Atom::Literal(value) if value == byte))
            {
                return Ok(true);
            }
        }
        let automaton = self
            .automata
            .entry((index, read))
            .or_insert_with(|| Automaton::compile(selector, read, work).map(Arc::new));
        match automaton {
            Ok(automaton) => automaton.covers(prefix, work),
            Err(()) => Err(()),
        }
    }
}

fn compatible(access: KeyAccess, read: bool) -> bool {
    if read {
        access != KeyAccess::Write
    } else {
        access != KeyAccess::Read
    }
}

struct Node {
    mask: Mask,
    star: bool,
    accept: bool,
    // An active final star accepts every possible remaining suffix.
    universal: bool,
}

struct Automaton {
    nodes: Vec<Node>,
    starts: Vec<u32>,
    alphabet: Vec<u8>,
}

impl Automaton {
    fn compile(selector: &Selector, read: bool, work: &mut usize) -> Result<Self, ()> {
        let size = selector
            .key_patterns
            .iter()
            .filter(|pattern| compatible(pattern.access, read))
            .try_fold(0usize, |size, pattern| {
                size.checked_add(pattern.glob.atoms.len() + 1)
            })
            .ok_or(())?;
        if size > MAX_NFA_STATES {
            return Err(());
        }
        charge(work, size)?;
        let mut nodes = Vec::with_capacity(size);
        let mut starts = Vec::new();
        let mut partitions = vec![[u64::MAX; 4]];
        for pattern in &selector.key_patterns {
            if !compatible(pattern.access, read) {
                continue;
            }
            starts.push(nodes.len() as u32);
            let trailing_stars = pattern
                .glob
                .atoms
                .iter()
                .rposition(|atom| !matches!(atom, Atom::Many))
                .map_or(0, |index| index + 1);
            for (index, atom) in pattern.glob.atoms.iter().enumerate() {
                let mask = atom_mask(atom, work)?;
                // Split the alphabet by each predicate. At most 256 disjoint
                // masks exist; there are no per-pattern 256-element tables.
                let count = partitions.len();
                for position in 0..count {
                    charge(work, 4)?;
                    let mut inside = [0; 4];
                    let mut outside = [0; 4];
                    for word in 0..4 {
                        inside[word] = partitions[position][word] & mask[word];
                        outside[word] = partitions[position][word] & !mask[word];
                    }
                    if inside != [0; 4] && outside != [0; 4] {
                        partitions[position] = inside;
                        partitions.push(outside);
                    }
                }
                let star = matches!(atom, Atom::Many);
                nodes.push(Node {
                    mask,
                    star,
                    accept: false,
                    universal: star && index >= trailing_stars,
                });
            }
            nodes.push(Node {
                mask: [0; 4],
                star: false,
                accept: true,
                universal: false,
            });
        }
        let alphabet = partitions
            .into_iter()
            .map(|mask| {
                let word = mask
                    .iter()
                    .position(|bits| *bits != 0)
                    .expect("nonempty alphabet partition");
                (word * 64 + mask[word].trailing_zeros() as usize) as u8
            })
            .collect();
        Ok(Self {
            nodes,
            starts,
            alphabet,
        })
    }

    fn closure(&self, states: &mut Vec<u32>, work: &mut usize) -> Result<(), ()> {
        let initial = states.len();
        // Inputs are ordered (starts and transition destinations both are).
        // Skip chains already expanded by an earlier input, keeping scratch
        // storage below twice the NFA size even for runs of adjacent stars.
        let mut covered = None;
        for position in 0..initial {
            let mut state = states[position] as usize;
            if covered.is_some_and(|end| state <= end) {
                continue;
            }
            while self.nodes[state].star {
                charge(work, 1)?;
                state += 1;
                states.push(state as u32);
            }
            covered = Some(state);
        }
        charge(
            work,
            states
                .len()
                .saturating_mul(states.len().max(1).ilog2() as usize + 1),
        )?;
        states.sort_unstable();
        states.dedup();
        Ok(())
    }

    fn step(
        &self,
        states: &[u32],
        byte: u8,
        next: &mut Vec<u32>,
        work: &mut usize,
    ) -> Result<(), ()> {
        next.clear();
        charge(work, states.len() + 1)?;
        for &state in states {
            let node = &self.nodes[state as usize];
            if node.mask[byte as usize / 64] & (1u64 << (byte % 64)) != 0 {
                next.push(state + u32::from(!node.star));
            }
        }
        self.closure(next, work)
    }

    fn covers(&self, prefix: &[u8], work: &mut usize) -> Proof {
        let mut state = self.starts.clone();
        let mut next = Vec::new();
        self.closure(&mut state, work)?;
        for &byte in prefix {
            self.step(&state, byte, &mut next, work)?;
            std::mem::swap(&mut state, &mut next);
            if state.is_empty() {
                return Ok(false);
            }
        }
        let mut stored_items = state.len();
        let mut seen = HashSet::new();
        let mut queue = VecDeque::new();
        // Arc shares each subset between the visited set and queue; transitions
        // reuse one scratch buffer rather than allocating for all 256 bytes.
        let state: Arc<[u32]> = state.into();
        seen.insert(state.clone());
        queue.push_back(state);
        while let Some(state) = queue.pop_front() {
            charge(work, state.len() + 1)?;
            if !state
                .iter()
                .any(|&position| self.nodes[position as usize].accept)
            {
                return Ok(false);
            }
            if state
                .iter()
                .any(|&position| self.nodes[position as usize].universal)
            {
                continue;
            }
            for &byte in &self.alphabet {
                self.step(&state, byte, &mut next, work)?;
                if seen.contains(next.as_slice()) {
                    continue;
                }
                stored_items = stored_items.checked_add(next.len()).ok_or(())?;
                if seen.len() >= MAX_SUBSETS || stored_items > MAX_SUBSET_ITEMS {
                    return Err(());
                }
                let successor: Arc<[u32]> = next.as_slice().into();
                seen.insert(successor.clone());
                queue.push_back(successor);
            }
        }
        Ok(true)
    }
}

fn atom_mask(atom: &Atom, work: &mut usize) -> Result<Mask, ()> {
    let mut mask = [0; 4];
    match atom {
        Atom::Many | Atom::Any => return Ok([u64::MAX; 4]),
        Atom::Literal(byte) => mask[*byte as usize / 64] |= 1u64 << (byte % 64),
        Atom::Class { negated, ranges } => {
            for &(start, end) in ranges {
                charge(work, usize::from(end) - usize::from(start) + 1)?;
                for byte in start..=end {
                    mask[byte as usize / 64] |= 1u64 << (byte % 64);
                }
            }
            if *negated {
                for word in &mut mask {
                    *word = !*word;
                }
            }
        }
    }
    Ok(mask)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn user(rules: &[&[u8]]) -> User {
        let mut user = User::empty("namespace");
        user.apply_rules(rules).unwrap();
        user
    }

    fn check(user: &User, prefixes: &[&[u8]], access: KeyAccess) -> Result<(), String> {
        let prefixes: Vec<_> = prefixes
            .iter()
            .map(|prefix| Bytes::copy_from_slice(prefix))
            .collect();
        user.authorize_prefixes_and_keys_logged(
            b"FT.SEARCH",
            &["read", "search"],
            &prefixes,
            access,
            &[],
            None,
        )
    }

    #[test]
    fn a_matching_probe_is_not_a_namespace_proof() {
        let user = user(&[b"+ft.search", b"~doc:?"]);
        assert!(check(&user, &[b"doc:"], KeyAccess::Read).is_err());
        assert!(check(&user, &[b"doc:*"], KeyAccess::Read).is_err());
    }

    #[test]
    fn command_and_all_namespaces_must_share_one_selector() {
        let user = user(&[b"+ft.search", b"(~doc:*)"]);
        assert!(check(&user, &[b"doc:"], KeyAccess::Read).is_err());
        let user = self::user(&[b"(+ft.search ~doc:*)", b"(+ft.search ~other:*)"]);
        assert!(check(&user, &[b"doc:", b"other:"], KeyAccess::Read).is_err());
        assert!(check(&user, &[b"doc:"], KeyAccess::Read).is_ok());
        assert!(
            user.authorize_prefixes_and_keys_logged(
                b"FT.INFO",
                &["read"],
                &[],
                KeyAccess::Read,
                &[],
                None
            )
            .is_err()
        );
    }

    #[test]
    fn namespaces_and_concrete_key_access_cannot_mix_selectors() {
        let prefixes = [Bytes::from_static(b"doc:")];
        let document = Bytes::from_static(b"doc:one");
        let other = Bytes::from_static(b"doc:two");
        let keys = [(&document, KeyAccess::Write)];
        let split = user(&[b"(+ft.dropindex %R~doc:*)", b"(+ft.dropindex %W~doc:one)"]);
        assert!(
            split
                .authorize_prefixes_and_keys_logged(
                    b"FT.DROPINDEX",
                    &["search", "write"],
                    &prefixes,
                    KeyAccess::Read,
                    &keys,
                    None,
                )
                .is_err()
        );
        let combined = user(&[b"(+ft.dropindex %R~doc:* %W~doc:one)"]);
        assert!(
            combined
                .authorize_prefixes_and_keys_logged(
                    b"FT.DROPINDEX",
                    &["search", "write"],
                    &prefixes,
                    KeyAccess::Read,
                    &keys,
                    None,
                )
                .is_ok()
        );
        assert!(
            combined
                .authorize_prefixes_and_keys_logged(
                    b"FT.DROPINDEX",
                    &["search", "write"],
                    &prefixes,
                    KeyAccess::Read,
                    &[(&document, KeyAccess::Write), (&other, KeyAccess::Write)],
                    None,
                )
                .is_err()
        );
        assert!(
            combined
                .authorize_prefixes_and_keys_logged(
                    b"FT.DROPINDEX",
                    &["search", "write"],
                    &prefixes,
                    KeyAccess::Read,
                    &[(&document, KeyAccess::ReadWrite)],
                    None,
                )
                .is_ok()
        );
    }

    #[test]
    fn classes_and_union_cover_all_binary_suffixes_including_empty() {
        let user = user(&[b"+ft.search", b"~doc:", b"~doc:[a-m]*", b"~doc:[^a-m]*"]);
        assert!(check(&user, &[b"doc:"], KeyAccess::ReadWrite).is_ok());
        let no_empty = self::user(&[b"+ft.search", b"~doc:[a-m]*", b"~doc:[^a-m]*"]);
        assert!(check(&no_empty, &[b"doc:"], KeyAccess::Read).is_err());
        let gap = self::user(&[b"+ft.search", b"~doc:", b"~doc:[\x00-\xfe]*"]);
        assert!(check(&gap, &[b"doc:"], KeyAccess::Read).is_err());
        let all = self::user(&[b"+ft.search", b"~", b"~[\x00-\x7f]*", b"~[\x80-\xff]*"]);
        assert!(check(&all, &[b""], KeyAccess::ReadWrite).is_ok());
    }

    #[test]
    fn union_inclusion_handles_nonterminal_star_loops_and_epsilon_chains() {
        let user = user(&[b"+ft.search", b"~", b"~**a", b"~***[^a]"]);
        assert!(check(&user, &[b""], KeyAccess::ReadWrite).is_ok());
        assert!(check(&user, &[b"\x00\xffa"], KeyAccess::Read).is_ok());
        let gap = self::user(&[b"+ft.search", b"~", b"~**a"]);
        assert!(check(&gap, &[b"a"], KeyAccess::Read).is_err());
    }

    #[test]
    fn compiled_glob_semantics_preserve_escapes_case_and_utf8_bytes() {
        let user = user(&[
            b"+ft.search",
            b"~Doc:\\*\\?\\[\\]\\\\*",
            b"~\xc4\x80:[z-a]*",
            b"~\xc4\x80:",
        ]);
        assert!(check(&user, &[b"Doc:*?[]\\\xff"], KeyAccess::Read).is_ok());
        assert!(check(&user, &[b"doc:*?[]\\"], KeyAccess::Read).is_err());
        assert!(check(&user, &[b"\xc4\x80:b"], KeyAccess::Read).is_ok());
        assert!(check(&user, &[b"\x00:b"], KeyAccess::Read).is_err());
        let escaped_class = self::user(&[b"+ft.search", b"~[\\-\\]]*"]);
        assert!(check(&escaped_class, &[b"]"], KeyAccess::Read).is_ok());
        assert!(check(&escaped_class, &[b"-"], KeyAccess::Read).is_ok());
    }

    #[test]
    fn read_write_requires_both_language_covers_not_one_shared_pattern() {
        let user = user(&[
            b"+ft.search",
            b"%R~doc:*",
            b"%W~doc:",
            b"%W~doc:[a-m]*",
            b"%W~doc:[^a-m]*",
        ]);
        assert!(check(&user, &[b"doc:"], KeyAccess::ReadWrite).is_ok());
        let incomplete = self::user(&[b"+ft.search", b"%R~doc:*", b"%W~doc:?"]);
        assert!(check(&incomplete, &[b"doc:"], KeyAccess::Read).is_ok());
        assert!(check(&incomplete, &[b"doc:"], KeyAccess::Write).is_err());
        assert!(check(&incomplete, &[b"doc:"], KeyAccess::ReadWrite).is_err());
    }

    #[test]
    fn adversarial_state_budget_fails_closed_and_caches_the_error() {
        let mut rules = vec![b"+ft.search".to_vec()];
        for index in 0..20 {
            let mut pattern = vec![b'~', b'a' + index];
            pattern.extend(std::iter::repeat_n(b'?', 1000));
            pattern.push(b'*');
            rules.push(pattern);
        }
        let refs: Vec<_> = rules.iter().map(Vec::as_slice).collect();
        let user = user(&refs);
        for _ in 0..2 {
            assert_eq!(
                check(&user, &[b""], KeyAccess::Read),
                Err(COMPLEXITY.into())
            );
        }
        let cache = user.namespace_cache.lock().unwrap();
        assert_eq!(cache.automata.len(), 1);
        assert_eq!(cache.proofs.len(), 1);
        assert_eq!(cache.proofs.values().next(), Some(&Err(())));
    }

    #[test]
    fn immutable_snapshots_cache_proofs_but_mutation_discards_them() {
        let original = user(&[b"+ft.search", b"~doc:", b"~doc:[a-m]*", b"~doc:[^a-m]*"]);
        assert!(check(&original, &[b"doc:"], KeyAccess::Read).is_ok());
        // A cached proof works with no remaining proof-work budget.
        let mut cache = original.namespace_cache.lock().unwrap();
        assert_eq!(
            cache.covers(
                0,
                &original.root,
                &Bytes::from_static(b"doc:"),
                true,
                &mut 5
            ),
            Ok(true)
        );
        drop(cache);
        let mut changed = original.clone();
        changed.apply_rules(&[b"resetkeys", b"~doc:?"]).unwrap();
        assert!(check(&changed, &[b"doc:"], KeyAccess::Read).is_err());
        assert!(check(&original, &[b"doc:"], KeyAccess::Read).is_ok());
    }

    #[test]
    fn work_and_prefix_budgets_are_explicit_and_cache_storage_is_bounded() {
        let user = user(&[b"+ft.search", b"~", b"~[a-m]*", b"~[^a-m]*"]);
        let mut work = 1;
        assert!(Automaton::compile(&user.root, true, &mut work).is_err());
        let prefix = vec![b'a'; MAX_PREFIX_BYTES + 1];
        assert_eq!(
            check(&user, &[&prefix], KeyAccess::Read),
            Err(COMPLEXITY.into())
        );
        for index in 0..MAX_CACHE_ENTRIES + 1 {
            assert!(check(&user, &[&index.to_be_bytes()], KeyAccess::Read).is_ok());
        }
        let cache = user.namespace_cache.lock().unwrap();
        assert!(cache.proofs.len() <= MAX_CACHE_ENTRIES);
        assert!(cache.prefix_bytes <= MAX_CACHE_BYTES);
    }
}
