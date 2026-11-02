use super::model::Token;
use rust_stemmers::{Algorithm, Stemmer};
use std::borrow::Borrow;
use std::collections::{BTreeMap, BTreeSet};
use std::ops::Bound;
use std::sync::{Arc, LazyLock};
use unicode_normalization::{UnicodeNormalization, char::is_combining_mark};

/// Search normalization is shared by indexing, query expansion and sortable strings.
pub fn normalize(input: &str) -> String {
    input
        .nfd()
        .filter(|c| !is_combining_mark(*c))
        .flat_map(char::to_lowercase)
        .collect()
}

pub fn language_supported(language: &str) -> bool {
    algorithm(language).is_some()
        || matches!(
            language.to_ascii_lowercase().as_str(),
            "chinese" | "malay" | "tagalog" | "none"
        )
}
fn algorithm(language: &str) -> Option<Algorithm> {
    Some(match language.to_ascii_lowercase().as_str() {
        "arabic" => Algorithm::Arabic,
        "danish" => Algorithm::Danish,
        "dutch" => Algorithm::Dutch,
        "english" => Algorithm::English,
        "finnish" => Algorithm::Finnish,
        "french" => Algorithm::French,
        "german" => Algorithm::German,
        "greek" => Algorithm::Greek,
        "hungarian" => Algorithm::Hungarian,
        "italian" => Algorithm::Italian,
        "norwegian" => Algorithm::Norwegian,
        "portuguese" => Algorithm::Portuguese,
        "romanian" => Algorithm::Romanian,
        "russian" => Algorithm::Russian,
        "spanish" => Algorithm::Spanish,
        "swedish" => Algorithm::Swedish,
        "tamil" => Algorithm::Tamil,
        "turkish" => Algorithm::Turkish,
        _ => return None,
    })
}
pub fn stem(term: &str, language: &str) -> String {
    match language.to_ascii_lowercase().as_str() {
        "malay" => stem_malay(term),
        "tagalog" => stem_tagalog(term),
        _ => match algorithm(language) {
            Some(a) => Stemmer::create(a).stem(term).into_owned(),
            None => term.to_owned(),
        },
    }
}

/// Surface and stem postings share positions; offsets are UTF-8 byte offsets into the source.
/// Stopwords consume positions, so removing them never manufactures an exact phrase.
pub fn tokenize(
    input: &str,
    language: &str,
    nostem: bool,
    stopwords: &BTreeSet<String>,
) -> Vec<Token> {
    tokenize_indexed(input, language, nostem, stopwords, 0)
}

/// Minimum stem length applies to indexed source words, not query expansion.
pub fn tokenize_indexed(
    input: &str,
    language: &str,
    nostem: bool,
    stopwords: &BTreeSet<String>,
    min_stem: usize,
) -> Vec<Token> {
    if language.eq_ignore_ascii_case("chinese") {
        return tokenize_chinese(input, stopwords);
    }
    let mut out = Vec::new();
    let mut term = String::new();
    let mut start = 0;
    let mut position = 0u32;
    let mut escaped = false;
    let flush =
        |out: &mut Vec<Token>, term: &mut String, start: usize, end: usize, position: &mut u32| {
            if term.is_empty() {
                return;
            }
            let value = normalize(term);
            term.clear();
            let pos = *position;
            *position = position.saturating_add(1);
            if stopwords.contains(&value) {
                return;
            }
            if !nostem && value.chars().count() >= min_stem {
                let root = stem(&value, language);
                if root != value {
                    out.push(Token {
                        term: format!("+{root}"),
                        position: pos,
                        start,
                        end,
                    });
                }
            }
            out.push(Token {
                term: value,
                position: pos,
                start,
                end,
            });
        };
    for (offset, ch) in input.char_indices() {
        if escaped {
            term.push(ch);
            escaped = false;
            continue;
        }
        if ch == '\\' {
            if term.is_empty() {
                start = offset;
            }
            escaped = true;
            continue;
        }
        if ch.is_alphanumeric() || ch == '_' || is_combining_mark(ch) {
            if term.is_empty() {
                start = offset;
            }
            term.push(ch);
        } else {
            flush(&mut out, &mut term, start, offset, &mut position);
        }
    }
    if escaped {
        term.push('\\');
    }
    flush(&mut out, &mut term, start, input.len(), &mut position);
    out
}

fn tokenize_chinese(input: &str, stopwords: &BTreeSet<String>) -> Vec<Token> {
    static DICTIONARY: LazyLock<jieba_rs::Jieba> = LazyLock::new(jieba_rs::Jieba::new);
    let no_stops = BTreeSet::new();
    let mut out = Vec::new();
    let mut position = 0u32;
    for word in DICTIONARY.cut(input, true) {
        let offset = word.as_ptr() as usize - input.as_ptr() as usize;
        for mut token in tokenize(word, "none", true, &no_stops) {
            token.position = position;
            position = position.saturating_add(1);
            token.start += offset;
            token.end += offset;
            if !stopwords.contains(&token.term) {
                out.push(token);
            }
        }
    }
    out
}

/// Double-Metaphone phonetic keys. Primary and alternate readings are indexed separately.
pub fn phonetic(term: &str, _mode: &str) -> Vec<String> {
    use rphonetic::{DoubleMetaphone, Encoder};
    let encoder = DoubleMetaphone::new(Some(8));
    let term = normalize(term);
    let primary = encoder.encode(&term);
    let alternate = encoder.encode_alternate(&term);
    let mut out = Vec::with_capacity(2);
    let distinct = !alternate.is_empty() && alternate != primary;
    if !primary.is_empty() {
        out.push(primary);
    }
    if distinct {
        out.push(alternate);
    }
    out
}

fn strip_suffix<'a>(word: &'a str, suffixes: &[&str], minimum: usize) -> &'a str {
    for suffix in suffixes {
        if let Some(root) = word.strip_suffix(suffix)
            && root.chars().count() >= minimum
        {
            return root;
        }
    }
    word
}
fn stem_malay(term: &str) -> String {
    if !term.is_ascii() {
        return term.to_owned();
    }
    let word = strip_suffix(term, &["lah", "kah", "tah", "pun"], 3);
    let word = strip_suffix(word, &["nya", "ku", "mu"], 3);
    let mut root = word.to_owned();
    let vowel = |c: u8| matches!(c, b'a' | b'e' | b'i' | b'o' | b'u');
    for (prefix, restore) in [
        ("menge", ""),
        ("penge", ""),
        ("meny", "s"),
        ("peny", "s"),
        ("meng", "k"),
        ("peng", "k"),
        ("mem", "p"),
        ("pem", "p"),
        ("men", "t"),
        ("pen", "t"),
    ] {
        if let Some(tail) = word.strip_prefix(prefix)
            && tail.len() >= 3
        {
            root = if !restore.is_empty() && tail.as_bytes().first().is_some_and(|c| vowel(*c)) {
                format!("{restore}{tail}")
            } else {
                tail.to_owned()
            };
            break;
        }
    }
    if root == word {
        for prefix in [
            "memper", "diper", "ber", "bel", "per", "pel", "ter", "di", "ke", "se", "me", "pe",
        ] {
            if let Some(tail) = word.strip_prefix(prefix)
                && tail.len() >= 3
            {
                root = tail.to_owned();
                break;
            }
        }
    }
    strip_suffix(&root, &["kan", "an", "i"], 3).to_owned()
}
fn stem_tagalog(term: &str) -> String {
    if !term.is_ascii() {
        return term.to_owned();
    }
    let mut root = term;
    for prefix in [
        "nakikipag",
        "pakikipag",
        "makikipag",
        "mangang",
        "tagapag",
        "nakapag",
        "makapag",
        "nakipag",
        "pakipag",
        "ipinag",
        "ipang",
        "magpa",
        "pagka",
        "pinag",
        "maka",
        "naka",
        "mag",
        "nag",
        "pag",
        "ma",
        "pa",
        "i",
    ] {
        if let Some(tail) = root.strip_prefix(prefix)
            && tail.len() >= 3
        {
            root = tail;
            break;
        }
    }
    root = strip_suffix(root, &["han", "hin", "an", "in"], 3);
    let mut root = root.to_owned();
    if root.len() >= 5 && matches!(&root[1..3], "um" | "in") {
        root.replace_range(1..3, "");
    }
    if root.len() >= 5 {
        let b = root.as_bytes();
        if b[0] == b[2] && matches!(b[1], b'a' | b'e' | b'i' | b'o' | b'u') {
            root.drain(..2);
        }
    }
    root
}

/// A dynamic ordered suffix dictionary. Suffix keys are views into shared terms,
/// so a vocabulary term requires linear storage, not copied quadratic substrings.
/// Exact suffix lookup is logarithmic; infix lookup visits only matching suffixes.
#[derive(Default)]
pub struct SuffixIndex {
    suffixes: BTreeMap<SuffixKey, BTreeSet<Arc<str>>>,
}
struct SuffixKey {
    term: Arc<str>,
    start: usize,
}
impl Borrow<str> for SuffixKey {
    fn borrow(&self) -> &str {
        &self.term[self.start..]
    }
}
impl PartialEq for SuffixKey {
    fn eq(&self, other: &Self) -> bool {
        self.borrowed() == other.borrowed()
    }
}
impl Eq for SuffixKey {}
impl PartialOrd for SuffixKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for SuffixKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.borrowed().cmp(other.borrowed())
    }
}
impl SuffixKey {
    fn borrowed(&self) -> &str {
        &self.term[self.start..]
    }
}
impl SuffixIndex {
    pub fn insert(&mut self, term: &str) {
        let term: Arc<str> = Arc::from(term);
        for (start, _) in term.char_indices() {
            self.suffixes
                .entry(SuffixKey {
                    term: Arc::clone(&term),
                    start,
                })
                .or_default()
                .insert(Arc::clone(&term));
        }
    }
    pub fn remove(&mut self, term: &str) {
        for (start, _) in term.char_indices() {
            let suffix = &term[start..];
            if let Some((_, mut terms)) = self.suffixes.remove_entry(suffix) {
                terms.remove(term);
                if let Some(owner) = terms.first() {
                    let start = owner.len() - suffix.len();
                    self.suffixes.insert(
                        SuffixKey {
                            term: Arc::clone(owner),
                            start,
                        },
                        terms,
                    );
                }
            }
        }
    }
    pub fn suffix<'a>(&'a self, needle: &str) -> impl Iterator<Item = &'a str> {
        self.suffixes
            .get(needle)
            .into_iter()
            .flat_map(|terms| terms.iter().map(Arc::as_ref))
    }
    pub fn infix<'a>(&'a self, needle: &'a str) -> impl Iterator<Item = &'a str> + 'a {
        self.suffixes
            .range::<str, _>((Bound::Included(needle), Bound::Unbounded))
            .take_while(move |(key, _)| key.borrowed().starts_with(needle))
            .flat_map(|(_, terms)| terms.iter().map(Arc::as_ref))
    }
    #[cfg(test)]
    pub fn terms(&self, needle: &str, suffix_only: bool, max: usize) -> Vec<String> {
        if max == 0 {
            return Vec::new();
        }
        if suffix_only {
            return self.suffix(needle).take(max).map(str::to_owned).collect();
        }
        let mut terms = BTreeSet::new();
        for term in self.infix(needle) {
            terms.insert(term);
            if terms.len() >= max {
                break;
            }
        }
        terms.into_iter().map(str::to_owned).collect()
    }
    /// Estimated owned storage, with shared string allocations counted once.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        let overhead = size_of::<usize>().saturating_mul(3);
        let mut seen = BTreeSet::new();
        let mut allocation = |term: &Arc<str>| -> usize {
            let pointer = Arc::as_ptr(term) as *const u8 as usize;
            if seen.insert(pointer) {
                term.len()
                    .saturating_add(size_of::<usize>().saturating_mul(2))
            } else {
                0
            }
        };
        let mut bytes = size_of::<Self>();
        for (key, terms) in &self.suffixes {
            bytes = bytes.saturating_add(
                size_of::<(SuffixKey, BTreeSet<Arc<str>>)>().saturating_add(overhead),
            );
            bytes = bytes.saturating_add(allocation(&key.term));
            bytes = bytes.saturating_add(
                terms
                    .len()
                    .saturating_mul(size_of::<Arc<str>>().saturating_add(overhead)),
            );
            for term in terms {
                bytes = bytes.saturating_add(allocation(term));
            }
        }
        bytes
    }
}
