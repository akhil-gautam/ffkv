//! Geospatial indexes are ordinary sorted sets with interleaved coordinate scores.
//! Wire/coordinate reference: Redis 7.2 geohash format (BSD); distance is haversine.
use super::sorted_sets::parse_float;
use super::{
    CommandSpec, Engine, Mutation, Resp, Value, array, bounded_replies, error, integer,
    integer_error, syntax_error,
};
use bytes::Bytes;

const LATITUDE: f64 = 85.05112878;
const EARTH: f64 = 6_372_797.560856;
const CELLS: f64 = (1u64 << 26) as f64;

pub(super) static COMMANDS: &[CommandSpec] = &[
    CommandSpec::handler(
        "geoadd",
        -5,
        &["write", "denyoom"],
        (1, 1, 1),
        Engine::geoadd,
    ),
    CommandSpec::handler("geodist", -4, &["readonly"], (1, 1, 1), Engine::geodist).max(5),
    CommandSpec::handler("geohash", -3, &["readonly"], (1, 1, 1), Engine::geoposition),
    CommandSpec::handler("geopos", -3, &["readonly"], (1, 1, 1), Engine::geoposition),
    CommandSpec::handler("geosearch", -7, &["readonly"], (1, 1, 1), Engine::geosearch),
    CommandSpec::handler(
        "geosearchstore",
        -8,
        &["write", "denyoom"],
        (1, 2, 1),
        Engine::geosearch,
    ),
    CommandSpec::handler(
        "georadius",
        -6,
        &["write", "denyoom", "movablekeys"],
        (1, 1, 1),
        Engine::geosearch,
    ),
    CommandSpec::handler(
        "georadiusbymember",
        -5,
        &["write", "denyoom", "movablekeys"],
        (1, 1, 1),
        Engine::geosearch,
    ),
    CommandSpec::handler(
        "georadius_ro",
        -6,
        &["readonly"],
        (1, 1, 1),
        Engine::geosearch,
    ),
    CommandSpec::handler(
        "georadiusbymember_ro",
        -5,
        &["readonly"],
        (1, 1, 1),
        Engine::geosearch,
    ),
];

fn number(raw: &[u8]) -> Result<f64, Resp> {
    parse_float(raw).ok_or_else(|| error("ERR value is not a valid float"))
}
fn coordinates(lon: &[u8], lat: &[u8]) -> Result<(f64, f64), Resp> {
    let (lon, lat) = (number(lon)?, number(lat)?);
    if !(-180.0..=180.0).contains(&lon) || !(-LATITUDE..=LATITUDE).contains(&lat) {
        return Err(Resp::Error(format!(
            "ERR invalid longitude,latitude pair {lon:.6},{lat:.6}"
        )));
    }
    Ok((lon, lat))
}
fn unit(raw: &[u8]) -> Result<f64, Resp> {
    if raw.eq_ignore_ascii_case(b"m") {
        Ok(1.0)
    } else if raw.eq_ignore_ascii_case(b"km") {
        Ok(1000.0)
    } else if raw.eq_ignore_ascii_case(b"ft") {
        Ok(0.3048)
    } else if raw.eq_ignore_ascii_case(b"mi") {
        Ok(1609.34)
    } else {
        Err(error(
            "ERR unsupported unit provided. please use M, KM, FT, MI",
        ))
    }
}
fn nonnegative(raw: &[u8]) -> Result<f64, Resp> {
    let value = number(raw).map_err(|_| error("ERR need numeric radius"))?;
    if value < 0.0 {
        return Err(error("ERR radius cannot be negative"));
    }
    Ok(value)
}
fn interleave(lat: u64, lon: u64) -> u64 {
    let mut bits = 0;
    for i in 0..32 {
        bits |= ((lat >> i) & 1) << (i * 2);
        bits |= ((lon >> i) & 1) << (i * 2 + 1);
    }
    bits
}
fn encode(lon: f64, lat: f64, latitude_range: f64) -> u64 {
    interleave(
        ((lat + latitude_range) / (2.0 * latitude_range) * CELLS) as u64,
        ((lon + 180.0) / 360.0 * CELLS) as u64,
    )
}
fn decode(score: f64) -> Option<(f64, f64)> {
    if !score.is_finite() || score < 0.0 || score >= (1u64 << 54) as f64 {
        return None;
    }
    let bits = score as u64;
    let (mut lat, mut lon) = (0u64, 0u64);
    for i in 0..27 {
        lat |= ((bits >> (2 * i)) & 1) << i;
        lon |= ((bits >> (2 * i + 1)) & 1) << i;
    }
    Some((
        ((lon as f64 + 0.5) / CELLS * 360.0 - 180.0).clamp(-180.0, 180.0),
        ((lat as f64 + 0.5) / CELLS * (2.0 * LATITUDE) - LATITUDE).clamp(-LATITUDE, LATITUDE),
    ))
}
fn distance(a: (f64, f64), b: (f64, f64)) -> f64 {
    let u = ((b.1 - a.1).to_radians() * 0.5).sin();
    let v = ((b.0 - a.0).to_radians() * 0.5).sin();
    let h = u * u + a.1.to_radians().cos() * b.1.to_radians().cos() * v * v;
    2.0 * EARTH * h.clamp(0.0, 1.0).sqrt().asin()
}
fn coord_reply(point: (f64, f64)) -> Resp {
    array(vec![Resp::Double(point.0), Resp::Double(point.1)])
}
fn distance_reply(meters: f64, conversion: f64) -> Resp {
    Resp::Bulk(Some(Bytes::from(format!("{:.4}", meters / conversion))))
}
fn hash_reply(point: (f64, f64)) -> Resp {
    const ALPHABET: &[u8] = b"0123456789bcdefghjkmnpqrstuvwxyz";
    let bits = encode(point.0, point.1, 90.0);
    let mut result = [b'0'; 11];
    for (i, byte) in result[..10].iter_mut().enumerate() {
        *byte = ALPHABET[((bits >> (52 - (i + 1) * 5)) & 31) as usize];
    }
    Resp::Bulk(Some(Bytes::copy_from_slice(&result)))
}

#[derive(Clone, Copy)]
enum Shape {
    Circle(f64),
    Box(f64, f64),
}
impl Shape {
    fn contains(self, center: (f64, f64), point: (f64, f64), dist: f64) -> bool {
        match self {
            Self::Circle(radius) => dist <= radius,
            Self::Box(width, height) => {
                EARTH * (point.1 - center.1).to_radians().abs() <= height * 0.5
                    && distance((center.0, point.1), point) <= width * 0.5
            }
        }
    }
    // Conservative spherical envelope. Dateline-crossing intervals are split.
    fn bounds(self, center: (f64, f64)) -> Vec<[f64; 4]> {
        let (horizontal, vertical) = match self {
            Self::Circle(r) => (r, r),
            Self::Box(w, h) => (w * 0.5, h * 0.5),
        };
        let lat_delta = (vertical / EARTH).to_degrees();
        let low = (center.1 - lat_delta).max(-LATITUDE);
        let high = (center.1 + lat_delta).min(LATITUDE);
        let extreme = low.abs().max(high.abs()).to_radians();
        let ratio = (horizontal / (2.0 * EARTH))
            .min(std::f64::consts::FRAC_PI_2)
            .sin()
            / extreme.cos();
        // Using the most poleward latitude also covers circles conservatively.
        let delta = if horizontal >= std::f64::consts::PI * EARTH || ratio >= 1.0 {
            180.0
        } else {
            (2.0 * ratio.asin()).to_degrees().min(180.0)
        };
        if delta >= 180.0 {
            return vec![[-180.0, 180.0, low, high]];
        }
        let left = center.0 - delta;
        let right = center.0 + delta;
        if left < -180.0 {
            vec![[-180.0, right, low, high], [left + 360.0, 180.0, low, high]]
        } else if right > 180.0 {
            vec![[left, 180.0, low, high], [-180.0, right - 360.0, low, high]]
        } else {
            vec![[left, right, low, high]]
        }
    }
}

// Cover an envelope with disjoint Morton/geohash prefix intervals. Recursion is
// bounded; boundary cells are refined eight times, interior cells need one seek.
fn cover(
    bounds: &[f64; 4],
    cell: [f64; 4],
    prefix: u64,
    depth: usize,
    ranges: &mut Vec<(u64, u64)>,
) {
    if cell[1] < bounds[0] || cell[0] > bounds[1] || cell[3] < bounds[2] || cell[2] > bounds[3] {
        return;
    }
    if depth == 8
        || (cell[0] >= bounds[0]
            && cell[1] <= bounds[1]
            && cell[2] >= bounds[2]
            && cell[3] <= bounds[3])
    {
        let shift = 52 - 2 * depth;
        ranges.push((prefix << shift, (prefix + 1) << shift));
        return;
    }
    let (lon, lat) = ((cell[0] + cell[1]) * 0.5, (cell[2] + cell[3]) * 0.5);
    for quadrant in 0..4u64 {
        let child = [
            if quadrant & 2 == 0 { cell[0] } else { lon },
            if quadrant & 2 == 0 { lon } else { cell[1] },
            if quadrant & 1 == 0 { cell[2] } else { lat },
            if quadrant & 1 == 0 { lat } else { cell[3] },
        ];
        cover(bounds, child, prefix * 4 + quadrant, depth + 1, ranges);
    }
}
struct Hit {
    member: Bytes,
    score: f64,
    point: (f64, f64),
    distance: f64,
}
#[derive(Default)]
struct Options {
    center: Option<(f64, f64)>,
    member: Option<Bytes>,
    shape: Option<Shape>,
    conversion: f64,
    order: i8,
    count: Option<usize>,
    any: bool,
    coord: bool,
    dist: bool,
    hash: bool,
    destination: Option<Bytes>,
    storedist: bool,
}
impl Options {
    fn parse(args: &[Bytes]) -> Result<(Self, usize), Resp> {
        let search = args[0].eq_ignore_ascii_case(b"geosearch")
            || args[0].eq_ignore_ascii_case(b"geosearchstore");
        let store = args[0].eq_ignore_ascii_case(b"geosearchstore");
        let readonly = args[0].eq_ignore_ascii_case(b"georadius_ro")
            || args[0].eq_ignore_ascii_case(b"georadiusbymember_ro");
        let bymember = args[0].eq_ignore_ascii_case(b"georadiusbymember")
            || args[0].eq_ignore_ascii_case(b"georadiusbymember_ro");
        let source = if store { 2 } else { 1 };
        let mut options = Self {
            conversion: 1.0,
            destination: store.then(|| args[1].clone()),
            ..Self::default()
        };
        let mut i = source + 1;
        if !search {
            if bymember {
                options.member = Some(args[i].clone());
                i += 1;
            } else {
                options.center = Some(coordinates(&args[i], &args[i + 1])?);
                i += 2;
            }
            let radius = nonnegative(&args[i])?;
            options.conversion = unit(&args[i + 1])?;
            options.shape = Some(Shape::Circle(radius * options.conversion));
            i += 2;
        }
        while i < args.len() {
            let token = &args[i];
            if token.eq_ignore_ascii_case(b"FROMMEMBER")
                && search
                && i + 1 < args.len()
                && options.center.is_none()
            {
                options.member = Some(args[i + 1].clone());
                i += 2;
            } else if token.eq_ignore_ascii_case(b"FROMLONLAT")
                && search
                && i + 2 < args.len()
                && options.member.is_none()
            {
                options.center = Some(coordinates(&args[i + 1], &args[i + 2])?);
                i += 3;
            } else if token.eq_ignore_ascii_case(b"BYRADIUS")
                && search
                && i + 2 < args.len()
                && !matches!(options.shape, Some(Shape::Box(..)))
            {
                let radius = nonnegative(&args[i + 1])?;
                options.conversion = unit(&args[i + 2])?;
                options.shape = Some(Shape::Circle(radius * options.conversion));
                i += 3;
            } else if token.eq_ignore_ascii_case(b"BYBOX")
                && search
                && i + 3 < args.len()
                && !matches!(options.shape, Some(Shape::Circle(..)))
            {
                let width = number(&args[i + 1]).map_err(|_| error("ERR need numeric width"))?;
                let height = number(&args[i + 2]).map_err(|_| error("ERR need numeric height"))?;
                if width < 0.0 || height < 0.0 {
                    return Err(error("ERR height or width cannot be negative"));
                }
                options.conversion = unit(&args[i + 3])?;
                options.shape = Some(Shape::Box(
                    width * options.conversion,
                    height * options.conversion,
                ));
                i += 4;
            } else if token.eq_ignore_ascii_case(b"ASC") {
                options.order = 1;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"DESC") {
                options.order = -1;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"WITHCOORD") {
                options.coord = true;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"WITHDIST") {
                options.dist = true;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"WITHHASH") {
                options.hash = true;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"ANY") {
                options.any = true;
                i += 1;
            } else if token.eq_ignore_ascii_case(b"COUNT") && i + 1 < args.len() {
                let count = integer(&args[i + 1]).ok_or_else(integer_error)?;
                if count <= 0 {
                    return Err(error("ERR COUNT must be > 0"));
                }
                options.count = Some(usize::try_from(count).map_err(|_| integer_error())?);
                i += 2;
            } else if token.eq_ignore_ascii_case(b"STOREDIST") && store {
                options.storedist = true;
                i += 1;
            } else if (token.eq_ignore_ascii_case(b"STORE")
                || token.eq_ignore_ascii_case(b"STOREDIST"))
                && !search
                && !readonly
                && i + 1 < args.len()
            {
                options.destination = Some(args[i + 1].clone());
                options.storedist = token.eq_ignore_ascii_case(b"STOREDIST");
                i += 2;
            } else {
                return Err(syntax_error());
            }
        }
        if options.center.is_none() && options.member.is_none() {
            return Err(error(
                "ERR exactly one of FROMMEMBER or FROMLONLAT can be specified",
            ));
        }
        if options.shape.is_none() {
            return Err(error(
                "ERR exactly one of BYRADIUS and BYBOX can be specified",
            ));
        }
        if options.any && options.count.is_none() {
            return Err(error("ERR the ANY argument requires COUNT argument"));
        }
        if options.destination.is_some() && (options.coord || options.dist || options.hash) {
            return Err(error(
                "ERR STORE is not compatible with WITHDIST, WITHHASH and WITHCOORD options",
            ));
        }
        if options.count.is_some() && options.order == 0 && !options.any {
            options.order = 1;
        }
        Ok((options, source))
    }
}

impl Engine {
    fn geoadd(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (mut nx, mut xx, mut ch) = (false, false, false);
        let mut i = 2;
        while i < args.len() {
            if args[i].eq_ignore_ascii_case(b"NX") {
                nx = true;
            } else if args[i].eq_ignore_ascii_case(b"XX") {
                xx = true;
            } else if args[i].eq_ignore_ascii_case(b"CH") {
                ch = true;
            } else {
                break;
            }
            i += 1;
        }
        if nx && xx {
            return error("ERR XX and NX options at the same time are not compatible");
        }
        if i == args.len() || !(args.len() - i).is_multiple_of(3) {
            return syntax_error();
        }
        let mut scores = Vec::with_capacity((args.len() - i) / 3);
        for triple in args[i..].as_chunks::<3>().0 {
            let point = match coordinates(&triple[0], &triple[1]) {
                Ok(point) => point,
                Err(reply) => return reply,
            };
            scores.push(encode(point.0, point.1, LATITUDE) as f64);
        }
        if let Err(reply) = self.sorted_set(&args[1], now) {
            return reply;
        }
        let (mut added, mut changed) = (0, 0);
        let mut updates = Vec::new();
        for (triple, score) in args[i..].as_chunks::<3>().0.iter().zip(scores) {
            let old = match self.entries.get(&args[1]).map(|entry| &entry.value) {
                Some(Value::SortedSet(value)) => value.geo_score(&triple[2]),
                _ => None,
            };
            if (nx && old.is_some()) || (xx && old.is_none()) || old == Some(score) {
                continue;
            }
            let Value::SortedSet(value) =
                self.collection_entry(&args[1], now, || Value::SortedSet(Box::default()))
            else {
                unreachable!()
            };
            value.geo_insert(triple[2].clone(), score);
            added += i64::from(old.is_none());
            changed += 1;
            if self.journal_enabled {
                updates.push((triple[2].clone(), score));
            }
        }
        if !updates.is_empty() {
            self.record(now, || Mutation::SortedSetAdd {
                key: args[1].clone(),
                entries: updates,
            });
        }
        Resp::Integer(if ch { changed } else { added })
    }
    fn geoposition(&mut self, args: &[Bytes], now: i64) -> Resp {
        let limit = self.reply_limit;
        let hash = args[0].eq_ignore_ascii_case(b"geohash");
        let value = match self.sorted_set(&args[1], now) {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        bounded_replies(
            args[2..].iter().map(|member| {
                value
                    .and_then(|value| value.geo_score(member))
                    .and_then(decode)
                    .map_or(
                        if hash {
                            Resp::Bulk(None)
                        } else {
                            Resp::Array(None)
                        },
                        |point| {
                            if hash {
                                hash_reply(point)
                            } else {
                                coord_reply(point)
                            }
                        },
                    )
            }),
            limit,
        )
    }
    fn geodist(&mut self, args: &[Bytes], now: i64) -> Resp {
        let conversion = match args.get(4).map_or(Ok(1.0), |raw| unit(raw)) {
            Ok(unit) => unit,
            Err(reply) => return reply,
        };
        let value = match self.sorted_set(&args[1], now) {
            Ok(Some(value)) => value,
            Ok(None) => return Resp::Bulk(None),
            Err(reply) => return reply,
        };
        match (
            value.geo_score(&args[2]).and_then(decode),
            value.geo_score(&args[3]).and_then(decode),
        ) {
            (Some(a), Some(b)) => distance_reply(distance(a, b), conversion),
            _ => Resp::Bulk(None),
        }
    }
    fn geosearch(&mut self, args: &[Bytes], now: i64) -> Resp {
        let (mut options, source) = match Options::parse(args) {
            Ok(parsed) => parsed,
            Err(reply) => return reply,
        };
        let value = match self.sorted_set(&args[source], now) {
            Ok(value) => value,
            Err(reply) => return reply,
        };
        let mut hits = Vec::new();
        if let Some(value) = value {
            if let Some(member) = &options.member {
                options.center = value.geo_score(member).and_then(decode);
                if options.center.is_none() {
                    return error("ERR could not decode requested zset member");
                }
            }
            let center = options.center.expect("validated search center");
            let shape = options.shape.expect("validated search shape");
            let mut ranges = Vec::new();
            for bounds in shape.bounds(center) {
                cover(
                    &bounds,
                    [-180.0, 180.0, -LATITUDE, LATITUDE],
                    0,
                    0,
                    &mut ranges,
                );
            }
            // Exact upper coordinate endpoints carry the 27th interleaved bit.
            ranges.push((1 << 52, 1 << 54));
            ranges.sort_unstable();
            let mut merged: Vec<(u64, u64)> = Vec::new();
            for (start, end) in ranges {
                if let Some(last) = merged.last_mut().filter(|last| start <= last.1) {
                    last.1 = last.1.max(end);
                } else {
                    merged.push((start, end));
                }
            }
            'ranges: for (start, end) in merged {
                for (member, score) in value.geo_range(start, end) {
                    let Some(point) = decode(score) else {
                        continue;
                    };
                    let dist = distance(center, point);
                    if shape.contains(center, point, dist) {
                        hits.push(Hit {
                            member: member.clone(),
                            score,
                            point,
                            distance: dist,
                        });
                        if options.any && options.count.is_some_and(|count| hits.len() >= count) {
                            break 'ranges;
                        }
                    }
                }
            }
        }
        if options.order != 0 {
            hits.sort_unstable_by(|a, b| {
                let order = a
                    .distance
                    .total_cmp(&b.distance)
                    .then_with(|| a.member.cmp(&b.member));
                if options.order < 0 {
                    order.reverse()
                } else {
                    order
                }
            });
        }
        if let Some(count) = options.count {
            hits.truncate(count);
        }
        if let Some(destination) = options.destination {
            let count = hits.len();
            if self.remove(&destination).is_some() {
                self.record(now, || Mutation::Delete {
                    key: destination.clone(),
                });
            }
            if !hits.is_empty() {
                let mut updates = Vec::new();
                let journaling = self.journal_enabled;
                let Value::SortedSet(value) =
                    self.collection_entry(&destination, now, || Value::SortedSet(Box::default()))
                else {
                    unreachable!()
                };
                for hit in hits {
                    let score = if options.storedist {
                        hit.distance / options.conversion
                    } else {
                        hit.score
                    };
                    if journaling {
                        updates.push((hit.member.clone(), score));
                    }
                    value.geo_insert(hit.member, score);
                }
                if !updates.is_empty() {
                    self.record(now, || Mutation::SortedSetAdd {
                        key: destination,
                        entries: updates,
                    });
                }
            }
            Resp::Integer(count as i64)
        } else {
            bounded_replies(
                hits.into_iter().map(|hit| {
                    let member = Resp::Bulk(Some(hit.member));
                    if !(options.dist || options.hash || options.coord) {
                        return member;
                    }
                    let mut result = vec![member];
                    if options.dist {
                        result.push(distance_reply(hit.distance, options.conversion));
                    }
                    if options.hash {
                        result.push(Resp::Integer(hit.score as i64));
                    }
                    if options.coord {
                        result.push(coord_reply(hit.point));
                    }
                    array(result)
                }),
                self.reply_limit,
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn run(engine: &mut Engine, args: &[&str]) -> Resp {
        engine.execute_at(
            &args
                .iter()
                .map(|s| Bytes::copy_from_slice(s.as_bytes()))
                .collect::<Vec<_>>(),
            1000,
        )
    }
    fn seed(engine: &mut Engine) {
        assert_eq!(
            run(
                engine,
                &[
                    "GEOADD",
                    "sicily",
                    "13.361389",
                    "38.115556",
                    "Palermo",
                    "15.087269",
                    "37.502669",
                    "Catania"
                ]
            ),
            Resp::Integer(2)
        );
    }
    #[test]
    fn redis_scores_hashes_and_distance_are_interoperable() {
        let mut engine = Engine::new();
        seed(&mut engine);
        assert_eq!(
            run(&mut engine, &["ZSCORE", "sicily", "Palermo"]),
            Resp::Double(3479099956230698.0)
        );
        assert_eq!(
            run(&mut engine, &["GEOHASH", "sicily", "Palermo", "Catania"]),
            array(vec![
                Resp::Bulk(Some(Bytes::from_static(b"sqc8b49rny0"))),
                Resp::Bulk(Some(Bytes::from_static(b"sqdtr74hyu0")))
            ])
        );
        assert_eq!(
            run(
                &mut engine,
                &["GEODIST", "sicily", "Palermo", "Catania", "km"]
            ),
            Resp::Bulk(Some(Bytes::from_static(b"166.2742")))
        );
    }
    #[test]
    fn search_box_count_store_and_readonly_legacy() {
        let mut engine = Engine::new();
        seed(&mut engine);
        let nearest = array(vec![Resp::Bulk(Some(Bytes::from_static(b"Catania")))]);
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEOSEARCH",
                    "sicily",
                    "FROMLONLAT",
                    "15",
                    "37",
                    "BYBOX",
                    "400",
                    "400",
                    "km",
                    "COUNT",
                    "1"
                ]
            ),
            nearest
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEOSEARCHSTORE",
                    "out",
                    "sicily",
                    "FROMMEMBER",
                    "Palermo",
                    "BYRADIUS",
                    "200",
                    "km"
                ]
            ),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut engine, &["GEODIST", "out", "Palermo", "Catania", "km"]),
            Resp::Bulk(Some(Bytes::from_static(b"166.2742")))
        );
        assert!(matches!(
            run(
                &mut engine,
                &[
                    "GEORADIUS_RO",
                    "sicily",
                    "15",
                    "37",
                    "200",
                    "km",
                    "STORE",
                    "out"
                ]
            ),
            Resp::Error(_)
        ));
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEORADIUSBYMEMBER",
                    "sicily",
                    "Palermo",
                    "200",
                    "km",
                    "STOREDIST",
                    "out"
                ]
            ),
            Resp::Integer(2)
        );
        assert_eq!(
            run(&mut engine, &["ZSCORE", "out", "Palermo"]),
            Resp::Double(0.0)
        );
    }
    #[test]
    fn coordinate_validation_is_atomic_and_nx_xx_preserve_type() {
        let mut engine = Engine::new();
        assert!(matches!(
            run(
                &mut engine,
                &["GEOADD", "x", "1", "2", "ok", "181", "0", "bad"]
            ),
            Resp::Error(_)
        ));
        assert_eq!(run(&mut engine, &["EXISTS", "x"]), Resp::Integer(0));
        assert_eq!(
            run(&mut engine, &["GEOADD", "x", "XX", "0", "0", "a"]),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, &["EXISTS", "x"]), Resp::Integer(0));
        assert_eq!(
            run(&mut engine, &["GEOADD", "x", "180", "85.05112878", "edge"]),
            Resp::Integer(1)
        );
        run(&mut engine, &["SET", "string", "value"]);
        assert!(matches!(
            run(&mut engine, &["GEOADD", "string", "0", "0", "a"]),
            Resp::Error(_)
        ));
    }
    #[test]
    fn dateline_and_small_radius_use_spatial_filtering() {
        let mut engine = Engine::new();
        run(
            &mut engine,
            &[
                "GEOADD", "world", "179.9", "0", "east", "-179.9", "0", "west", "0", "0", "far",
            ],
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEOSEARCHSTORE",
                    "near",
                    "world",
                    "FROMLONLAT",
                    "180",
                    "0",
                    "BYRADIUS",
                    "30",
                    "km"
                ]
            ),
            Resp::Integer(2)
        );
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEORADIUS",
                    "world",
                    "0",
                    "0",
                    "1",
                    "m",
                    "COUNT",
                    "1",
                    "ANY"
                ]
            ),
            array(vec![Resp::Bulk(Some(Bytes::from_static(b"far")))])
        );
    }
    #[test]
    fn update_ttl_and_destination_overwrite_semantics() {
        let mut engine = Engine::new();
        seed(&mut engine);
        run(&mut engine, &["PEXPIRE", "sicily", "10000"]);
        assert_eq!(
            run(
                &mut engine,
                &["GEOADD", "sicily", "CH", "14", "38", "Palermo"]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["PTTL", "sicily"]), Resp::Integer(10000));
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEOSEARCHSTORE",
                    "sicily",
                    "sicily",
                    "FROMMEMBER",
                    "Palermo",
                    "BYRADIUS",
                    "1",
                    "m"
                ]
            ),
            Resp::Integer(1)
        );
        assert_eq!(run(&mut engine, &["PTTL", "sicily"]), Resp::Integer(-1));
        run(&mut engine, &["SET", "out", "wrongtype"]);
        assert_eq!(
            run(
                &mut engine,
                &[
                    "GEOSEARCHSTORE",
                    "out",
                    "missing",
                    "FROMMEMBER",
                    "absent",
                    "BYRADIUS",
                    "1",
                    "km"
                ]
            ),
            Resp::Integer(0)
        );
        assert_eq!(run(&mut engine, &["EXISTS", "out"]), Resp::Integer(0));
    }
    #[test]
    fn underflowing_geo_arguments_are_rejected_before_alias_destination_mutation() {
        let mut engine = Engine::new();
        run(&mut engine, &["GEOADD", "geo", "1", "1", "a"]);
        run(&mut engine, &["PEXPIRE", "geo", "10000"]);
        let original = run(&mut engine, &["ZSCORE", "geo", "a"]);
        for options in [
            vec!["FROMLONLAT", "0", "0", "BYRADIUS", "1e-9999", "m"],
            vec!["FROMLONLAT", "1e-9999", "0", "BYRADIUS", "1", "m"],
            vec!["FROMLONLAT", "0", "-1e-9999", "BYRADIUS", "1", "m"],
            vec!["FROMMEMBER", "a", "BYBOX", "1e-9999", "1", "m"],
            vec!["FROMMEMBER", "a", "BYBOX", "1", "1e-9999", "m"],
        ] {
            let mut args = vec!["GEOSEARCHSTORE", "geo", "geo"];
            args.extend(options);
            assert!(
                matches!(run(&mut engine, &args), Resp::Error(_)),
                "{args:?}"
            );
            assert_eq!(run(&mut engine, &["ZSCORE", "geo", "a"]), original);
            assert_eq!(run(&mut engine, &["ZCARD", "geo"]), Resp::Integer(1));
            assert_eq!(run(&mut engine, &["PTTL", "geo"]), Resp::Integer(10000));
        }
        assert!(matches!(
            run(
                &mut engine,
                &[
                    "GEOADD", "geo", "2", "2", "valid", "1e-9999", "0", "invalid"
                ],
            ),
            Resp::Error(_),
        ));
        assert_eq!(run(&mut engine, &["ZCARD", "geo"]), Resp::Integer(1));
        assert_eq!(run(&mut engine, &["ZSCORE", "geo", "a"]), original);
    }

    #[test]
    fn geo_numeric_zero_and_representable_subnormals_remain_valid() {
        let mut engine = Engine::new();
        for value in ["0", "-0", "0e-9999", "4.9406564584124654e-324", "1e-320"] {
            assert_eq!(
                run(&mut engine, &["GEOADD", "geo", value, value, "a"]),
                Resp::Integer(if value == "0" { 1 } else { 0 }),
            );
            assert_eq!(
                run(
                    &mut engine,
                    &[
                        "GEOSEARCHSTORE",
                        "geo",
                        "geo",
                        "FROMMEMBER",
                        "a",
                        "BYRADIUS",
                        value,
                        "m",
                    ],
                ),
                Resp::Integer(1),
                "{value}",
            );
        }
    }
}
