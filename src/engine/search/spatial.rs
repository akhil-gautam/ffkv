use super::model::{DocId, DocSet};
use geo::{BoundingRect, CoordsIter, Geometry, Point, Relate, Validation};
use std::collections::BTreeMap;

const EARTH_RADIUS: f64 = 6_372_797.560856;
const GEO_LAT_LIMIT: f64 = 85.05112878;
const TOL: f64 = 1e-11;

pub fn validate_geo(lon: f64, lat: f64) -> Result<(), String> {
    if !lon.is_finite()
        || !lat.is_finite()
        || !(-180.0..=180.0).contains(&lon)
        || !(-GEO_LAT_LIMIT..=GEO_LAT_LIMIT).contains(&lat)
    {
        return Err("Invalid GEO coordinates".into());
    }
    Ok(())
}
pub fn geo_distance(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let (a, b) = (lat1.to_radians(), lat2.to_radians());
    let delta_lon = ((lon2 - lon1 + 180.0).rem_euclid(360.0) - 180.0).to_radians();
    let h = ((b - a) / 2.0).sin().powi(2) + a.cos() * b.cos() * (delta_lon / 2.0).sin().powi(2);
    2.0 * EARTH_RADIUS * h.clamp(0.0, 1.0).sqrt().asin()
}
fn meters(radius: f64, unit: &str) -> Result<f64, String> {
    if !radius.is_finite() || radius < 0.0 {
        return Err("GEO radius must be nonnegative and finite".into());
    }
    let factor = match unit.to_ascii_lowercase().as_str() {
        "m" => 1.0,
        "km" => 1000.0,
        "mi" => 1609.344,
        "ft" => 0.3048,
        _ => return Err("Unsupported GEO unit".into()),
    };
    Ok(radius * factor)
}
fn cell(p: [f64; 2]) -> (i32, i32) {
    (
        (p[1] + 90.0).floor().clamp(0.0, 179.0) as i32,
        (p[0] + 180.0).floor().clamp(0.0, 359.0) as i32,
    )
}
#[derive(Clone, Debug, Default)]
pub struct GeoIndex {
    cells: BTreeMap<(i32, i32), DocSet>,
    points: BTreeMap<DocId, Vec<[f64; 2]>>,
}
impl GeoIndex {
    pub fn new() -> Self {
        Self::default()
    }
    /// Counts owned point capacities and live postings, estimating tree-node overhead.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        let overhead = 3usize.saturating_mul(size_of::<usize>());
        let mut bytes = size_of::<Self>();
        for p in self.points.values() {
            bytes = bytes
                .saturating_add(size_of::<(DocId, Vec<[f64; 2]>)>())
                .saturating_add(overhead)
                .saturating_add(p.capacity().saturating_mul(size_of::<[f64; 2]>()));
        }
        for ids in self.cells.values() {
            bytes = bytes
                .saturating_add(size_of::<((i32, i32), DocSet)>())
                .saturating_add(overhead)
                .saturating_add(
                    ids.len()
                        .saturating_mul(size_of::<DocId>().saturating_add(overhead)),
                );
        }
        bytes
    }
    /// The coordinator validates every point before committing any field of a document.
    pub fn insert(&mut self, id: DocId, points: &[[f64; 2]]) {
        self.remove(id);
        for p in points {
            self.cells.entry(cell(*p)).or_default().insert(id);
        }
        if !points.is_empty() {
            self.points.insert(id, points.to_vec());
        }
    }
    pub fn remove(&mut self, id: DocId) {
        if let Some(points) = self.points.remove(&id) {
            for p in points {
                let key = cell(p);
                if let Some(ids) = self.cells.get_mut(&key) {
                    ids.remove(&id);
                    if ids.is_empty() {
                        self.cells.remove(&key);
                    }
                }
            }
        }
    }
    pub fn query(&self, lon: f64, lat: f64, radius: f64, unit: &str) -> Result<DocSet, String> {
        validate_geo(lon, lat)?;
        let radius = meters(radius, unit)?;
        let angle = (radius / EARTH_RADIUS).min(std::f64::consts::PI);
        let low = (lat - angle.to_degrees()).max(-90.0);
        let high = (lat + angle.to_degrees()).min(90.0);
        let all_lon = low <= -90.0 || high >= 90.0 || angle >= std::f64::consts::PI;
        let delta = if all_lon {
            180.0
        } else {
            (angle.sin() / lat.to_radians().cos())
                .clamp(-1.0, 1.0)
                .asin()
                .abs()
                .to_degrees()
        };
        let mut intervals = [(-180.0, 180.0); 2];
        let count = if all_lon {
            1
        } else if lon - delta <= -180.0 {
            intervals[0] = (-180.0, lon + delta);
            intervals[1] = (lon - delta + 360.0, 180.0);
            2
        } else if lon + delta >= 180.0 {
            intervals[0] = (lon - delta, 180.0);
            intervals[1] = (-180.0, lon + delta - 360.0);
            2
        } else {
            intervals[0] = (lon - delta, lon + delta);
            1
        };
        let mut candidates = DocSet::new();
        let (y0, y1) = (cell([0.0, low]).0, cell([0.0, high]).0);
        for y in y0..=y1 {
            for &(a, b) in &intervals[..count] {
                let (x0, x1) = (cell([a, 0.0]).1, cell([b, 0.0]).1);
                for (_, ids) in self.cells.range((y, x0)..=(y, x1)) {
                    candidates.extend(ids);
                }
            }
        }
        candidates.retain(|id| {
            self.points[id]
                .iter()
                .any(|p| geo_distance(lon, lat, p[0], p[1]) <= radius)
        });
        Ok(candidates)
    }
}

#[derive(Clone, Copy, Debug)]
struct Bounds {
    min: [f64; 2],
    max: [f64; 2],
}
impl Bounds {
    fn union(self, o: Self) -> Self {
        Self {
            min: [self.min[0].min(o.min[0]), self.min[1].min(o.min[1])],
            max: [self.max[0].max(o.max[0]), self.max[1].max(o.max[1])],
        }
    }
    fn intersects(self, o: Self) -> bool {
        (0..2).all(|i| self.min[i] <= o.max[i] && o.min[i] <= self.max[i])
    }
}
/// Augmented interval treap: subtree rectangles reject entire branches before exact predicates.
#[derive(Clone, Debug)]
struct SpatialNode {
    id: DocId,
    priority: u64,
    bounds: Bounds,
    subtree: Bounds,
    left: Option<Box<SpatialNode>>,
    right: Option<Box<SpatialNode>>,
}
impl SpatialNode {
    fn refresh(&mut self) {
        self.subtree = self.bounds;
        if let Some(n) = &self.left {
            self.subtree = self.subtree.union(n.subtree);
        }
        if let Some(n) = &self.right {
            self.subtree = self.subtree.union(n.subtree);
        }
    }
    fn key(&self) -> (geo_key::Float, DocId) {
        (geo_key::Float(self.bounds.min[0]), self.id)
    }
}
mod geo_key {
    #[derive(Clone, Copy, Debug)]
    pub struct Float(pub f64);
    impl PartialEq for Float {
        fn eq(&self, o: &Self) -> bool {
            self.0.total_cmp(&o.0).is_eq()
        }
    }
    impl Eq for Float {}
    impl PartialOrd for Float {
        fn partial_cmp(&self, o: &Self) -> Option<std::cmp::Ordering> {
            Some(self.cmp(o))
        }
    }
    impl Ord for Float {
        fn cmp(&self, o: &Self) -> std::cmp::Ordering {
            self.0.total_cmp(&o.0)
        }
    }
}
fn priority(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}
fn split(
    root: Option<Box<SpatialNode>>,
    key: (geo_key::Float, DocId),
) -> (Option<Box<SpatialNode>>, Option<Box<SpatialNode>>) {
    let Some(mut n) = root else {
        return (None, None);
    };
    if n.key() < key {
        let (a, b) = split(n.right.take(), key);
        n.right = a;
        n.refresh();
        (Some(n), b)
    } else {
        let (a, b) = split(n.left.take(), key);
        n.left = b;
        n.refresh();
        (a, Some(n))
    }
}
fn tree_insert(
    root: Option<Box<SpatialNode>>,
    mut node: Box<SpatialNode>,
) -> Option<Box<SpatialNode>> {
    let Some(mut root) = root else {
        return Some(node);
    };
    if node.priority > root.priority {
        let (a, b) = split(Some(root), node.key());
        node.left = a;
        node.right = b;
        node.refresh();
        Some(node)
    } else {
        if node.key() < root.key() {
            root.left = tree_insert(root.left.take(), node);
        } else {
            root.right = tree_insert(root.right.take(), node);
        }
        root.refresh();
        Some(root)
    }
}
fn merge(a: Option<Box<SpatialNode>>, b: Option<Box<SpatialNode>>) -> Option<Box<SpatialNode>> {
    match (a, b) {
        (None, b) => b,
        (a, None) => a,
        (Some(mut a), Some(mut b)) => {
            if a.priority > b.priority {
                a.right = merge(a.right.take(), Some(b));
                a.refresh();
                Some(a)
            } else {
                b.left = merge(Some(a), b.left.take());
                b.refresh();
                Some(b)
            }
        }
    }
}
fn tree_remove(
    root: Option<Box<SpatialNode>>,
    key: (geo_key::Float, DocId),
) -> Option<Box<SpatialNode>> {
    let mut root = root?;
    match key.cmp(&root.key()) {
        std::cmp::Ordering::Equal => return merge(root.left.take(), root.right.take()),
        std::cmp::Ordering::Less => root.left = tree_remove(root.left.take(), key),
        std::cmp::Ordering::Greater => root.right = tree_remove(root.right.take(), key),
    }
    root.refresh();
    Some(root)
}
fn tree_query(root: &Option<Box<SpatialNode>>, bounds: Bounds, out: &mut DocSet) {
    if let Some(n) = root {
        if !n.subtree.intersects(bounds) {
            return;
        }
        if n.bounds.intersects(bounds) {
            out.insert(n.id);
        }
        tree_query(&n.left, bounds, out);
        tree_query(&n.right, bounds, out);
    }
}
#[derive(Clone, Debug)]
struct Shape {
    geometry: Geometry<f64>,
    bounds: Bounds,
    spherical: Option<SphericalShape>,
}
#[derive(Clone, Debug)]
pub struct ShapeIndex {
    spherical: bool,
    shapes: BTreeMap<DocId, Shape>,
    root: Option<Box<SpatialNode>>,
}
impl ShapeIndex {
    pub fn new(spherical: bool) -> Self {
        Self {
            spherical,
            shapes: BTreeMap::new(),
            root: None,
        }
    }
    /// Capacity-based owned geometry/tree memory; allocator metadata is estimated.
    pub fn memory_bytes(&self) -> usize {
        use std::mem::size_of;
        let entry = size_of::<(DocId, Shape)>()
            .saturating_add(3usize.saturating_mul(size_of::<usize>()))
            .saturating_add(size_of::<SpatialNode>());
        let mut bytes = size_of::<Self>().saturating_add(self.shapes.len().saturating_mul(entry));
        for shape in self.shapes.values() {
            if let Geometry::Polygon(p) = &shape.geometry {
                bytes = bytes.saturating_add(
                    p.exterior()
                        .0
                        .capacity()
                        .saturating_mul(size_of::<geo::Coord<f64>>()),
                );
                bytes = bytes.saturating_add(
                    p.interiors()
                        .len()
                        .saturating_mul(size_of::<geo::LineString<f64>>()),
                );
                for ring in p.interiors() {
                    bytes = bytes.saturating_add(
                        ring.0
                            .capacity()
                            .saturating_mul(size_of::<geo::Coord<f64>>()),
                    );
                }
            }
            if let Some(SphericalShape::Polygon(rings)) = &shape.spherical {
                bytes = bytes.saturating_add(rings.capacity().saturating_mul(size_of::<Vec<V3>>()));
                for ring in rings {
                    bytes = bytes.saturating_add(ring.capacity().saturating_mul(size_of::<V3>()));
                }
            }
        }
        bytes
    }
    pub fn insert(&mut self, id: DocId, wkt: &str) -> Result<(), String> {
        let shape = parse_geometry(wkt, self.spherical)?;
        self.remove(id);
        self.root = tree_insert(
            self.root.take(),
            Box::new(SpatialNode {
                id,
                priority: priority(id),
                bounds: shape.bounds,
                subtree: shape.bounds,
                left: None,
                right: None,
            }),
        );
        self.shapes.insert(id, shape);
        Ok(())
    }
    pub fn remove(&mut self, id: DocId) {
        if let Some(s) = self.shapes.remove(&id) {
            self.root = tree_remove(self.root.take(), (geo_key::Float(s.bounds.min[0]), id));
        }
    }
    pub fn query(&self, predicate: &str, wkt: &str) -> Result<DocSet, String> {
        let predicate = predicate.to_ascii_uppercase();
        if !matches!(
            predicate.as_str(),
            "WITHIN" | "CONTAINS" | "INTERSECTS" | "DISJOINT"
        ) {
            return Err("Unknown GEOSHAPE predicate".into());
        }
        let query = parse_geometry(wkt, self.spherical)?;
        let mut candidates = DocSet::new();
        tree_query(&self.root, query.bounds, &mut candidates);
        let mut result = if predicate == "DISJOINT" {
            self.shapes.keys().copied().collect()
        } else {
            DocSet::new()
        };
        for id in candidates {
            let shape = &self.shapes[&id];
            let matches = if self.spherical {
                let (a, b) = (
                    shape.spherical.as_ref().unwrap(),
                    query.spherical.as_ref().unwrap(),
                );
                match predicate.as_str() {
                    "WITHIN" => spherical_within(a, b),
                    "CONTAINS" => spherical_within(b, a),
                    _ => spherical_intersects(a, b),
                }
            } else {
                let relation = shape.geometry.relate(&query.geometry);
                match predicate.as_str() {
                    "WITHIN" => relation.is_within(),
                    "CONTAINS" => relation.is_contains(),
                    _ => relation.is_intersects(),
                }
            };
            if predicate == "DISJOINT" {
                if matches {
                    result.remove(&id);
                }
            } else if matches {
                result.insert(id);
            }
        }
        Ok(result)
    }
}
pub fn validate_geometry(wkt: &str, spherical: bool) -> Result<(), String> {
    parse_geometry(wkt, spherical).map(|_| ())
}
fn parse_geometry(input: &str, spherical: bool) -> Result<Shape, String> {
    let parsed = input
        .parse::<wkt::Wkt<f64>>()
        .map_err(|e| format!("Invalid WKT: {e}"))?;
    match &parsed {
        wkt::Wkt::Point(p) => {
            if p.dimension() != wkt::types::Dimension::XY {
                return Err("GEOSHAPE requires two-dimensional coordinates".into());
            }
        }
        wkt::Wkt::Polygon(p) => {
            if p.dimension() != wkt::types::Dimension::XY {
                return Err("GEOSHAPE requires two-dimensional coordinates".into());
            }
            if p.rings().is_empty() {
                return Err("Empty GEOSHAPE is not supported".into());
            }
            for r in p.rings() {
                if r.coords().len() < 4 || r.coords().first() != r.coords().last() {
                    return Err(
                        "Polygon rings must be closed with at least three distinct points".into(),
                    );
                }
            }
        }
        _ => return Err("GEOSHAPE supports POINT and POLYGON".into()),
    }
    let geometry = Geometry::<f64>::try_from(parsed).map_err(|e| format!("Invalid WKT: {e}"))?;
    if geometry.coords_iter().any(|c| {
        !c.x.is_finite()
            || !c.y.is_finite()
            || (spherical && (!(-180.0..=180.0).contains(&c.x) || !(-90.0..=90.0).contains(&c.y)))
    }) {
        return Err("Invalid GEOSHAPE coordinates".into());
    }
    let Some(rect) = geometry.bounding_rect() else {
        return Err("Empty GEOSHAPE is not supported".into());
    };
    let mut bounds = Bounds {
        min: [rect.min().x, rect.min().y],
        max: [rect.max().x, rect.max().y],
    };
    let sphere = if spherical {
        Some(SphericalShape::from_geometry(&geometry)?)
    } else {
        geometry
            .check_validation()
            .map_err(|e| format!("Invalid geometry: {e}"))?;
        None
    };
    if spherical
        && geometry
            .coords_iter()
            .any(|c| c.x.abs() == 180.0 || c.y.abs() == 90.0)
    {
        bounds.min[0] = -180.0;
        bounds.max[0] = 180.0;
    }
    if let Some(s) = &sphere
        && let SphericalShape::Polygon(rings) = s
    {
        // Great-circle edges can bow beyond their endpoint latitudes. Conservatively
        // include poles/dateline when needed; exact spherical predicates do the filtering.
        for ring in rings {
            for edge in ring.windows(2) {
                let a = edge[0];
                let b = edge[1];
                let normal = cross(a, b);
                let z = [0.0, 0.0, 1.0];
                let extremum =
                    normalize(sub(z, scale(normal, dot(z, normal) / dot(normal, normal))));
                for p in [extremum, scale(extremum, -1.0)] {
                    if on_arc(p, a, b) {
                        let lat = p[2].clamp(-1.0, 1.0).asin().to_degrees();
                        bounds.min[1] = bounds.min[1].min(lat);
                        bounds.max[1] = bounds.max[1].max(lat);
                    }
                }
            }
        }
        if rings.iter().any(|r| {
            r.windows(2).any(|w| {
                longitude(w[0]).is_sign_positive() != longitude(w[1]).is_sign_positive()
                    && (longitude(w[0]) - longitude(w[1])).abs() > 180.0
            })
        }) {
            bounds.min[0] = -180.0;
            bounds.max[0] = 180.0;
        }
        for (pole, lat) in [([0.0, 0.0, 1.0], 90.0), ([0.0, 0.0, -1.0], -90.0)] {
            if locate(s, pole) >= 0 {
                bounds.min[0] = -180.0;
                bounds.max[0] = 180.0;
                bounds.min[1] = bounds.min[1].min(lat);
                bounds.max[1] = bounds.max[1].max(lat);
            }
        }
    }
    Ok(Shape {
        geometry,
        bounds,
        spherical: sphere,
    })
}

type V3 = [f64; 3];
fn dot(a: V3, b: V3) -> f64 {
    a[0] * b[0] + a[1] * b[1] + a[2] * b[2]
}
fn cross(a: V3, b: V3) -> V3 {
    [
        a[1] * b[2] - a[2] * b[1],
        a[2] * b[0] - a[0] * b[2],
        a[0] * b[1] - a[1] * b[0],
    ]
}
fn scale(a: V3, t: f64) -> V3 {
    [a[0] * t, a[1] * t, a[2] * t]
}
fn sub(a: V3, b: V3) -> V3 {
    [a[0] - b[0], a[1] - b[1], a[2] - b[2]]
}
fn normalize(a: V3) -> V3 {
    let norm = dot(a, a).sqrt();
    if norm == 0.0 {
        [0.0, 0.0, 0.0]
    } else {
        scale(a, 1.0 / norm)
    }
}
fn angle(a: V3, b: V3) -> f64 {
    dot(cross(a, b), cross(a, b)).sqrt().atan2(dot(a, b))
}
fn cartesian(p: Point<f64>) -> V3 {
    let (lon, lat) = (p.x().to_radians(), p.y().to_radians());
    [lat.cos() * lon.cos(), lat.cos() * lon.sin(), lat.sin()]
}
fn longitude(p: V3) -> f64 {
    p[1].atan2(p[0]).to_degrees()
}
fn on_arc(p: V3, a: V3, b: V3) -> bool {
    let normal = cross(a, b);
    dot(p, normal).abs() <= TOL * dot(normal, normal).sqrt()
        && (angle(a, p) + angle(p, b) - angle(a, b)).abs() <= TOL
}
struct ArcIntersections {
    points: [V3; 4],
    len: usize,
}
impl ArcIntersections {
    fn is_empty(&self) -> bool {
        self.len == 0
    }
}
impl IntoIterator for ArcIntersections {
    type Item = V3;
    type IntoIter = std::iter::Take<std::array::IntoIter<V3, 4>>;
    fn into_iter(self) -> Self::IntoIter {
        self.points.into_iter().take(self.len)
    }
}
fn arc_intersections(a: V3, b: V3, c: V3, d: V3) -> ArcIntersections {
    let mut result = ArcIntersections {
        points: [[0.0; 3]; 4],
        len: 0,
    };
    let n = cross(normalize(cross(a, b)), normalize(cross(c, d)));
    let candidates = if dot(n, n) < TOL * TOL {
        [a, b, c, d]
    } else {
        let p = normalize(n);
        [p, scale(p, -1.0), p, scale(p, -1.0)]
    };
    for p in candidates {
        if on_arc(p, a, b)
            && on_arc(p, c, d)
            && !result.points[..result.len]
                .iter()
                .any(|v| angle(*v, p) < TOL)
        {
            result.points[result.len] = p;
            result.len += 1;
        }
    }
    result
}
#[derive(Clone, Debug)]
enum SphericalShape {
    Point(V3),
    Polygon(Vec<Vec<V3>>),
}
impl SphericalShape {
    fn from_geometry(g: &Geometry<f64>) -> Result<Self, String> {
        match g {
            Geometry::Point(p) => Ok(Self::Point(cartesian(*p))),
            Geometry::Polygon(p) => {
                let rings = std::iter::once(p.exterior())
                    .chain(p.interiors())
                    .map(|r| {
                        let mut ring = r.points().map(cartesian).collect::<Vec<_>>();
                        ring.dedup_by(|a, b| *a == *b);
                        ring
                    })
                    .collect::<Vec<_>>();
                for ring in &rings {
                    if ring.len() < 4 {
                        return Err("Invalid spherical polygon ring".into());
                    }
                    for edge in ring.windows(2) {
                        let distance = angle(edge[0], edge[1]);
                        if distance <= TOL || distance >= std::f64::consts::PI - TOL {
                            return Err(
                                "Spherical edges must be nonzero, non-antipodal minor arcs".into(),
                            );
                        }
                    }
                    for i in 0..ring.len() - 1 {
                        for j in i + 1..ring.len() - 1 {
                            if j == i + 1 || (i == 0 && j == ring.len() - 2) {
                                continue;
                            }
                            if !arc_intersections(ring[i], ring[i + 1], ring[j], ring[j + 1])
                                .is_empty()
                            {
                                return Err("Self-intersecting spherical polygon".into());
                            }
                        }
                    }
                    if signed_area(ring).abs() < TOL {
                        return Err("Spherical polygon has zero area".into());
                    }
                }
                for i in 1..rings.len() {
                    if ring_location(&rings[0], rings[i][0]) != 1 {
                        return Err("Polygon hole must lie inside exterior".into());
                    }
                    for j in 0..i {
                        for a in rings[i].windows(2) {
                            for b in rings[j].windows(2) {
                                if !arc_intersections(a[0], a[1], b[0], b[1]).is_empty() {
                                    return Err("Spherical polygon rings intersect".into());
                                }
                            }
                        }
                        if j > 0
                            && (ring_location(&rings[j], rings[i][0]) >= 0
                                || ring_location(&rings[i], rings[j][0]) >= 0)
                        {
                            return Err("Spherical polygon holes overlap".into());
                        }
                    }
                }
                Ok(Self::Polygon(rings))
            }
            _ => Err("Unsupported spherical geometry".into()),
        }
    }
    fn any_vertex(&self, mut predicate: impl FnMut(V3) -> bool) -> bool {
        match self {
            Self::Point(p) => predicate(*p),
            Self::Polygon(r) => r
                .iter()
                .any(|r| r[..r.len() - 1].iter().copied().any(&mut predicate)),
        }
    }
}
fn signed_area(ring: &[V3]) -> f64 {
    let a = ring[0];
    let mut area = 0.0;
    for i in 1..ring.len() - 2 {
        let (b, c) = (ring[i], ring[i + 1]);
        area += 2.0 * dot(a, cross(b, c)).atan2(1.0 + dot(a, b) + dot(b, c) + dot(c, a));
    }
    while area > 2.0 * std::f64::consts::PI {
        area -= 4.0 * std::f64::consts::PI;
    }
    while area < -2.0 * std::f64::consts::PI {
        area += 4.0 * std::f64::consts::PI;
    }
    area
}
// -1 exterior, 0 boundary, 1 interior. Winding sign selects the smaller spherical region.
fn ring_location(ring: &[V3], p: V3) -> i8 {
    let mut sum = 0.0;
    for e in ring.windows(2) {
        if on_arc(p, e[0], e[1]) {
            return 0;
        }
        let (a, b) = (e[0], e[1]);
        sum += dot(p, cross(a, b)).atan2(dot(a, b) - dot(a, p) * dot(b, p));
    }
    if sum * signed_area(ring).signum() > std::f64::consts::PI {
        1
    } else {
        -1
    }
}
fn ring_interior_point(ring: &[V3]) -> Option<V3> {
    let orientation = signed_area(ring).signum();
    for edge in ring.windows(2) {
        let midpoint = normalize([
            edge[0][0] + edge[1][0],
            edge[0][1] + edge[1][1],
            edge[0][2] + edge[1][2],
        ]);
        let inward = scale(normalize(cross(edge[0], edge[1])), orientation);
        let mut offset = angle(edge[0], edge[1]).min(1.0) * 0.01;
        while offset > TOL {
            let p = normalize([
                midpoint[0] * offset.cos() + inward[0] * offset.sin(),
                midpoint[1] * offset.cos() + inward[1] * offset.sin(),
                midpoint[2] * offset.cos() + inward[2] * offset.sin(),
            ]);
            if ring_location(ring, p) == 1 {
                return Some(p);
            }
            offset *= 0.5;
        }
    }
    None
}
fn locate(shape: &SphericalShape, p: V3) -> i8 {
    match shape {
        SphericalShape::Point(q) => {
            if angle(*q, p) <= TOL {
                1
            } else {
                -1
            }
        }
        SphericalShape::Polygon(rings) => {
            let outside = ring_location(&rings[0], p);
            if outside != 1 {
                return outside;
            }
            for r in &rings[1..] {
                match ring_location(r, p) {
                    0 => return 0,
                    1 => return -1,
                    _ => {}
                }
            }
            1
        }
    }
}
fn spherical_intersects(a: &SphericalShape, b: &SphericalShape) -> bool {
    if a.any_vertex(|p| locate(b, p) >= 0) || b.any_vertex(|p| locate(a, p) >= 0) {
        return true;
    }
    if let (SphericalShape::Polygon(a), SphericalShape::Polygon(b)) = (a, b) {
        for ar in a {
            for br in b {
                for ae in ar.windows(2) {
                    for be in br.windows(2) {
                        if !arc_intersections(ae[0], ae[1], be[0], be[1]).is_empty() {
                            return true;
                        }
                    }
                }
            }
        }
    }
    false
}
fn spherical_within(a: &SphericalShape, b: &SphericalShape) -> bool {
    if let SphericalShape::Point(p) = a {
        return locate(b, *p) == 1;
    }
    let (SphericalShape::Polygon(ar), SphericalShape::Polygon(br)) = (a, b) else {
        return false;
    };
    if a.any_vertex(|p| locate(b, p) < 0) {
        return false;
    }
    // Split each minor arc at every boundary crossing. Testing every open subarc
    // handles concavity, holes, coincident boundaries, and boundary-only contacts.
    for ring in ar {
        for edge in ring.windows(2) {
            let mut splits = vec![edge[0], edge[1]];
            for r in br {
                for e in r.windows(2) {
                    splits.extend(arc_intersections(edge[0], edge[1], e[0], e[1]));
                }
            }
            splits.sort_by(|a, b| angle(edge[0], *a).total_cmp(&angle(edge[0], *b)));
            splits.dedup_by(|a, b| angle(*a, *b) <= TOL);
            for s in splits.windows(2) {
                let midpoint = normalize([s[0][0] + s[1][0], s[0][1] + s[1][1], s[0][2] + s[1][2]]);
                if locate(b, midpoint) < 0 {
                    return false;
                }
            }
        }
    }
    // A query hole entirely enclosed by the candidate excludes that candidate.
    for hole in &br[1..] {
        if hole[..hole.len() - 1].iter().any(|p| locate(a, *p) == 1)
            || ring_interior_point(hole).is_some_and(|p| locate(a, p) == 1)
        {
            return false;
        }
    }
    true
}
