use crate::test_runner::mock_data::constants::ConstantValues;
use crate::test_runner::mock_data::random::{Random, RandomValue};
use fake::{Fake, Faker};

// =============================================================================
// WKT (Well-Known Text) formatting utilities
//
// Used by MySQL spatial types (13.4). WKT is the standard text representation
// for geometry objects as defined by the Open Geospatial Consortium (OGC).
// MySQL uses ST_GeomFromText('WKT') to insert spatial values.
// =============================================================================

/// Format a geo_types::Coord as WKT coordinate pair: "x y"
pub fn wkt_coord(c: &geo_types::Coord<f64>) -> String {
    format!("{} {}", c.x, c.y)
}

/// Format a geo_types::Point as WKT: "POINT(x y)"
pub fn wkt_point(p: &geo_types::Point<f64>) -> String {
    format!("POINT({} {})", p.x(), p.y())
}

/// Format a geo_types::LineString as WKT coordinate list: "(x1 y1, x2 y2, ...)"
/// Returns None if fewer than 2 points.
pub fn wkt_linestring_coords(ls: &geo_types::LineString<f64>) -> Option<String> {
    if ls.0.len() < 2 {
        return None;
    }
    let coords: Vec<String> = ls.0.iter().map(wkt_coord).collect();
    Some(format!("({})", coords.join(", ")))
}

/// Format a geo_types::LineString as WKT: "LINESTRING(x1 y1, x2 y2, ...)"
/// Returns None if fewer than 2 points.
pub fn wkt_linestring(ls: &geo_types::LineString<f64>) -> Option<String> {
    wkt_linestring_coords(ls).map(|coords| format!("LINESTRING{}", coords))
}

/// Format a geo_types::Polygon ring coords as WKT: "((x1 y1, ...), ...)"
/// Returns None if the exterior ring has fewer than 4 points.
/// Interior rings (holes) with fewer than 4 points are silently skipped
/// because MySQL requires every ring to be a valid closed linear ring.
pub fn wkt_polygon_rings(poly: &geo_types::Polygon<f64>) -> Option<String> {
    let ext = poly.exterior();
    if ext.0.len() < 4 {
        return None;
    }
    let mut rings = Vec::new();
    let ext_coords: Vec<String> = ext.0.iter().map(wkt_coord).collect();
    rings.push(format!("({})", ext_coords.join(", ")));
    for hole in poly.interiors() {
        if hole.0.len() >= 4 {
            let hole_coords: Vec<String> = hole.0.iter().map(wkt_coord).collect();
            rings.push(format!("({})", hole_coords.join(", ")));
        }
    }
    Some(format!("({})", rings.join(", ")))
}

/// Format a geo_types::Polygon as WKT: "POLYGON((x1 y1, ..., x1 y1), ...)"
/// Returns None if the exterior ring has fewer than 4 points.
pub fn wkt_polygon(poly: &geo_types::Polygon<f64>) -> Option<String> {
    wkt_polygon_rings(poly).map(|rings| format!("POLYGON{}", rings))
}

/// WKT-formatted Point. Wraps geo_types::Point for WKT output.
pub struct WktPoint(pub geo_types::Point<f64>);

impl std::fmt::Display for WktPoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", wkt_point(&self.0))
    }
}

impl RandomValue for WktPoint {
    fn next_value(random: &mut Random) -> String {
        let p: geo_types::Point<f64> = Faker.fake_with_rng(&mut random.rng);
        WktPoint(p).to_string()
    }
}

impl ConstantValues for WktPoint {
    fn next_values() -> Vec<String> {
        [
            "POINT(0 0)",                                 // origin
            "POINT(-1 -1)",                               // negative coordinates
            "POINT(180 90)",                              // max geographic lon/lat
            "POINT(-180 -90)",                            // min geographic lon/lat
            "POINT(1e15 1e15)",                           // large coordinates (DOUBLE range)
            "POINT(1e-10 1e-10)",                         // small coordinates (precision)
            "POINT(0.123456789012345 0.987654321098765)", // high decimal precision
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted LineString. Wraps geo_types::LineString for WKT output.
pub struct WktLineString(pub geo_types::LineString<f64>);

impl std::fmt::Display for WktLineString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match wkt_linestring(&self.0) {
            Some(wkt) => write!(f, "{}", wkt),
            None => write!(f, "LINESTRING(0 0, 1 1)"),
        }
    }
}

impl RandomValue for WktLineString {
    fn next_value(random: &mut Random) -> String {
        let ls: geo_types::LineString<f64> = Faker.fake_with_rng(&mut random.rng);
        WktLineString(ls).to_string()
    }
}

impl ConstantValues for WktLineString {
    fn next_values() -> Vec<String> {
        [
            "LINESTRING(0 0, 1 1)",                // minimum (2 points)
            "LINESTRING(0 0, 0 0)",                // zero-length (degenerate)
            "LINESTRING(0 0, 1 1, 2 2)",           // collinear points
            "LINESTRING(-10 -10, 10 10, 20 -20)",  // mixed signs / zig-zag
            "LINESTRING(1e15 1e15, -1e15 -1e15)",  // large span
            "LINESTRING(0 0, 1e-10 1e-10)",        // very small segment
            "LINESTRING(0 0, 1 0, 1 1, 0 1, 0 0)", // closed ring as linestring
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted Polygon. Wraps geo_types::Polygon for WKT output.
pub struct WktPolygon(pub geo_types::Polygon<f64>);

impl std::fmt::Display for WktPolygon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match wkt_polygon(&self.0) {
            Some(wkt) => write!(f, "{}", wkt),
            None => write!(f, "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))"),
        }
    }
}

impl RandomValue for WktPolygon {
    /// Generate a random valid axis-aligned rectangle polygon.
    /// We avoid `Faker`-generated polygons because they often produce
    /// self-intersecting rings or interior rings with fewer than 4 points,
    /// which MySQL rejects via ST_GeomFromText.
    fn next_value(random: &mut Random) -> String {
        let cx = random.next_f64().abs() * 100.0;
        let cy = random.next_f64().abs() * 100.0;
        let hw = random.next_f64().abs() * 10.0 + 0.01;
        let hh = random.next_f64().abs() * 10.0 + 0.01;
        let (x0, y0) = (cx - hw, cy - hh);
        let (x1, y1) = (cx + hw, cy + hh);
        format!(
            "POLYGON(({} {}, {} {}, {} {}, {} {}, {} {}))",
            x0, y0, x1, y0, x1, y1, x0, y1, x0, y0
        )
    }
}

impl ConstantValues for WktPolygon {
    fn next_values() -> Vec<String> {
        [
            "POLYGON((0 0, 1 0, 0.5 1, 0 0))",        // minimum triangle
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0))", // square
            "POLYGON((0 0, 10 0, 10 10, 0 10, 0 0), (2 2, 8 2, 8 8, 2 8, 2 2))", // with hole
            "POLYGON((-1e10 -1e10, 1e10 -1e10, 1e10 1e10, -1e10 1e10, -1e10 -1e10))", // large coordinates
            "POLYGON((0 0, 1e-10 0, 1e-10 1e-10, 0 1e-10, 0 0))", // tiny polygon (precision)
            "POLYGON((0 0, 100 0, 100 0.001, 0 0.001, 0 0))",     // very thin / narrow
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted MultiPoint. Composes from two random Points.
pub struct WktMultiPoint;

impl RandomValue for WktMultiPoint {
    fn next_value(random: &mut Random) -> String {
        let p1: geo_types::Point<f64> = Faker.fake_with_rng(&mut random.rng);
        let p2: geo_types::Point<f64> = Faker.fake_with_rng(&mut random.rng);
        format!(
            "MULTIPOINT(({} {}), ({} {}))",
            p1.x(),
            p1.y(),
            p2.x(),
            p2.y()
        )
    }
}

impl ConstantValues for WktMultiPoint {
    fn next_values() -> Vec<String> {
        [
            "MULTIPOINT((0 0))",                        // single point (minimum)
            "MULTIPOINT((0 0), (0 0))",                 // duplicate / coincident points
            "MULTIPOINT((0 0), (1 1), (2 2))",          // collinear points
            "MULTIPOINT((-1e10 -1e10), (1e10 1e10))",   // large spread
            "MULTIPOINT((1e-10 1e-10), (2e-10 2e-10))", // very close points (precision)
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted MultiLineString. Composes from two random LineStrings.
pub struct WktMultiLineString;

impl RandomValue for WktMultiLineString {
    fn next_value(random: &mut Random) -> String {
        let ls1: geo_types::LineString<f64> = Faker.fake_with_rng(&mut random.rng);
        let ls2: geo_types::LineString<f64> = Faker.fake_with_rng(&mut random.rng);
        let coords1 = wkt_linestring_coords(&ls1).unwrap_or_else(|| "(0 0, 1 1)".to_string());
        let coords2 = wkt_linestring_coords(&ls2).unwrap_or_else(|| "(0 0, 2 2)".to_string());
        format!("MULTILINESTRING({}, {})", coords1, coords2)
    }
}

impl ConstantValues for WktMultiLineString {
    fn next_values() -> Vec<String> {
        [
            "MULTILINESTRING((0 0, 1 1))", // single linestring (minimum)
            "MULTILINESTRING((0 0, 1 1), (1 1, 2 2))", // connected / shared endpoint
            "MULTILINESTRING((0 0, 10 0), (0 5, 10 5))", // parallel lines
            "MULTILINESTRING((0 0, 10 10), (0 10, 10 0))", // crossing / X shape
            "MULTILINESTRING((-1e10 0, 1e10 0), (0 -1e10, 0 1e10))", // large cross
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted MultiPolygon. Composes from two random Polygons.
pub struct WktMultiPolygon;

impl RandomValue for WktMultiPolygon {
    /// Generate two non-overlapping random rectangles.
    /// We avoid `Faker`-generated polygons because they often produce
    /// self-intersecting rings or interior rings with fewer than 4 points,
    /// which MySQL rejects via ST_GeomFromText.
    fn next_value(random: &mut Random) -> String {
        // First rectangle in [0, 50] range
        let cx1 = random.next_f64().abs() * 50.0;
        let cy1 = random.next_f64().abs() * 50.0;
        let hw1 = random.next_f64().abs() * 5.0 + 0.01;
        let hh1 = random.next_f64().abs() * 5.0 + 0.01;
        // Second rectangle offset by 200 to guarantee no overlap
        let cx2 = random.next_f64().abs() * 50.0 + 200.0;
        let cy2 = random.next_f64().abs() * 50.0 + 200.0;
        let hw2 = random.next_f64().abs() * 5.0 + 0.01;
        let hh2 = random.next_f64().abs() * 5.0 + 0.01;
        format!(
            "MULTIPOLYGON((({x0} {y0}, {x1} {y0}, {x1} {y1}, {x0} {y1}, {x0} {y0})), \
             (({x2} {y2}, {x3} {y2}, {x3} {y3}, {x2} {y3}, {x2} {y2})))",
            x0 = cx1 - hw1,
            y0 = cy1 - hh1,
            x1 = cx1 + hw1,
            y1 = cy1 + hh1,
            x2 = cx2 - hw2,
            y2 = cy2 - hh2,
            x3 = cx2 + hw2,
            y3 = cy2 + hh2,
        )
    }
}

impl ConstantValues for WktMultiPolygon {
    fn next_values() -> Vec<String> {
        [
            "MULTIPOLYGON(((0 0, 1 0, 0.5 1, 0 0)))",                                                                      // single triangle (minimum)
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((20 20, 30 20, 30 30, 20 30, 20 20)))",                          // two disjoint squares
            "MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((10 0, 20 0, 20 10, 10 10, 10 0)))",                             // adjacent (shared edge)
            "MULTIPOLYGON(((-1e10 -1e10, 1e10 -1e10, 1e10 1e10, -1e10 1e10, -1e10 -1e10)), ((0 0, 1 0, 1 1, 0 1, 0 0)))",   // huge + tiny
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// WKT-formatted GeometryCollection. Composes Point + LineString.
pub struct WktGeometryCollection;

impl RandomValue for WktGeometryCollection {
    fn next_value(random: &mut Random) -> String {
        let p: geo_types::Point<f64> = Faker.fake_with_rng(&mut random.rng);
        let ls_wkt = WktLineString::next_value(random);
        format!("GEOMETRYCOLLECTION({}, {})", wkt_point(&p), ls_wkt)
    }
}

impl ConstantValues for WktGeometryCollection {
    fn next_values() -> Vec<String> {
        [
            "GEOMETRYCOLLECTION(POINT(0 0))",                                                                                   // single element (minimum)
            "GEOMETRYCOLLECTION(POINT(1 1), LINESTRING(0 0, 2 2))",                                                             // point + line
            "GEOMETRYCOLLECTION(POINT(0 0), LINESTRING(0 0, 1 1), POLYGON((0 0, 10 0, 10 10, 0 10, 0 0)))",                     // all basic types
            "GEOMETRYCOLLECTION(POINT(1e15 1e15), LINESTRING(-1e15 -1e15, 1e15 1e15))",                                         // large coordinates
            "GEOMETRYCOLLECTION(POINT(0 0), POINT(1 1), POINT(2 2), LINESTRING(0 0, 1 0), LINESTRING(1 0, 2 0))",               // many elements
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wkt_random_values() {
        let mut random = Random::new(Some(42));
        for _ in 0..5 {
            let point = WktPoint::next_value(&mut random);
            println!("WktPoint: {}", point);
            assert!(point.starts_with("POINT("));

            let linestring = WktLineString::next_value(&mut random);
            println!("WktLineString: {}", linestring);
            assert!(linestring.starts_with("LINESTRING("));

            let polygon = WktPolygon::next_value(&mut random);
            println!("WktPolygon: {}", polygon);
            assert!(polygon.starts_with("POLYGON("));

            let multipoint = WktMultiPoint::next_value(&mut random);
            println!("WktMultiPoint: {}", multipoint);
            assert!(multipoint.starts_with("MULTIPOINT("));

            let multilinestring = WktMultiLineString::next_value(&mut random);
            println!("WktMultiLineString: {}", multilinestring);
            assert!(multilinestring.starts_with("MULTILINESTRING("));

            let multipolygon = WktMultiPolygon::next_value(&mut random);
            println!("WktMultiPolygon: {}", multipolygon);
            assert!(multipolygon.starts_with("MULTIPOLYGON("));

            let geomcollection = WktGeometryCollection::next_value(&mut random);
            println!("WktGeometryCollection: {}", geomcollection);
            assert!(geomcollection.starts_with("GEOMETRYCOLLECTION("));

            println!("---");
        }
    }

    #[test]
    fn test_wkt_constant_values() {
        println!("WktPoint: {:?}", WktPoint::next_values());
        println!("WktLineString: {:?}", WktLineString::next_values());
        println!("WktPolygon: {:?}", WktPolygon::next_values());
        println!("WktMultiPoint: {:?}", WktMultiPoint::next_values());
        println!(
            "WktMultiLineString: {:?}",
            WktMultiLineString::next_values()
        );
        println!("WktMultiPolygon: {:?}", WktMultiPolygon::next_values());
        println!(
            "WktGeometryCollection: {:?}",
            WktGeometryCollection::next_values()
        );
    }
}
