use crate::test_runner::mock_utils::constants::ConstantValues;
use crate::test_runner::mock_utils::random::{Random, RandomValue};
use fake::{Fake, Faker};

/// PostgreSQL point: (x,y)
pub struct Point(pub geo_types::Point<f64>);

impl std::fmt::Display for Point {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "({},{})", self.0.x(), self.0.y())
    }
}

impl RandomValue for Point {
    fn next_value(random: &mut Random) -> String {
        Point(Faker.fake_with_rng(&mut random.rng)).to_string()
    }
}

impl ConstantValues for Point {
    fn next_values() -> Vec<String> {
        [
            "(0,0)",                 // origin
            "(1,1)",                 // unit point
            "(-1,-1)",               // negative coordinates
            "(1e10,1e10)",           // large values
            "(1e-10,1e-10)",         // small values
            "(Infinity,Infinity)",   // positive infinity
            "(-Infinity,-Infinity)", // negative infinity
            "(NaN,NaN)",             // not a number
            "(Infinity,-Infinity)",  // mixed infinity
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL line: {A,B,C} represents Ax + By + C = 0
/// Defined by two points, outputs as {A,B,C}
pub struct Line(pub geo_types::Line<f64>);

impl std::fmt::Display for Line {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Given two points (x1,y1) and (x2,y2), compute A, B, C for Ax + By + C = 0
        let (x1, y1) = (self.0.start.x, self.0.start.y);
        let (x2, y2) = (self.0.end.x, self.0.end.y);
        let a = y2 - y1;
        let b = x1 - x2;
        let c = x2 * y1 - x1 * y2;
        write!(f, "{{{},{},{}}}", a, b, c)
    }
}

impl RandomValue for Line {
    fn next_value(random: &mut Random) -> String {
        Line(Faker.fake_with_rng(&mut random.rng)).to_string()
    }
}

impl ConstantValues for Line {
    fn next_values() -> Vec<String> {
        [
            "{0,-1,0}",       // horizontal line y = 0
            "{1,0,0}",        // vertical line x = 0
            "{1,-1,0}",       // diagonal line y = x
            "{1,1,0}",        // diagonal line y = -x
            "{0,-1,5}",       // horizontal line y = 5
            "{Infinity,0,0}", // infinite coefficient A
            "{0,Infinity,0}", // infinite coefficient B
            "{0,0,Infinity}", // infinite coefficient C
            "{NaN,NaN,NaN}",  // NaN coefficients
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL line segment (lseg): [(x1,y1),(x2,y2)]
pub struct LineSegment(pub geo_types::Line<f64>);

impl std::fmt::Display for LineSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "[({},{}),({},{})]",
            self.0.start.x, self.0.start.y, self.0.end.x, self.0.end.y
        )
    }
}

impl RandomValue for LineSegment {
    fn next_value(random: &mut Random) -> String {
        LineSegment(Faker.fake_with_rng(&mut random.rng)).to_string()
    }
}

impl ConstantValues for LineSegment {
    fn next_values() -> Vec<String> {
        [
            "[(0,0),(0,0)]",                               // zero-length segment
            "[(0,0),(1,1)]",                               // unit segment
            "[(-1,-1),(1,1)]",                             // segment crossing origin
            "[(0,0),(1e10,1e10)]",                         // large segment
            "[(0,0),(Infinity,Infinity)]",                 // infinite endpoint
            "[(-Infinity,-Infinity),(Infinity,Infinity)]", // infinite span
            "[(NaN,NaN),(NaN,NaN)]",                       // NaN segment
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL box: (x1,y1),(x2,y2) - opposite corners
pub struct Box(pub geo_types::Rect<f64>);

impl std::fmt::Display for Box {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let min = self.0.min();
        let max = self.0.max();
        write!(f, "({},{}),({},{})", max.x, max.y, min.x, min.y)
    }
}

impl RandomValue for Box {
    fn next_value(random: &mut Random) -> String {
        Box(Faker.fake_with_rng(&mut random.rng)).to_string()
    }
}

impl ConstantValues for Box {
    fn next_values() -> Vec<String> {
        [
            "(0,0),(0,0)",                               // zero-area box
            "(1,1),(0,0)",                               // unit box
            "(1,1),(-1,-1)",                             // symmetric box
            "(1e10,1e10),(-1e10,-1e10)",                 // large box
            "(Infinity,Infinity),(-Infinity,-Infinity)", // infinite box
            "(NaN,NaN),(NaN,NaN)",                       // NaN box
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL path: [(x1,y1),...] (open) or ((x1,y1),...) (closed)
pub struct Path {
    pub points: geo_types::LineString<f64>,
    pub closed: bool,
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (open, close) = if self.closed { ('(', ')') } else { ('[', ']') };
        write!(f, "{}", open)?;
        let coords: Vec<String> = self
            .points
            .coords()
            .map(|c| format!("({},{})", c.x, c.y))
            .collect();
        write!(f, "{}", coords.join(","))?;
        write!(f, "{}", close)
    }
}

impl RandomValue for Path {
    fn next_value(random: &mut Random) -> String {
        let points: geo_types::LineString<f64> = Faker.fake_with_rng(&mut random.rng);
        let closed: bool = Faker.fake_with_rng(&mut random.rng);
        Path { points, closed }.to_string()
    }
}

impl ConstantValues for Path {
    fn next_values() -> Vec<String> {
        [
            "[(0,0),(1,0),(1,1)]",           // open path
            "((0,0),(1,0),(1,1))",           // closed path
            "[(0,0),(1,1),(2,0),(3,1)]",     // multi-point open path
            "((-1,-1),(1,-1),(1,1),(-1,1))", // closed rectangular path
            "[(0,0),(Infinity,Infinity)]",   // path to infinity
            "[(NaN,NaN),(NaN,NaN)]",         // NaN path
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL polygon: ((x1,y1),...)
pub struct Polygon(pub geo_types::Polygon<f64>);

impl std::fmt::Display for Polygon {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "(")?;
        let coords: Vec<String> = self
            .0
            .exterior()
            .coords()
            .map(|c| format!("({},{})", c.x, c.y))
            .collect();
        write!(f, "{}", coords.join(","))?;
        write!(f, ")")
    }
}

impl RandomValue for Polygon {
    fn next_value(random: &mut Random) -> String {
        // geo_types::Polygon will auto-close the LineString
        Polygon(Faker.fake_with_rng(&mut random.rng)).to_string()
    }
}

impl ConstantValues for Polygon {
    fn next_values() -> Vec<String> {
        [
            "((0,0),(1,0),(0.5,1))",             // triangle
            "((0,0),(1,0),(1,1),(0,1))",         // square
            "((-1,-1),(1,-1),(1,1),(-1,1))",     // symmetric square
            "((0,0),(Infinity,0),(0,Infinity))", // infinite polygon
            "((NaN,NaN),(NaN,NaN),(NaN,NaN))",   // NaN polygon
        ]
        .iter()
        .map(|s| s.to_string())
        .collect()
    }
}

/// PostgreSQL circle: <(x,y),r>
pub struct Circle {
    pub center: geo_types::Point<f64>,
    pub radius: f64,
}

impl std::fmt::Display for Circle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "<({},{}),{}>",
            self.center.x(),
            self.center.y(),
            self.radius
        )
    }
}

impl RandomValue for Circle {
    fn next_value(random: &mut Random) -> String {
        let center: geo_types::Point<f64> = Faker.fake_with_rng(&mut random.rng);
        let radius: f64 = Faker.fake_with_rng::<f64, _>(&mut random.rng).abs();
        Circle { center, radius }.to_string()
    }
}

impl ConstantValues for Circle {
    fn next_values() -> Vec<String> {
        [
            "<(0,0),0>",               // zero-radius circle (point)
            "<(0,0),1>",               // unit circle
            "<(1,1),1>",               // offset center
            "<(0,0),1e10>",            // large radius
            "<(-1,-1),0.5>",           // negative center coordinates
            "<(0,0),Infinity>",        // infinite radius
            "<(Infinity,Infinity),1>", // infinite center
            "<(NaN,NaN),NaN>",         // NaN circle
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
    fn test_all_next_values() {
        let mut random = Random::new(None);

        for _ in 0..5 {
            let point = Point::next_value(&mut random);
            println!("Point: {}", point);
            assert!(point.starts_with('(') && point.ends_with(')'));

            let line = Line::next_value(&mut random);
            println!("Line: {}", line);
            assert!(line.starts_with('{') && line.ends_with('}'));

            let lseg = LineSegment::next_value(&mut random);
            println!("LineSegment: {}", lseg);
            assert!(lseg.starts_with('[') && lseg.ends_with(']'));

            let box_val = Box::next_value(&mut random);
            println!("Box: {}", box_val);
            assert!(box_val.starts_with('(') && box_val.ends_with(')'));

            let path = Path::next_value(&mut random);
            println!("Path: {}", path);
            // open path starts with '[', closed path starts with '('
            assert!(path.starts_with('[') || path.starts_with('('));

            let polygon = Polygon::next_value(&mut random);
            println!("Polygon: {}", polygon);
            assert!(polygon.starts_with('(') && polygon.ends_with(')'));

            let circle = Circle::next_value(&mut random);
            println!("Circle: {}", circle);
            assert!(circle.starts_with('<') && circle.ends_with('>'));

            println!("---");
        }
    }

    #[test]
    fn test_all_constant_values() {
        println!("Point constants: {:?}", Point::next_values());
        println!("Line constants: {:?}", Line::next_values());
        println!("LineSegment constants: {:?}", LineSegment::next_values());
        println!("Box constants: {:?}", Box::next_values());
        println!("Path constants: {:?}", Path::next_values());
        println!("Polygon constants: {:?}", Polygon::next_values());
        println!("Circle constants: {:?}", Circle::next_values());
    }
}
