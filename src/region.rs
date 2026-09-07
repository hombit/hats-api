//! The spatial constraint: a shape on the sky, as a structured field rather than as part
//! of the caller's expression.
//!
//! Structured because the constraint has to be *recognised* to be planned on. A named
//! field is recognised by construction; a `cone_contains(…)` call inside `where` would be
//! a pattern match against whatever phrasing the caller happened to use, and an
//! unrecognised one degrades to reading every partition without saying so.
//!
//! Each shape lowers to one boolean expression over two columns of the file, and the array
//! of them is a union: a row inside any shape qualifies. Nothing here prunes partitions —
//! a request names one file today — but the expression is built so that the parts of it
//! that *can* prune are plain comparisons against the coordinate columns, which row-group
//! statistics and the page index both understand.
//!
//! Degrees for every position, and ICRS throughout. Only an extent carries its unit in its
//! name, because only an extent has a second unit anyone would write: a bare `radius`
//! reads as arcseconds to anyone coming from `hats.search.region_search.cone_filter` and
//! as degrees to anyone reading this, and no validation can tell those apart — both are
//! legal radii.

use std::f64::consts::PI;

use datafusion::common::DFSchema;
use datafusion::functions::math::expr_fn::{cos, sin};
use datafusion::logical_expr::Expr;
use datafusion::prelude::lit;
use serde::Deserialize;

use crate::error::ApiError;
use crate::sql;

/// The field these refusals name, which is what the caller wrote in their body.
const FIELD: &str = "region";

/// Degrees to radians, and the half of it a haversine's argument wants.
///
/// For the expression side only, where the operand is a column: a `radians()` call there
/// would be another function in the plan doing what multiplying by a literal already does.
/// The scalar arithmetic uses `f64::to_radians`.
const DEGREE: f64 = PI / 180.0;
const HALF_DEGREE: f64 = PI / 360.0;

/// How far a bound derived from a circle is widened, in degrees.
///
/// Such a bound is exact, and an exact bound is the wrong thing to compare a rounded value
/// against: a row sitting on the boundary would be dropped by the bound before the
/// haversine test it belongs to ever saw it, which is a missing row rather than an error.
/// A nanodegree is four microarcseconds — finer than any astrometry this serves — so
/// widening past the rounding costs nothing measurable.
const PAD: f64 = 1e-9;

/// How close to 1 the sine ratio in [`ra_reach`] may come before its bound is given up on.
///
/// `asin` steepens without limit as its argument approaches 1, so past this the reach it
/// returns carries a rounding error of its own larger than [`PAD`] — and a bound too tight
/// by more than the pad drops rows.
const NEAR_ONE: f64 = 1e-6;

/// One shape on the sky.
#[derive(Debug, Clone, Copy, PartialEq, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase", deny_unknown_fields)]
pub enum Region {
    /// The cone search, under ADQL's name for it: everything within the radius of a point.
    ///
    /// Exactly one of the two radii, which is what keeps the unit unmistakable.
    Circle {
        ra: f64,
        dec: f64,
        radius_deg: Option<f64>,
        radius_arcsec: Option<f64>,
    },
    /// A range in each coordinate — `ra: [349.5, 10.5], dec: [-20, -10]` — inclusive at
    /// both ends.
    ///
    /// Not a shape bounded by great circles, and not ADQL's `BOX`, which takes a centre
    /// with a width and a height and is deprecated in ADQL 2.1 besides. This is the
    /// product of two scalar ranges, which is why it is the cheap shape: it is what
    /// `ra BETWEEN … AND dec BETWEEN …` already says. `hats` and `lsdb` call it a box and
    /// take it this way, so the numbers carry across from `box_search` unchanged.
    ///
    /// **`ra` is directed, not least-to-greatest.** It runs eastward from the first value
    /// to the second, so `[350, 10]` is twenty degrees across the origin and `[10, 350]`
    /// is the three hundred and forty the other way. There is no ordering on a circle for
    /// a `min`/`max` pair to have meant.
    ///
    /// **`dec` is ordered**, first no greater than second. A reversed one is refused: it
    /// has no second reading to be confused with, so it is a mistake rather than a shape,
    /// and answering it with no rows would be indistinguishable from a range that held
    /// none.
    Box { ra: [f64; 2], dec: [f64; 2] },
}

/// A spatial constraint, and the two columns of the file it is tested against.
///
/// The columns are named by the request rather than looked for. A parquet file carries
/// nothing that says which of its columns are a position, so anything else here would be
/// guessing from names — and a guess that picks the wrong pair answers a different question
/// than the one asked, without saying so. A HATS catalog's `properties` does name them, and
/// is where they will come from for a catalog rather than for a lone file.
///
/// The names are not part of a [`Region`]: a shape is geometry and says nothing about a
/// file, and every shape in one request shares the same two columns. Per-shape names would
/// let a request refine one circle against `objra` and another against `ra`, which is not
/// something a caller could mean.
#[derive(Debug, Clone, Copy)]
pub struct Spatial<'a> {
    pub regions: &'a [Region],
    pub ra_column: &'a str,
    pub dec_column: &'a str,
}

/// The union of every shape, as one predicate over the file's coordinate columns.
///
/// The array is a union rather than an intersection, which is worth saying in a refusal
/// too: a caller coming from `where` reads a list as something joined by `AND`.
pub fn predicate(schema: &DFSchema, spatial: &Spatial<'_>) -> Result<Expr, ApiError> {
    if spatial.regions.is_empty() {
        return Err(ApiError::bad_request(format!(
            "{FIELD} is empty, so it constrains nothing; omit it to select every row"
        )));
    }
    let ra = sql::coordinate_column(schema, spatial.ra_column, "ra_column")?;
    let dec = sql::coordinate_column(schema, spatial.dec_column, "dec_column")?;
    spatial
        .regions
        .iter()
        .map(|region| region.predicate(&ra, &dec))
        .reduce(|left, right| Ok(left?.or(right?)))
        .unwrap_or_else(|| unreachable!("the array was checked to be non-empty"))
}

impl Region {
    /// One shape, as a predicate over the two coordinate expressions.
    fn predicate(&self, ra_column: &Expr, dec_column: &Expr) -> Result<Expr, ApiError> {
        match *self {
            Self::Circle {
                ra,
                dec,
                radius_deg,
                radius_arcsec,
            } => {
                finite("ra", ra)?;
                declination("dec", dec)?;
                let radius = radius(radius_deg, radius_arcsec)?;
                Ok(circle(ra_column, dec_column, ra, dec, radius))
            }
            Self::Box {
                ra: [ra_from, ra_to],
                dec: [dec_from, dec_to],
            } => {
                finite("ra", ra_from)?;
                finite("ra", ra_to)?;
                declination("dec", dec_from)?;
                declination("dec", dec_to)?;
                if dec_from > dec_to {
                    return Err(ApiError::bad_request(format!(
                        "{FIELD}: dec runs from the first value to the second, so the \
                         first cannot be the greater of the two"
                    )));
                }
                let span = eastward_span(ra_from, ra_to)?;
                Ok(sky_box(
                    ra_column, dec_column, ra_from, span, dec_from, dec_to,
                ))
            }
        }
    }
}

/// The one radius, in degrees. Exactly one of the two spellings, so the unit is never
/// something a reader has to decide.
fn radius(degrees: Option<f64>, arcseconds: Option<f64>) -> Result<f64, ApiError> {
    let radius = match (degrees, arcseconds) {
        (Some(_), Some(_)) => {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: radius_deg and radius_arcsec are two ways of saying the same \
                 thing; send one of them"
            )));
        }
        (Some(degrees), None) => degrees,
        (None, Some(arcseconds)) => arcseconds / 3600.0,
        (None, None) => {
            return Err(ApiError::bad_request(format!(
                "{FIELD}: a circle needs a radius, as radius_deg or radius_arcsec"
            )));
        }
    };
    if !radius.is_finite() || radius <= 0.0 || radius > 180.0 {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: a radius is greater than 0 and at most 180 degrees"
        )));
    }
    Ok(radius)
}

/// How far eastward the arc from `from` to `to` runs, in degrees.
///
/// The two naming the same point is refused rather than read. `hats` reads it as every
/// right ascension; the other reading is an empty box; the two differ by a whole turn, and
/// nothing in the answer would tell a caller which they got. A caller who meant everything
/// writes a whole turn — `[0, 360]`.
fn eastward_span(from: f64, to: f64) -> Result<f64, ApiError> {
    // Before the wrap, so that a whole turn is the whole circle rather than nothing.
    if to - from >= 360.0 {
        return Ok(360.0);
    }
    let span = (to - from).rem_euclid(360.0);
    if span == 0.0 {
        return Err(ApiError::bad_request(format!(
            "{FIELD}: ra runs eastward from the first value to the second, and these are \
             the same point on the sky; write [0, 360] for every right ascension"
        )));
    }
    Ok(span)
}

/// Everything within `radius` degrees of `(ra, dec)`.
///
/// Two parts, and only the first is the answer. The haversine test is exact and reads both
/// coordinate columns of every row it is given; the coordinate ranges around it contain the
/// circle, so they are what lets row-group statistics and the page index throw row groups
/// away before the trigonometry runs over any of their rows.
fn circle(ra_column: &Expr, dec_column: &Expr, ra: f64, dec: f64, radius: f64) -> Expr {
    // The haversine of the separation, compared as a haversine rather than as an angle.
    // Haversine is monotone in the separation across the whole range an angular distance
    // can take, so recovering the angle with an `asin` would be one more function evaluated
    // per row, and one more rounding, for the same set of rows. It also makes the right
    // ascension need no wrapping: the difference enters as `sin²(Δ/2)`, which a whole turn
    // leaves alone, so a file writing `[0, 360)` and one writing `[-180, 180)` give the
    // same answer with no arithmetic on the column.
    //
    // Each literal is an `f64`, so a `Float32` column is widened before any of this runs
    // rather than the trigonometry being done at the column's precision.
    let hav_dec = squared(sin((dec_column.clone() - lit(dec)) * lit(HALF_DEGREE)));
    let hav_ra = squared(sin((ra_column.clone() - lit(ra)) * lit(HALF_DEGREE)));
    let separation =
        hav_dec + lit(dec.to_radians().cos()) * cos(dec_column.clone() * lit(DEGREE)) * hav_ra;
    // Inclusive, so a point exactly the radius away is inside.
    let exact = separation.lt_eq(lit((radius * HALF_DEGREE).sin().powi(2)));

    let reach = ra_reach(dec, radius);
    let bounds = [
        declination_within(dec_column, dec - radius - PAD, dec + radius + PAD),
        // Symmetric about `ra`, and passed as the range itself so that the two endpoints
        // are the numbers they should be rather than a centre taken apart again.
        right_ascension_within(ra_column, ra - reach, 2.0 * reach),
    ];
    bounds.into_iter().flatten().fold(exact, Expr::and)
}

/// The greatest offset in right ascension anywhere in a disk of angular radius `radius`
/// about `dec`. 180 means there is nothing to bound.
///
/// `sin(reach) = sin(radius) / cos(dec)`, which is exact rather than an approximation.
/// Maximizing the offset over the disk puts the extreme at `sin(dec) = sin(dec₀)/cos(r)`,
/// and substituting that back leaves `cos(reach) = √(cos²r − sin²dec₀) / cos dec₀` — the
/// same angle, since `cos²r − sin²δ` and `cos²δ − sin²r` are both `1 − sin²r − sin²δ`. The
/// maximum is on the boundary rather than inside because right ascension has no interior
/// critical point anywhere it is defined, and the one place it is not defined is a pole.
fn ra_reach(dec: f64, radius: f64) -> f64 {
    // A disk reaching a pole covers every right ascension, since that is where they all
    // meet. This is also the branch that keeps a radius past 90 out of the ratio below,
    // where `sin` has turned back down and would report a reach of almost nothing for a
    // disk covering almost everything.
    if dec.abs() + radius >= 90.0 {
        return 180.0;
    }
    let ratio = radius.to_radians().sin() / dec.to_radians().cos();
    // A disk whose boundary passes close to a pole, where the reach is nearly a quarter
    // turn either way and worth little as a bound — so giving it up costs less than
    // trusting an `asin` this steep. See [`NEAR_ONE`].
    if ratio > 1.0 - NEAR_ONE {
        return 180.0;
    }
    ratio.asin().to_degrees() + PAD
}

/// A range in each coordinate, inclusive at both ends.
///
/// Both halves are the shape itself rather than a bound around it, so neither is padded:
/// widening one would return rows the caller did not ask for.
fn sky_box(
    ra_column: &Expr,
    dec_column: &Expr,
    ra_from: f64,
    ra_span: f64,
    dec_from: f64,
    dec_to: f64,
) -> Expr {
    let bounds = [
        declination_within(dec_column, dec_from, dec_to),
        right_ascension_within(ra_column, ra_from, ra_span),
    ];
    // A box spanning both coordinates fully is the whole sphere, and has nothing to test.
    bounds
        .into_iter()
        .flatten()
        .reduce(Expr::and)
        .unwrap_or(lit(true))
}

/// `from <= dec <= to`, or `None` when the range leaves no declination out.
fn declination_within(column: &Expr, from: f64, to: f64) -> Option<Expr> {
    (from > -90.0 || to < 90.0).then(|| {
        column
            .clone()
            .between(lit(from.max(-90.0)), lit(to.min(90.0)))
    })
}

/// Right ascension on the arc running eastward from `from` through `span` degrees, or
/// `None` when that is the whole circle.
fn right_ascension_within(column: &Expr, from: f64, span: f64) -> Option<Expr> {
    arcs(from, span)
        .into_iter()
        .map(|(low, high)| column.clone().between(lit(low), lit(high)))
        .reduce(Expr::or)
}

/// The closed ranges of raw values an arc covers: eastward from `from` through `span`
/// degrees. Empty means every value is inside it.
///
/// Three ranges rather than one, because which numbers a file writes for a given angle is
/// not something this service gets to assume: the same point on the sky is `350` where
/// right ascension runs `[0, 360)` and `-10` where it runs `[-180, 180)`, and an arc across
/// the origin is two ranges under either. Matching the arc at each whole turn either side
/// of the one it was written in covers both conventions and covers the wrap, without any
/// arithmetic on the column — a `%` there would be correct and would also stop the
/// comparison pruning anything.
fn arcs(from: f64, span: f64) -> Vec<(f64, f64)> {
    if span >= 360.0 {
        return Vec::new();
    }
    let from = from.rem_euclid(360.0);
    (-1..=1)
        .map(|turn| {
            let turn = f64::from(turn) * 360.0;
            (from + turn, from + span + turn)
        })
        .collect()
}

/// `expr * expr`. DataFusion's common-subexpression pass is what keeps the operand from
/// being evaluated twice, and writing it this way needs no `power` in the expression.
fn squared(expr: Expr) -> Expr {
    expr.clone() * expr
}

fn finite(name: &str, value: f64) -> Result<(), ApiError> {
    match value.is_finite() {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "{FIELD}: {name} must be a number of degrees"
        ))),
    }
}

fn declination(name: &str, value: f64) -> Result<(), ApiError> {
    finite(name, value)?;
    match (-90.0..=90.0).contains(&value) {
        true => Ok(()),
        false => Err(ApiError::bad_request(format!(
            "{FIELD}: {name} is a declination in degrees, so it lies between -90 and 90"
        ))),
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float32Array, Float64Array, Int64Array, RecordBatch};
    use datafusion::parquet::arrow::ArrowWriter;

    use super::*;
    use crate::query::{self, Order, Predicate, Projection, Selection};
    use crate::storage::RemoteFile;

    /// How a file writes right ascension.
    ///
    /// A parameter because a file's convention is not something this service gets to
    /// assume, and a predicate that only works for one of them silently drops the rows of
    /// the other.
    #[derive(Debug, Clone, Copy)]
    enum Convention {
        /// `[0, 360)`, which is what HATS writes.
        Positive,
        /// `[-180, 180)`.
        Signed,
    }

    /// What type a file's coordinate columns are.
    ///
    /// `Float32` is ordinary in a catalog — a position needs nowhere near `f64` for many
    /// surveys, and halving the column is worth it. Every literal in the expressions above
    /// is an `f64`, so the column is widened before the arithmetic rather than the
    /// arithmetic being done at single precision; this is what checks that.
    #[derive(Debug, Clone, Copy)]
    enum Precision {
        F64,
        F32,
    }

    /// A fixture: where the points are written and how.
    #[derive(Debug, Clone, Copy)]
    struct Fixture {
        convention: Convention,
        precision: Precision,
    }

    impl Fixture {
        /// Every combination, since a region has to answer the same way over all of them.
        const ALL: [Self; 4] = [
            Self {
                convention: Convention::Positive,
                precision: Precision::F64,
            },
            Self {
                convention: Convention::Positive,
                precision: Precision::F32,
            },
            Self {
                convention: Convention::Signed,
                precision: Precision::F64,
            },
            Self {
                convention: Convention::Signed,
                precision: Precision::F32,
            },
        ];

        /// A value as this file will hold it.
        ///
        /// The rounding is applied to what the reference below compares against as well as
        /// to the file, so the two are asked about the same numbers. Otherwise every
        /// single-precision case would differ from the reference by the rounding rather
        /// than by anything the predicate did.
        #[expect(
            clippy::cast_possible_truncation,
            reason = "narrowing to f32 is what this is for, and the value is read back \
                      widened so the reference and the file agree on it"
        )]
        fn stored(self, value: f64) -> f64 {
            match self.precision {
                Precision::F64 => value,
                Precision::F32 => f64::from(value as f32),
            }
        }

        fn right_ascension(self, ra: f64) -> f64 {
            self.stored(match self.convention {
                Convention::Signed if ra >= 180.0 => ra - 360.0,
                _ => ra,
            })
        }

        /// The points as this file holds them: an id, a right ascension in the file's own
        /// convention, and a declination.
        fn points(self) -> Vec<(i64, f64, f64)> {
            sky()
                .into_iter()
                .map(|(id, ra, dec)| (id, self.right_ascension(ra), self.stored(dec)))
                .collect()
        }

        /// The points as a parquet file.
        ///
        /// The coordinate columns are mixed-case on purpose: they are resolved the same way
        /// any other column name is, and a fixture spelling them in lowercase would not
        /// notice if that stopped being true.
        fn on_disk(self) -> (tempfile::TempDir, RemoteFile) {
            let points = self.points();
            let column = |values: Vec<f64>| -> ArrayRef {
                match self.precision {
                    Precision::F64 => Arc::new(Float64Array::from(values)),
                    #[expect(
                        clippy::cast_possible_truncation,
                        reason = "every value here has already been through `stored`, so \
                                  narrowing it back is exact"
                    )]
                    Precision::F32 => Arc::new(Float32Array::from_iter_values(
                        values.into_iter().map(|value| value as f32),
                    )),
                }
            };
            let batch = RecordBatch::try_from_iter_with_nullable([
                (
                    "objectid",
                    Arc::new(Int64Array::from_iter_values(
                        points.iter().map(|(id, _, _)| *id),
                    )) as ArrayRef,
                    false,
                ),
                (
                    "objRA",
                    column(points.iter().map(|(_, ra, _)| *ra).collect()),
                    true,
                ),
                (
                    "objDec",
                    column(points.iter().map(|(_, _, dec)| *dec).collect()),
                    true,
                ),
            ])
            .unwrap();

            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("part0.parquet");
            let file = std::fs::File::create(&path).unwrap();
            let mut writer = ArrowWriter::try_new(file, batch.schema(), None).unwrap();
            writer.write(&batch).unwrap();
            writer.close().unwrap();
            (dir, crate::storage::open_mounted(&path).unwrap())
        }

        /// The ids these regions select, according to the service.
        async fn selected(self, regions: &[Region]) -> Vec<i64> {
            let (_dir, file) = self.on_disk();
            ids(&file, regions, "objRA", "objDec").await.unwrap()
        }

        /// The same question answered here, in a few lines of arithmetic per point, so that
        /// what the planned expression returns is checked against the formulas rather than
        /// against a recorded answer.
        ///
        /// This is also the only thing checking the coordinate bounds a circle carries.
        /// They are an optimization, ANDed onto an exact test, so a bound a hair too tight
        /// does not fail — it drops rows near the edge and returns a smaller answer than
        /// the formula asked for.
        fn reference(self, regions: &[Region]) -> Vec<i64> {
            self.points()
                .into_iter()
                .filter(|(_, ra, dec)| regions.iter().any(|region| holds(region, *ra, *dec)))
                .map(|(id, _, _)| id)
                .collect()
        }
    }

    /// Whether a point is in a region, worked out here rather than shared with the code
    /// under test.
    fn holds(region: &Region, ra: f64, dec: f64) -> bool {
        match *region {
            Region::Circle {
                ra: ra0,
                dec: dec0,
                radius_deg,
                radius_arcsec,
            } => {
                let radius = radius(radius_deg, radius_arcsec).unwrap();
                // The haversine formula, written out.
                let hav = ((dec - dec0) * HALF_DEGREE).sin().powi(2)
                    + dec0.to_radians().cos()
                        * dec.to_radians().cos()
                        * ((ra - ra0) * HALF_DEGREE).sin().powi(2);
                hav <= (radius * HALF_DEGREE).sin().powi(2)
            }
            Region::Box {
                ra: [from, to],
                dec: [low, high],
            } => {
                let span = eastward_span(from, to).unwrap();
                let offset = (ra - from).rem_euclid(360.0);
                (low..=high).contains(&dec) && (span >= 360.0 || offset <= span)
            }
        }
    }

    /// Points every region below is meant to select at least one of, so that a region
    /// selecting nothing is a failure rather than a case that checked nothing.
    ///
    /// All of them are somewhere a spatial predicate goes wrong: either side of the
    /// right-ascension origin, both poles, and just short of a pole.
    const PLANTED: [(f64, f64); 10] = [
        (0.0, 0.0),
        (0.05, 10.0),
        (359.95, 10.0),
        (180.0, 0.0),
        (0.0, 90.0),
        (0.0, -90.0),
        (123.4, 89.99),
        (89.2, 89.3),
        (200.0, 45.0),
        (320.65747, -12.35315),
    ];

    /// The planted points, and a scatter over the whole sphere around them. Right ascension
    /// is in `[0, 360)` here; what a file writes is the fixture's doing.
    fn sky() -> Vec<(i64, f64, f64)> {
        let mut points = Vec::new();
        for i in 0..2_000i64 {
            let mixed = i.cast_unsigned().wrapping_mul(0x9E37_79B9_7F4A_7C15);
            let unit = |shift: u32| {
                let bits = u32::try_from((mixed >> shift) & 0xFFFF_FFFF).unwrap();
                f64::from(bits) / f64::from(u32::MAX)
            };
            // Uniform in sin(dec), so the poles are sampled as densely as the equator and
            // the declination bounds are exercised where they are tightest. Nothing lands
            // on a whole degree, which is where every region below has its edges.
            points.push((
                i,
                unit(32) * 360.0,
                (unit(0) * 2.0 - 1.0).asin().to_degrees(),
            ));
        }
        for (offset, (ra, dec)) in PLANTED.into_iter().enumerate() {
            points.push((10_000 + i64::try_from(offset).unwrap(), ra, dec));
        }
        points
    }

    fn limits() -> sql::Limits {
        (&crate::config::LimitsConfig::default()).into()
    }

    /// The `objectid` of every row a region kept, in ascending order.
    async fn ids(
        file: &RemoteFile,
        regions: &[Region],
        ra_column: &str,
        dec_column: &str,
    ) -> Result<Vec<i64>, ApiError> {
        let selection = Selection {
            projection: Projection::Columns("objectid"),
            predicate: Predicate::All,
            spatial: Some(Spatial {
                regions,
                ra_column,
                dec_column,
            }),
            limit: None,
        };
        let result = query::run(file, &selection, limits(), Order::File).await?;
        let mut ids = Vec::new();
        for batch in &result.batches {
            let column = batch.column_by_name("objectid").unwrap();
            let values = column.as_any().downcast_ref::<Int64Array>().unwrap();
            ids.extend(values.values().iter().copied());
        }
        ids.sort_unstable();
        Ok(ids)
    }

    fn circle_at(ra: f64, dec: f64, radius_deg: f64) -> Region {
        Region::Circle {
            ra,
            dec,
            radius_deg: Some(radius_deg),
            radius_arcsec: None,
        }
    }

    fn box_over(ra: [f64; 2], dec: [f64; 2]) -> Region {
        Region::Box { ra, dec }
    }

    /// Every circle worth asking about: away from anything special, across the
    /// right-ascension origin, over each pole, one whose boundary passes near enough to a
    /// pole to reach [`NEAR_ONE`], one small enough to hold a single point, and one holding
    /// the whole sky.
    fn circles() -> Vec<Region> {
        vec![
            circle_at(320.65747, -12.35315, 10.0),
            circle_at(0.0, 10.0, 3.0),
            circle_at(359.5, 10.0, 3.0),
            circle_at(0.0, 89.0, 3.0),
            circle_at(45.0, -89.5, 5.0),
            circle_at(89.2, 89.0, 0.999_999_99),
            circle_at(0.05, 10.0, 0.01),
            circle_at(200.0, 45.0, 120.0),
            circle_at(10.0, 20.0, 180.0),
        ]
    }

    /// And every box: an ordinary one, an arc across the origin, its long-way-round
    /// complement, a polar cap, a narrow polar strip, and the whole sky.
    fn boxes() -> Vec<Region> {
        vec![
            box_over([315.0, 325.0], [-15.0, -10.0]),
            box_over([355.0, 5.0], [5.0, 15.0]),
            box_over([5.0, 355.0], [5.0, 15.0]),
            box_over([0.0, 360.0], [88.0, 90.0]),
            box_over([100.0, 150.0], [89.5, 90.0]),
            box_over([0.0, 360.0], [-90.0, 90.0]),
        ]
    }

    /// The circle's guarantee: the rows the planned expression returns are exactly the rows
    /// the haversine formula selects — over both conventions and both column types.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_circle_selects_what_the_haversine_formula_selects() {
        for fixture in Fixture::ALL {
            for circle in circles() {
                let regions = [circle];
                let expected = fixture.reference(&regions);
                assert!(
                    !expected.is_empty(),
                    "{circle:?} selects nothing over {fixture:?}, so it checks nothing"
                );
                assert_eq!(
                    fixture.selected(&regions).await,
                    expected,
                    "{circle:?} over {fixture:?}"
                );
            }
        }
    }

    /// And the box's: a range in each coordinate, with right ascension read eastward from
    /// the first value to the second.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_box_selects_a_range_in_each_coordinate() {
        for fixture in Fixture::ALL {
            for region in boxes() {
                let regions = [region];
                let expected = fixture.reference(&regions);
                assert!(
                    !expected.is_empty(),
                    "{region:?} selects nothing over {fixture:?}, so it checks nothing"
                );
                assert_eq!(
                    fixture.selected(&regions).await,
                    expected,
                    "{region:?} over {fixture:?}"
                );
            }
        }
    }

    /// `ra` is directed rather than least-to-greatest, so the two orderings of one pair are
    /// two different boxes whose union is the whole band — and neither is empty.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_two_orderings_of_a_right_ascension_range_are_different_boxes() {
        let fixture = Fixture {
            convention: Convention::Positive,
            precision: Precision::F64,
        };
        let dec = [5.0, 15.0];
        let across = fixture.selected(&[box_over([355.0, 5.0], dec)]).await;
        let round = fixture.selected(&[box_over([5.0, 355.0], dec)]).await;
        let band = fixture.selected(&[box_over([0.0, 360.0], dec)]).await;

        assert!(!across.is_empty() && !round.is_empty());
        assert!(across.len() < round.len(), "the short way round is smaller");
        // The two share only the points at exactly 5 and 355, of which the fixture has
        // none, so together they are the band.
        let mut union = [across, round].concat();
        union.sort_unstable();
        union.dedup();
        assert_eq!(union, band);
    }

    /// The array is a union, which is the part a caller coming from `where` will expect to
    /// be an intersection. Two disjoint shapes are the case that says which it is.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_array_of_regions_is_a_union() {
        let fixture = Fixture {
            convention: Convention::Positive,
            precision: Precision::F64,
        };
        let regions = [
            circle_at(320.65747, -12.35315, 10.0),
            box_over([100.0, 150.0], [89.5, 90.0]),
        ];
        let both = fixture.selected(&regions).await;
        let first = fixture.selected(&regions[..1]).await;
        let second = fixture.selected(&regions[1..]).await;

        assert_eq!(both, fixture.reference(&regions));
        assert!(both.len() > first.len() && both.len() > second.len());
    }

    /// The two radii are two spellings of one number, so a circle written each way is the
    /// same circle.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_two_radii_mean_the_same_thing() {
        let fixture = Fixture {
            convention: Convention::Positive,
            precision: Precision::F64,
        };
        let arcseconds = Region::Circle {
            ra: 320.65747,
            dec: -12.35315,
            radius_deg: None,
            radius_arcsec: Some(1800.0),
        };
        let by_degrees = fixture
            .selected(&[circle_at(320.65747, -12.35315, 0.5)])
            .await;
        assert!(!by_degrees.is_empty());
        assert_eq!(fixture.selected(&[arcseconds]).await, by_degrees);
    }

    /// A coordinate column answers to the same names any other column does — the file's
    /// spelling and its lowercase — and a name matching neither is refused rather than
    /// leaving the region silently untested.
    #[tokio::test(flavor = "multi_thread")]
    async fn the_coordinate_columns_are_named_the_way_any_column_is() {
        let (_dir, file) = Fixture {
            convention: Convention::Positive,
            precision: Precision::F64,
        }
        .on_disk();
        let regions = [circle_at(320.65747, -12.35315, 10.0)];

        let rows = ids(&file, &regions, "objRA", "objDec").await.unwrap();
        assert!(!rows.is_empty());
        assert_eq!(ids(&file, &regions, "objra", "objdec").await.unwrap(), rows);
        // Neither the file's spelling nor its lowercase.
        let error = ids(&file, &regions, "OBJRA", "objdec")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("OBJRA"), "{error}");
        assert!(ids(&file, &regions, "objra", "nosuchcolumn").await.is_err());
    }

    /// A column that is not a number cannot hold a coordinate, and saying so is better than
    /// the planner's account of a subtraction it could not type.
    #[tokio::test(flavor = "multi_thread")]
    async fn a_coordinate_column_has_to_be_a_number() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("part0.parquet");
        std::fs::write(&path, query::tests::fixture()).unwrap();
        let file = crate::storage::open_mounted(&path).unwrap();

        let error = ids(&file, &[circle_at(0.0, 0.0, 1.0)], "band", "objectid")
            .await
            .unwrap_err()
            .to_string();
        assert!(error.contains("band"), "{error}");
    }

    /// A schema to check the refusals against, since none of them reaches a file.
    fn schema() -> DFSchema {
        use datafusion::arrow::datatypes::{DataType, Field, Schema};

        DFSchema::try_from(Schema::new(vec![
            Field::new("ra", DataType::Float64, true),
            Field::new("dec", DataType::Float64, true),
        ]))
        .unwrap()
    }

    fn plan(regions: &[Region]) -> Result<Expr, ApiError> {
        predicate(
            &schema(),
            &Spatial {
                regions,
                ra_column: "ra",
                dec_column: "dec",
            },
        )
    }

    fn refuse(region: Region) -> String {
        plan(&[region]).unwrap_err().to_string()
    }

    /// A shape that describes no region is the caller's mistake. Every one of these would
    /// otherwise be an expression quietly matching everything or nothing.
    #[test]
    fn a_shape_that_is_not_one_is_refused() {
        assert!(refuse(circle_at(0.0, 91.0, 1.0)).contains("dec"));
        assert!(refuse(circle_at(0.0, -90.5, 1.0)).contains("dec"));
        assert!(refuse(circle_at(f64::INFINITY, 0.0, 1.0)).contains("ra"));
        assert!(refuse(circle_at(0.0, 0.0, 0.0)).contains("radius"));
        assert!(refuse(circle_at(0.0, 0.0, -1.0)).contains("radius"));
        assert!(refuse(circle_at(0.0, 0.0, 181.0)).contains("radius"));

        // A dec range runs first to second, and reversed has no second reading.
        assert!(refuse(box_over([0.0, 10.0], [20.0, 10.0])).contains("dec"));
        assert!(refuse(box_over([0.0, 10.0], [-91.0, 10.0])).contains("dec"));

        // And an empty array, which would otherwise be a request whose spatial constraint
        // went missing.
        let error = plan(&[]).unwrap_err().to_string();
        assert!(error.contains(FIELD), "{error}");
    }

    /// Exactly one radius. Both is a caller who thinks they differ; neither has no default,
    /// since a circle of an assumed size is a wrong answer rather than an error.
    #[test]
    fn a_circle_takes_exactly_one_radius() {
        let circle = |radius_deg, radius_arcsec| Region::Circle {
            ra: 0.0,
            dec: 0.0,
            radius_deg,
            radius_arcsec,
        };
        assert!(refuse(circle(Some(1.0), Some(3600.0))).contains("one of them"));
        assert!(refuse(circle(None, None)).contains("radius_deg or radius_arcsec"));
        assert!(refuse(circle(None, Some(0.0))).contains("radius"));
    }

    /// The two ends of a right-ascension range naming the same point is refused rather than
    /// read. `hats` reads it as every right ascension; the other reading is an empty box;
    /// the two differ by a whole turn and the answer would not say which it was.
    #[test]
    fn a_right_ascension_range_of_no_width_is_refused() {
        // The same point named twice, however it is spelled: `[350, -10]` runs a whole turn
        // back to where it started.
        for range in [[10.0, 10.0], [0.0, 0.0], [350.0, -10.0]] {
            let error = refuse(box_over(range, [-10.0, 10.0]));
            assert!(error.contains("[0, 360]"), "{range:?}: {error}");
        }
        // A whole turn forward is every right ascension, which is what a caller who meant
        // everything writes — from any starting point, not only from zero.
        for range in [[0.0, 360.0], [10.0, 370.0], [-180.0, 180.0]] {
            assert!(plan(&[box_over(range, [-10.0, 10.0])]).is_ok(), "{range:?}");
        }
    }

    /// An arc is matched at each whole turn either side of the one it was written in, which
    /// is what makes it independent of the file's convention and of the wrap.
    #[test]
    fn an_arc_covers_every_spelling_of_its_angles() {
        assert!(arcs(0.0, 360.0).is_empty());
        assert_eq!(
            arcs(330.0, 40.0),
            [(-30.0, 10.0), (330.0, 370.0), (690.0, 730.0)]
        );
        // A start outside `[0, 360)` names the same arc as its normalized form.
        assert_eq!(arcs(-30.0, 40.0), arcs(330.0, 40.0));
    }

    /// The ra half-width of a disk, against the largest offset actually on its boundary.
    ///
    /// A point at angular distance `radius` from the centre along position angle `theta`
    /// has `tan(offset) = sin r sin θ / (cos dec₀ cos r − sin dec₀ sin r cos θ)`, which
    /// shares nothing with the closed form under test and goes through `atan2` rather than
    /// `asin`, so it stays well conditioned where the closed form does not.
    ///
    /// The extreme is evaluated at the position angle it is known to be at rather than
    /// searched for. Near a pole the peak is narrow enough that a scan of a hundred
    /// thousand steps walks straight past it, and a scan that misses the peak reports a
    /// reach the bound comfortably contains — which is a test that passes with the
    /// [`NEAR_ONE`] guard removed. `cos θ = tan dec₀ tan radius` is that angle, from
    /// substituting the extremal declination back into the circle.
    #[test]
    fn the_ra_reach_of_a_disk_contains_its_boundary() {
        let boundary_reach = |dec0: f64, radius: f64| {
            let (dec0, radius) = (dec0.to_radians(), radius.to_radians());
            let offset = |theta: f64| {
                let y = radius.sin() * theta.sin();
                let x = dec0.cos() * radius.cos() - dec0.sin() * radius.sin() * theta.cos();
                y.atan2(x).abs()
            };
            let peak = (dec0.tan() * radius.tan()).clamp(-1.0, 1.0).acos();
            let mut worst = offset(peak);
            for step in 0..10_000 {
                worst = worst.max(offset(2.0 * PI * f64::from(step) / 10_000.0));
            }
            worst.to_degrees()
        };

        for (dec0, radius) in [
            (0.0, 10.0),
            (60.0, 10.0),
            (80.0, 5.0),
            (89.0, 0.5),
            (89.9, 0.09),
            (-89.0, 0.99),
            // Where `asin` is too steep to trust, and the bound is given up on. Without
            // that, these are the cases whose bound falls short by more than `PAD` — which
            // is rows dropped, since the bound is ANDed onto an exact test.
            (89.0, 0.999_999_999_999),
            (-89.0, 0.999_999_999_999),
            (45.0, 44.999_999_999_999),
            (10.0, 79.999_999_999_999),
            // Reaching a pole, where every right ascension is inside.
            (89.0, 1.0),
            (0.0, 90.0),
            (10.0, 179.0),
        ] {
            let reach = ra_reach(dec0, radius);
            let boundary = boundary_reach(dec0, radius);
            assert!(
                reach >= boundary,
                "dec {dec0}, radius {radius}: bounded {reach} but the boundary reaches \
                 {boundary}, short by {:.3e} degrees",
                boundary - reach
            );
        }
    }

    /// The shapes are read from a caller's JSON, so what that JSON may say is part of the
    /// contract: a `type` that is not one, a misspelled field, a range of the wrong length
    /// and a radius with no unit in its name are each refused rather than defaulted.
    #[test]
    fn a_region_is_read_from_json_exactly() {
        let region = |json: serde_json::Value| serde_json::from_value::<Region>(json);

        assert_eq!(
            region(serde_json::json!({
                "type": "circle", "ra": 1.0, "dec": 2.0, "radius_deg": 3.0,
            }))
            .unwrap(),
            circle_at(1.0, 2.0, 3.0)
        );
        assert_eq!(
            region(serde_json::json!({
                "type": "box", "ra": [349.5, 10.5], "dec": [-20.0, -10.0],
            }))
            .unwrap(),
            box_over([349.5, 10.5], [-20.0, -10.0])
        );

        for wrong in [
            // A shape this service does not have.
            serde_json::json!({"type": "polygon", "vertices": []}),
            // ADQL's BOX, which is a centre with extents and is not what this takes.
            serde_json::json!({
                "type": "box", "ra": 10.0, "dec": 20.0, "width": 1.0, "height": 1.0,
            }),
            // A field that is not one of the shape's.
            serde_json::json!({
                "type": "circle", "ra": 1.0, "dec": 2.0, "radius_deg": 3.0, "radus": 4.0,
            }),
            // A radius with no unit in its name.
            serde_json::json!({"type": "circle", "ra": 1.0, "dec": 2.0, "radius": 3.0}),
            // A range that is not a pair.
            serde_json::json!({"type": "box", "ra": [1.0], "dec": [-1.0, 1.0]}),
            serde_json::json!({"type": "box", "ra": [1.0, 2.0, 3.0], "dec": [-1.0, 1.0]}),
            // A frame, which this service has no transformation for: everything is ICRS.
            serde_json::json!({
                "type": "circle", "ra": 1.0, "dec": 2.0, "radius_deg": 3.0,
                "frame": "galactic",
            }),
        ] {
            assert!(region(wrong.clone()).is_err(), "{wrong}");
        }
    }
}
