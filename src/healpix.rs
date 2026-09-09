//! A shape on the sky as HEALPix cells: which partitions of a catalog it can touch, and
//! which rows of one it cannot.
//!
//! **Two jobs, and they are not one covering at two scales — they are two coverings.**
//! Against a catalog, a covering says which partitions are wholly inside the region, which
//! the boundary crosses, and which are outside: the first need no spatial test at all and
//! the last need not be opened. That is answered once per partition, so how many cells it
//! took does not matter, and the depth follows the catalog's own order. Against the rows of
//! one partition, a covering becomes an expression every row is evaluated against, so what
//! matters is how short it is: the depth follows the size of the shape, and never goes
//! coarser than the partition it is filtering — a region far larger than a partition,
//! described at its own scale, has nothing to say about which part of that partition to
//! read. `Detail` is which is being asked for.
//!
//! **The covering is never exact, and both of its errors are on the safe side.** The outer
//! set is a superset of the region, so a cell it rejects really is outside; the inner set is
//! a subset, so a cell it accepts really is inside. Everything between the two is decided by
//! the geometry, which is the answer. That is what lets the covering be coarse, and coarse
//! is what makes it cheap.
//!
//! The two answer different questions, and only one of them is about being right. **The
//! outer set is what keeps the filter correct** — it contains the region, so nothing it
//! rejects was wanted, and `outer AND exact` on its own is a complete filter. The inner set
//! never removes a row; it only says which rows need no geometry, so a cell inside it is a
//! cell whose rows are answered by the covering alone. Against a catalog that is decisive: a
//! partition wholly inside the region is read with no spatial predicate at all.
//!
//! **The covering depth is never 29**, whatever the HEALPix column is written at. A covering
//! fine enough to be exact along a boundary is enormous — a one-degree circle's boundary is
//! of order 10⁷ cells at order 29 — and a row tested against thousands of ranges costs more
//! than the trigonometry that was being saved.
//!
//! `cdshealpix` computes the coverings and `moc` holds them; neither is reimplemented here.

use std::f64::consts::{FRAC_PI_2, PI};
use std::ops::Range;

use cdshealpix::nested::n_hash;
use datafusion::arrow::datatypes::DataType;
use datafusion::common::{DFSchema, ScalarValue};
use datafusion::logical_expr::Expr;
use datafusion::prelude::lit;
use moc::elem::range::MocRange;
use moc::moc::range::{CellSelection, RangeMOC};
use moc::qty::Hpx;
use moc::ranges::SNORanges;

use crate::error::ApiError;
use crate::hats::{HatsPartition, HatsPartitionList};
use crate::region::Shape;
use crate::sql;

/// The deepest HEALPix order a cell number fits a 64-bit integer at, and the order HATS
/// recommends a catalog write its column at.
pub const MAX_ORDER: u8 = 29;

/// The column name HATS recommends for the HEALPix cell of a row's position.
///
/// A recommendation and not a rule. A request says which column holds the cell and at what
/// order; a catalog's `properties` says so in `hats_col_healpix` and
/// `hats_col_healpix_order`, and falls back to this pair when it says neither. What makes
/// the fallback safe is that it is a candidate rather than a claim — [`SpatialIndex::resolve`]
/// asks the file's schema, and a file with no such column is queried on the geometry alone.
pub const DEFAULT_HEALPIX_COLUMN_NAME: &str = "_healpix_29";

/// Roughly how many cells a covering of a shape may spend along its boundary, where the
/// covering is going into an expression over a file's rows.
///
/// **A range is not a cheap membership test.** The ranges become
/// `h BETWEEN a AND b OR h BETWEEN c AND d OR …`, which DataFusion evaluates as two
/// comparison kernels and an `OR` per range over every batch — the cost per row grows with
/// the number of ranges, with no tree and no search. Sixty of them are more arithmetic per
/// row than the four sines and two cosines of the haversine they were meant to spare it.
///
/// So what the ranges are actually for is **skipping row groups**, and that is what this is
/// sized to: a partition holds tens of row groups, and no set of ranges can skip more of
/// them than exist. A dozen or so bounds spread across a partition's HEALPix column can
/// exclude nearly all of the groups a region does not reach; the rest of the work is the
/// geometry's, which is exact and which the scan has to run anyway.
///
/// Measured, over a 60,000-row fixture in 30 row groups sorted by its HEALPix column: a
/// one-degree cone read the same 75 KiB of 207 KiB at budgets of 8, 64 and 256. The groups
/// skipped were the same ones. Only the length of the expression differed, which is the
/// thing that costs and the thing this keeps down.
const ROW_RANGE_BUDGET: u32 = 16;

/// How much finer than a catalog's own order a covering for choosing partitions is built.
///
/// Nothing per row happens with this covering: it answers [`Coverage::cover`] once per
/// partition, so its range count does not matter and its depth is not a budget. What
/// matters is the depth relative to the partitions being classified. At the catalog's own
/// order a partition the region merely touches is one cell, so it can only ever come back
/// as a boundary; each order finer splits that cell four ways and lets most of the pieces
/// resolve to inside or outside. Two orders is sixteen pieces, which settles the great
/// majority, and further orders divide a decision that has already been made.
const PARTITION_DETAIL: u8 = 2;

/// How much finer than a partition's own order a covering of its rows is built.
///
/// Sixteen cells across the partition, which is `2^4`. The region's boundary crosses a
/// partition roughly along a chord, so that is about sixteen cells along it and about as
/// many ranges — [`ROW_RANGE_BUDGET`], arrived at from the other end.
///
/// This is a floor, not a target: where the region is smaller than the partition its own
/// size asks for something finer, and finer is tighter and prunes better. What the floor
/// prevents is the opposite case, a region so much larger than the partition that a covering
/// sized for it cannot tell one part of the partition from another.
const PARTITION_ROWS_DETAIL: u8 = 4;

/// The deepest covering [`depth_for`] will choose.
///
/// A cell at order 20 is about 0.7 arcseconds across. Past that a range stops being able to
/// exclude anything a row group holds — a partition is written with far more than one row
/// per 0.7 arcsecond — so the extra depth buys no pruning and costs a longer expression on
/// every row. A shape smaller than that cell is still answered exactly, by the geometric
/// test that runs behind the covering.
///
/// It bounds the partition-choosing depth too, where what it prevents is different: a
/// catalog partitioned at order 19 would otherwise have a region covered at order 21, whose
/// cells are a tenth of an arcsecond and whose boundary is millions of them.
const MAX_DEPTH: u8 = 20;

/// The area of an order-0 HEALPix cell, in square degrees.
///
/// The twelve of them tile the sphere, so one is `4 pi / 12` steradians; the rest is the
/// square of the degrees in a radian. The area is exact — every cell at a given order has
/// the same one — and the square root of it is the side a cell would have if it were a
/// square, which no cell is: their shape varies from a diamond at the equator to a
/// four-cornered wedge at a pole, and a boundary crosses more or fewer of them per degree
/// depending on where it runs.
///
/// So this is a nominal scale, not a measurement, and it is all [`depth_for`] needs: what it
/// decides is which of thirty depths to build a covering at, and being out by a factor of
/// two there costs a factor of two in ranges rather than an answer. Written as an area
/// rather than as that nominal side because a square root is not something a `const` may
/// contain, and `depth_for` wants the square of a ratio anyway.
const BASE_CELL_AREA: f64 = (4.0 * PI / 12.0) * (180.0 / PI) * (180.0 / PI);

/// The widest cone anything here asks for, in degrees.
///
/// **No cone wider than this, ever.** A cone covering is a distance test per cell, and it
/// stops being one as the radius approaches a half turn: at 179 degrees the covering comes
/// back missing a tenth of the cells the cone holds, which for a superset is a lost row.
/// Everything wider is written as the complement of a cone narrower than this — a disk
/// larger than a hemisphere is the sky without the disk opposite it — and a declination band
/// is cut at the equator so that each half is measured from the pole it is nearer to.
///
/// It is also the widest cone worth having: one reaching a quarter turn already covers half
/// the sky, so a piece of a box's arc longer than this is cut in two rather than enclosed.
const QUARTER_TURN: f64 = 90.0;

/// How much finer than the covering depth a cone is evaluated at.
///
/// `cone_coverage_approx` tests a cell by its distance to the cone rather than exactly, so
/// it keeps cells that are near the cone without touching it. Descending two orders before
/// merging back costs a factor of sixteen in the cells visited and removes most of them.
const CONE_DELTA: u8 = 2;

/// The stretch of [`MAX_ORDER`] cells one cell spans.
///
/// The nested numbering is what makes this a range at all: a cell's children are contiguous
/// and follow it, so an order-4 cell is exactly the order-29 cells between these two bounds.
/// It is what orders a catalog's partitions — a mixed-order list sorts by it correctly, an
/// order-4 partition landing among the order-8 ones that would have subdivided it — and it is
/// the span a partition's own rows lie in.
#[must_use]
pub fn span(order: u8, pixel: u64) -> Range<u64> {
    MocRange::<u64, Hpx<u64>>::from((order.min(MAX_ORDER), pixel)).0
}

/// A HEALPix covering of the union of some shapes, from both sides.
///
/// The two sets are not complements: between them lies the shell of cells the boundary
/// crosses, which is where the geometry has to be evaluated.
#[derive(Debug, Clone)]
pub struct Coverage {
    /// Cells the region covers wholly. A subset of the region.
    inner: RangeMOC<u64, Hpx<u64>>,
    /// Cells the region touches at all. A superset of the region.
    outer: RangeMOC<u64, Hpx<u64>>,
}

/// What a covering is being built to answer, which is what its depth is chosen for.
///
/// The two jobs want different depths and the difference is not a matter of degree. Asking
/// which partitions a region touches is a question about cells the size of a partition, and
/// costs one comparison of ranges per partition however many ranges there are. Asking which
/// rows of a partition can be skipped puts every range into an expression that every row is
/// evaluated against, and the answer has to stay short.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Detail {
    /// For [`Coverage::cover`]: a couple of orders finer than the catalog's own, so that a
    /// partition the region merely touches is more often resolved than left to a row test.
    ///
    /// `catalog_order` is the **deepest** order the catalog partitions at — `hats_order`.
    /// A HATS catalog is partitioned hierarchically and its partitions sit at whatever
    /// orders the density asked for, a dense field split to the deepest and an empty one
    /// left near the top; the list is cells of mixed orders that tile the sky between them.
    /// Taking the deepest means the covering is fine enough to classify the smallest of
    /// them, and a covering finer than a cell classifies that cell no worse — it is only
    /// coarser that loses.
    Partitions { catalog_order: u8 },
    /// For [`Coverage::prefilter`]: as coarse as the shape allows, since every range is
    /// arithmetic on every row — but never so coarse that it says nothing about the file it
    /// is filtering.
    ///
    /// `partition_order` is the order of the catalog cell the file *is*, where the file is a
    /// partition of a catalog. That is knowable without reading anything — a partition at
    /// `Norder=k, Npix=p` holds exactly the cells `p << 2(29-k)` up to `(p+1) << 2(29-k)` —
    /// and it is what stops a covering sized for the region from being useless on the file.
    /// A region thirty degrees across is covered at order 2, whose cells are fifteen degrees
    /// wide; an order-5 partition is under two, so the whole of it falls inside one covering
    /// cell and the only range left after [`Coverage::within`] is one no row can fail.
    ///
    /// `None` for a file that is not a catalog partition, where nothing says how its column
    /// is distributed and the shape is all there is to go on.
    Rows { partition_order: Option<u8> },
}

/// How much of one cell — a catalog partition — a region covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Cover {
    /// The region does not reach this cell: nothing in it can qualify, and it need not be
    /// read at all.
    Outside,
    /// The region crosses this cell: its rows need a spatial test.
    Boundary,
    /// The region contains this cell: every row in it qualifies, and no spatial test on it
    /// can do anything but cost time.
    Inside,
}

/// One partition a region reaches, and how much of that partition it covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Reached<'a> {
    pub partition: &'a HatsPartition,
    /// Never [`Cover::Outside`]: a partition is reached because the outer covering
    /// intersects its cell, which is the same question [`Coverage::cover`] answers.
    pub cover: Cover,
}

impl Coverage {
    /// The covering of a union of shapes.
    ///
    /// Each shape is covered at its own depth, since a request may put a cone of an
    /// arcsecond beside a box of a degree and one depth cannot suit both; the union of the
    /// two takes the finer of them, which leaves the coarser shape's cells exactly where
    /// they were.
    ///
    /// The inner set is the union of each shape's own interior, which is a subset of the
    /// interior of the union — two shapes can cover a cell between them without either
    /// covering it alone. That cell then lands in the boundary shell and its rows meet the
    /// geometric test, which is the direction that costs time rather than rows.
    pub fn of(shapes: &[Shape], detail: Detail) -> Self {
        let empty = || RangeMOC::new(0, Default::default());
        let (inner, outer) = shapes
            .iter()
            .map(|shape| shape.covering(detail))
            .reduce(|(inner, outer), (shape_inner, shape_outer)| {
                (inner.union(&shape_inner), outer.union(&shape_outer))
            })
            .unwrap_or_else(|| (empty(), empty()));
        Self { inner, outer }
    }

    /// How much of the cell `pixel` at HEALPix order `order` this region covers.
    ///
    /// Both answers away from [`Cover::Boundary`] are conservative in the same direction the
    /// coverings are: a partition reported as boundary may in truth be wholly inside or
    /// wholly outside, and pays a spatial test it did not need. Neither of the other two is
    /// ever reported wrongly, which is what makes them safe to act on.
    ///
    /// Two binary searches over the coverings' ranges, and nothing per range. Asking whether
    /// the cell is *covered at all* is the cheap form of the question: `cell_fraction`
    /// answers a strictly harder one, walking every range the cell overlaps and summing
    /// their widths, and a coarse partition against a fine covering overlaps a great many.
    pub fn cover(&self, order: u8, pixel: u64) -> Cover {
        let cell = span(order, pixel);
        if self.inner.moc_ranges().contains_range(&cell) {
            Cover::Inside
        } else if self.outer.moc_ranges().intersects_range(&cell) {
            Cover::Boundary
        } else {
            Cover::Outside
        }
    }

    /// The partitions of a catalog this region reaches, in the list's own order.
    ///
    /// **Driven from the covering, not from the partition list.** Asking [`Self::cover`]
    /// about every partition is a pass over the whole catalog to find the handful a region
    /// touches — a hundred thousand classifications to keep four. A covering has tens of
    /// ranges, and the list is sorted by [`HatsPartition::span`]'s start, so each range is
    /// answered by searching into it. `cover` stays what classifies a candidate once found;
    /// it is the loop around it that should not be the catalog.
    ///
    /// The searches resume from where the last one landed, so the whole walk crosses the
    /// list once rather than once per range. That is also what keeps a partition coarser
    /// than the covering — one cell overlapping several ranges — out of the answer twice.
    ///
    /// The list is taken for a tiling, which is what a catalog's partitions are. Nothing
    /// checks it: a catalog whose cells nest gets whatever falls out, the way anything
    /// reading a broken catalog does.
    pub fn reaches<'a>(&self, partitions: &'a HatsPartitionList) -> Vec<Reached<'a>> {
        let cells = partitions.cells();
        let mut reached = Vec::new();
        let mut cursor = 0;
        for range in self.outer.moc_ranges().0.0.iter() {
            let from = gallop(cells, cursor, |cell| cell.span().end <= range.start);
            let to = gallop(cells, from, |cell| cell.span().start < range.end);
            reached.extend(
                cells
                    .get(from..to)
                    .unwrap_or_default()
                    .iter()
                    .map(|partition| Reached {
                        partition,
                        cover: self.cover(partition.order, partition.pixel),
                    }),
            );
            cursor = to;
        }
        reached
    }

    /// This covering restricted to one cell — a catalog partition.
    ///
    /// A region spanning a thousand partitions describes each of them with the handful of
    /// its own ranges that fall inside, rather than with every range it has: a partition's
    /// rows can only be tested against bounds its own cell contains, and the rest are
    /// comparisons the plan carries for nothing.
    #[must_use]
    pub fn within(&self, order: u8, pixel: u64) -> Self {
        let order = order.min(MAX_ORDER);
        let cell = RangeMOC::from_cells(order, std::iter::once((order, pixel)), Some(1));
        Self {
            inner: self.inner.and(&cell),
            outer: self.outer.and(&cell),
        }
    }

    /// The spatial predicate for one file's rows: the cheap tests, then `exact` behind them.
    ///
    /// `inner OR (outer AND exact)`. **The outer half is what makes the filter correct** —
    /// it contains the region, so nothing it rejects was wanted, and it alone would be a
    /// complete filter. The inner half never removes a row; it says which rows need no
    /// geometry, so the haversine runs only on the shell between the two. Both are
    /// comparisons against a literal, which is the form row-group statistics, the page index
    /// and a bloom filter can all prune on — though only the outer half's bounds can prune
    /// anything, the inner set being contained in it.
    ///
    /// What the inner half is worth is not measured. It costs two comparisons per inner
    /// range on every row, and saves the geometry on batches where it comes out true for
    /// four rows in five — DataFusion evaluates the right side of an `OR` unless the left is
    /// that lopsided. With the column sorted, a batch well inside the region is exactly that
    /// lopsided, which is the case it is kept for.
    ///
    /// **Whether they prune is the file's doing, not this expression's.** A catalog sorted
    /// by its HEALPix column has tight statistics on it and skips row groups wholesale; one
    /// that is not — which nothing about a file in the wild promises, `hats_cols_sort` being
    /// a note about how a catalog was built rather than a guarantee about how it was
    /// written — reads the column and gets the same rows, having only saved the
    /// trigonometry. Neither case is a different answer, which is why the sorting is nothing
    /// to check for.
    ///
    /// **The covering is the left operand of the `AND`, and that is what makes it a saving
    /// rather than an addition.** DataFusion's `AND` looks at the left side before evaluating
    /// the right: all false and the right side is skipped, and under a fifth true it switches
    /// to selecting those rows and evaluating the right side on them alone. So a covering
    /// that admits few rows is a covering that spares the trigonometry, which is the whole
    /// point — and a covering that admits most of them costs almost nothing over running the
    /// trigonometry alone. Putting `exact` on the left would spend the sines on every row and
    /// then filter the answer. The optimizer would move it back — it counts `BETWEEN` and a
    /// comparison as cheap and a call to `sin` as expensive, and orders conjuncts cheap
    /// first — but the expression should not be relying on being corrected.
    ///
    /// The covering is dropped entirely rather than allowed to grow: a shape whose covering
    /// needs more ranges than the budget allows would put more comparisons in front of the
    /// geometry than the geometry costs.
    ///
    /// `partition` is the catalog cell this file *is*, and the covering is cut down to it
    /// first. That is not an optimization to remember — it is what keeps the covering inside
    /// the budget at all. A covering built for a partition is as fine as the partition
    /// requires rather than as coarse as the region allows, so over a whole region it can
    /// carry more ranges than a row expression may; cut to one partition it carries the few
    /// that partition could hold. Passing `None` — a file that is not a catalog partition —
    /// is why the depth was not raised in the first place, so nothing needs cutting.
    pub fn prefilter(
        &self,
        partition: Option<(u8, u64)>,
        index: &SpatialIndex,
        exact: Expr,
    ) -> Expr {
        let cut;
        let coverage = match partition {
            Some((order, pixel)) => {
                cut = self.within(order, pixel);
                &cut
            }
            None => self,
        };
        let Some(outer) = index.ranges(&coverage.outer, Side::Outer) else {
            return exact;
        };
        match index.ranges(&coverage.inner, Side::Inner) {
            Some(inner) => inner.or(outer.and(exact)),
            None => outer.and(exact),
        }
    }
}

/// The first index at or after `from` where `still` stops holding.
///
/// `still` has to be true for a stretch of the list and false past it, which a list sorted
/// by [`HatsPartition::span`]'s start and disjoint gives both of [`Coverage::reaches`]'s
/// bounds.
///
/// **It doubles out from `from` before it searches**, so what a search costs is the gap it
/// actually crosses rather than the length of the list. Three ways to answer a covering's
/// ranges against a catalog, for tens of ranges over a hundred thousand partitions: a merge
/// cannot skip and walks all hundred thousand; a plain binary search per range pays the full
/// `log P` however near the last answer was; doubling pays the log of the distance travelled,
/// which summed over the ranges is what a merge would cost if it were allowed to skip. A
/// merge only overtakes when the covering has about as many ranges as the catalog has
/// partitions, and a region that large is one where most of the catalog is being opened
/// anyway.
fn gallop(cells: &[HatsPartition], from: usize, still: impl Fn(&HatsPartition) -> bool) -> usize {
    let rest = cells.get(from..).unwrap_or_default();
    // Everything below `low` satisfies `still`; `step` is how far the next probe reaches
    // past it. Probing at `low + step - 1` rather than scanning is what makes the skipped
    // entries free: the list is monotone under `still`, so one probe that holds carries
    // every entry behind it.
    let mut low = 0;
    let mut step = 1;
    while rest.get(low + step - 1).is_some_and(&still) {
        low += step;
        step *= 2;
    }
    // The probe that failed — or the end of the list — is the far side of the answer.
    let high = (low + step - 1).min(rest.len());
    let window = rest.get(low..high).unwrap_or_default();
    from + low + window.partition_point(|cell| still(cell))
}

/// A file's HEALPix column: which column, what it holds, and what order it is at.
///
/// The order is the caller's to state. HATS recommends `_healpix_29` and nothing more than
/// recommends it: the column may be called anything and be written at any order, and the
/// order is what decides which cell a value names. Reading a column written at order 12 as
/// though it were at order 29 puts every bound about 10¹⁰ times too high, which selects no
/// rows — a wrong answer rather than an error, and one no check of the column alone could
/// catch.
#[derive(Debug, Clone)]
pub struct SpatialIndex {
    column: Expr,
    /// The integer type the column holds, so a bound beside it is a literal of that same
    /// type. Any other integer type is a comparison DataFusion widens both sides of, and a
    /// widened comparison prunes nothing.
    cell_type: DataType,
    order: u8,
}

/// Which way a rounded range may be wrong.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Side {
    /// A covering that has to stay a subset: rounded inward, and dropped when nothing of it
    /// survives a whole cell of the column.
    Inner,
    /// A covering that has to stay a superset: rounded outward.
    Outer,
}

impl SpatialIndex {
    /// The column in the file's own spelling, with the order checked against what the column
    /// can actually hold.
    ///
    /// A cell at order `k` is a number below `12 * 4^k`, so a coarse enough catalog fits its
    /// column into an `Int32` or a `UInt16` and there is every reason for one to do so. What
    /// cannot be allowed is the pair disagreeing: an order whose cells do not fit the column
    /// means the column is not holding what the request says it is, and a bound past the
    /// type's own range is a comparison arrow answers by saturating rather than by failing.
    pub fn resolve(
        schema: &DFSchema,
        column: &str,
        order: u8,
        field: &str,
    ) -> Result<Self, ApiError> {
        if order > MAX_ORDER {
            return Err(ApiError::bad_request(format!(
                "{field}: {order} is past {MAX_ORDER}, the deepest HEALPix order"
            )));
        }
        let (column, cell_type) = sql::integer_column(schema, column, "healpix_column")?;
        let cells = n_hash(order);
        match capacity(&cell_type) {
            Some(capacity) if capacity >= cells - 1 => Ok(Self {
                column,
                cell_type,
                order,
            }),
            _ => Err(ApiError::bad_request(format!(
                "healpix_column: this column holds {cell_type:?}, too narrow for the \
                 {cells} cells of HEALPix order {order}"
            ))),
        }
    }

    /// One covering as a predicate over this column, or `None` when there is nothing useful
    /// to say — an empty covering, or one too detailed to be worth carrying.
    ///
    /// Both bounds are inclusive. A HEALPix range is half-open, but the value past its end
    /// need not be one the column's type can hold: the last cell of the sky at the order a
    /// column is exactly wide enough for is its type's largest value, and one past it is not
    /// a literal at all.
    fn ranges(&self, moc: &RangeMOC<u64, Hpx<u64>>, side: Side) -> Option<Expr> {
        let bounds = self.bounds(moc, side);
        // A covering built for partitions, used here — or one whose ranges a coarse column
        // did not merge as expected — would put more comparisons on every row than the
        // geometry behind them costs. Then it is better to have no prefilter at all.
        let too_many = match u32::try_from(bounds.len()) {
            Ok(count) => count > ROW_RANGE_BUDGET,
            Err(_) => true,
        };
        if bounds.is_empty() || too_many {
            return None;
        }
        bounds
            .into_iter()
            .map(|(low, high)| {
                self.column
                    .clone()
                    .between(self.cell(low), self.cell(high - 1))
            })
            .reduce(Expr::or)
    }

    /// One cell number as a literal of the column's own type.
    ///
    /// Every value reaching this is below `12 * 4^order`, which [`Self::resolve`] checked
    /// the type holds, so the conversion is exact. A conversion that failed anyway would
    /// leave a `UInt64` beside a narrower column, which DataFusion answers by widening both
    /// sides: the same rows, more slowly, rather than a bound that has moved.
    fn cell(&self, value: u64) -> Expr {
        let value = ScalarValue::UInt64(Some(value));
        lit(value.cast_to(&self.cell_type).unwrap_or(value))
    }

    /// A covering's ranges in this column's own units, rounded the way `side` may be wrong.
    ///
    /// A covering is built at whatever depth its shape asks for, which need not be the order
    /// the column is written at: a catalog may write its cells at order 12, and then a range
    /// of order-29 indices has to become a range of order-12 ones. Rounding outward keeps a
    /// superset a superset, so the outer set still rejects only what is outside; rounding
    /// inward keeps a subset a subset, so the inner set still admits only what is inside,
    /// and a range with no whole cell of the column left in it disappears rather than
    /// becoming a cell it does not fill.
    ///
    /// Rounding merges ranges — several fine ranges inside one coarse cell become one — so
    /// the result is coalesced, which is also what keeps them ordered and disjoint for the
    /// expression above.
    fn bounds(&self, moc: &RangeMOC<u64, Hpx<u64>>, side: Side) -> Vec<(u64, u64)> {
        let shift = 2 * u32::from(MAX_ORDER - self.order);
        let cell = 1_u64 << shift;
        let mut bounds: Vec<(u64, u64)> = Vec::new();
        for range in moc.moc_ranges().0.0.iter() {
            let (low, high) = match side {
                Side::Outer => (range.start >> shift, range.end.div_ceil(cell)),
                Side::Inner => (range.start.div_ceil(cell), range.end >> shift),
            };
            if low >= high {
                continue;
            }
            match bounds.last_mut() {
                // The ranges arrive ordered and disjoint, so a new one either touches the
                // last or starts past it.
                Some(last) if last.1 >= low => last.1 = last.1.max(high),
                _ => bounds.push((low, high)),
            }
        }
        bounds
    }
}

/// The largest value an integer type holds, or `None` for a type that is not one.
///
/// A signed type gives up its top bit, so it holds a HEALPix order less deep than the
/// unsigned type of the same size: `Int32` reaches order 13 and `UInt32` order 14.
fn capacity(data_type: &DataType) -> Option<u64> {
    Some(match data_type {
        DataType::Int8 => i8::MAX.cast_unsigned().into(),
        DataType::Int16 => i16::MAX.cast_unsigned().into(),
        DataType::Int32 => i32::MAX.cast_unsigned().into(),
        DataType::Int64 => i64::MAX.cast_unsigned(),
        DataType::UInt8 => u8::MAX.into(),
        DataType::UInt16 => u16::MAX.into(),
        DataType::UInt32 => u32::MAX.into(),
        DataType::UInt64 => u64::MAX,
        _ => return None,
    })
}

impl Shape {
    /// This shape's inner and outer coverings, at the depth its own size asks for.
    fn covering(&self, detail: Detail) -> (RangeMOC<u64, Hpx<u64>>, RangeMOC<u64, Hpx<u64>>) {
        match *self {
            Self::Circle { ra, dec, radius } => {
                // The circumference of a small circle of angular radius `r` is `2 pi sin r`,
                // which is the length the covering has to follow. Past a quarter turn it
                // shrinks again while the disk keeps growing, so the widest circle is what
                // sizes anything larger.
                let depth = detail.depth(360.0 * radius.min(QUARTER_TURN).to_radians().sin());
                if radius >= 180.0 {
                    let sky = RangeMOC::new_full_domain(depth);
                    return (sky.clone(), sky);
                }
                if radius <= QUARTER_TURN {
                    let cone = |selection| cone(ra, dec, radius, depth, selection);
                    return (cone(CellSelection::Inside), cone(CellSelection::All));
                }
                // A disk larger than a hemisphere is the sky without the disk opposite it,
                // which is the narrower of the two and so the one a cone may be asked for.
                // The complement swaps which covering is which: the sky without every cell
                // the far disk touches holds only cells wholly inside this one, and the sky
                // without the cells wholly inside the far disk holds every cell this one
                // touches.
                let far = |selection| cone(ra + 180.0, -dec, 180.0 - radius, depth, selection);
                (
                    far(CellSelection::All).complement(),
                    far(CellSelection::Inside).complement(),
                )
            }
            Self::Box {
                ra_from,
                ra_span,
                dec_from,
                dec_to,
            } => {
                // Two parallels of the arc's own length, and two meridian segments. A
                // parallel is shorter the further it is from the equator, so the box's
                // perimeter follows the one nearer to it.
                let along = ra_span * dec_from.abs().min(dec_to.abs()).to_radians().cos();
                let depth = detail.depth(2.0 * (along + dec_to - dec_from));
                // A box with no declination between its two is a parallel: it has no
                // interior for an inner covering, and no band for the outer one to
                // intersect. So it gets the sky, which says nothing and costs one comparison
                // no row can fail, and its rows are decided by the geometry alone.
                if dec_from >= dec_to {
                    return (
                        RangeMOC::new(depth, Default::default()),
                        RangeMOC::new_full_domain(depth),
                    );
                }
                let inner = inside_a_box(ra_from, ra_span, dec_from, dec_to, depth);
                // The two supersets a box is the intersection of. Each is built from cone
                // coverings, which are documented to include cells the shape misses and
                // never to miss one it covers — so their intersection contains the box.
                let mut outer = declinations_within(dec_from, dec_to, depth);
                if ra_span < 360.0 {
                    outer = outer.and(&right_ascensions_within(
                        ra_from, ra_span, dec_from, dec_to, depth,
                    ));
                }
                (inner, outer)
            }
        }
    }
}

/// The cells wholly inside a box.
///
/// From the walk along the box itself, whose "wholly covered" flags are the one thing that
/// walk promises: a cell it flags is genuinely covered, and it is free to flag fewer than it
/// might. Fewer is the direction an inner covering may be wrong in — a cell left out has its
/// rows tested by the geometry, which is slower and not different — so what it loses is
/// affordable here and would not be in [`declinations_within`].
fn inside_a_box(
    ra_from: f64,
    ra_span: f64,
    dec_from: f64,
    dec_to: f64,
    depth: u8,
) -> RangeMOC<u64, Hpx<u64>> {
    if ra_span >= 360.0 {
        return RangeMOC::from_ring(
            0.0,
            FRAC_PI_2,
            (90.0 - dec_to).to_radians(),
            (90.0 - dec_from).to_radians(),
            depth,
            CONE_DELTA,
            CellSelection::Inside,
        );
    }
    let from = ra_from.rem_euclid(360.0);
    // An arc whose first value is the greater is read as crossing the origin, so the wrap
    // needs no arithmetic of its own — except for an arc ending exactly at the origin, which
    // written as `0` would read as the arc the other way round.
    let to = match (from + ra_span).rem_euclid(360.0) {
        0.0 => 360.0,
        wrapped => wrapped,
    };
    RangeMOC::from_zone(
        from.to_radians(),
        dec_from.to_radians(),
        to.to_radians(),
        dec_to.to_radians(),
        depth,
        CellSelection::Inside,
    )
}

/// A superset of every position between two declinations: the ring about a pole they cut out
/// of the sphere.
///
/// A ring is two cone coverings, and a cone covering is a distance test per cell — no walk
/// along an edge, and a documented superset. That is why the box's declinations are covered
/// this way rather than taken from the walk that produced its inner covering: the walk drops
/// wedges of a cell beyond an edge that lies on the seam between two base cells, which for a
/// superset is a lost row rather than a slower query. The two edges of a box are exactly
/// where a caller writes a round number, and every seam is one.
///
/// A band straddling the equator is the union of its two halves, each measured from the pole
/// it is nearer to. That is what keeps every radius inside [`QUARTER_TURN`]: measured from
/// one pole, a band reaching past the equator is a ring whose outer radius is more than a
/// quarter turn, and the far side of such a cone is where the covering starts losing cells.
fn declinations_within(dec_from: f64, dec_to: f64, depth: u8) -> RangeMOC<u64, Hpx<u64>> {
    // Distances from a pole rather than declinations: from the north pole a declination is
    // `90 - dec` away, and the nearer edge of the band is the ring's inner radius.
    let north = |from: f64, to: f64| ring(FRAC_PI_2, 90.0 - to, 90.0 - from, depth);
    let south = |from: f64, to: f64| ring(-FRAC_PI_2, from + 90.0, to + 90.0, depth);
    match (dec_from < 0.0, dec_to > 0.0) {
        (true, true) => south(dec_from, 0.0).union(&north(0.0, dec_to)),
        (true, false) => south(dec_from, dec_to),
        _ => north(dec_from, dec_to),
    }
}

/// The cells a ring of these radii about `lat` touches, both radii in degrees.
fn ring(lat: f64, inner: f64, outer: f64, depth: u8) -> RangeMOC<u64, Hpx<u64>> {
    RangeMOC::from_ring(
        0.0,
        lat,
        inner.to_radians(),
        outer.to_radians(),
        depth,
        CONE_DELTA,
        CellSelection::All,
    )
}

/// The cells a cone of `radius` degrees about a position in degrees touches, or those it
/// covers wholly.
fn cone(
    ra: f64,
    dec: f64,
    radius: f64,
    depth: u8,
    selection: CellSelection,
) -> RangeMOC<u64, Hpx<u64>> {
    RangeMOC::from_cone(
        ra.rem_euclid(360.0).to_radians(),
        dec.to_radians(),
        radius.to_radians(),
        depth,
        CONE_DELTA,
        selection,
    )
}

/// A superset of a box's arc of right ascension: the cones enclosing the pieces of it.
///
/// One cone over the whole box would be as wide as the box is long, and a cone wider than a
/// quarter turn covers most of the sky, so the arc is cut into pieces short enough for each
/// cone to be worth having — four at most, since the arc is under a whole turn here.
///
/// Each piece's cone is centred on the middle of the piece and reaches its farthest corner.
/// A corner is the farthest point of a piece: along a parallel the distance from the centre
/// grows with the difference in right ascension, and along a meridian `cos d` is
/// `R cos(dec - phi)` for constants of the centre's own, which has no interior minimum — so
/// both coordinates put the maximum at an end, and the ends are the corners.
fn right_ascensions_within(
    ra_from: f64,
    ra_span: f64,
    dec_from: f64,
    dec_to: f64,
    depth: u8,
) -> RangeMOC<u64, Hpx<u64>> {
    // How many quarter turns the arc needs, counted rather than computed and rounded: an arc
    // under a whole turn takes at most four, so the search is over before it starts.
    const MOST: u32 = 4;
    let count = (1..MOST)
        .find(|pieces| ra_span <= QUARTER_TURN * f64::from(*pieces))
        .unwrap_or(MOST);
    let piece = ra_span / f64::from(count);
    let dec = midpoint(dec_from, dec_to);
    (0..count)
        .map(|index| {
            let ra = ra_from + piece * (f64::from(index) + 0.5);
            let radius = [
                (ra_from + piece * f64::from(index), dec_from),
                (ra_from + piece * f64::from(index), dec_to),
                (ra_from + piece * f64::from(index + 1), dec_from),
                (ra_from + piece * f64::from(index + 1), dec_to),
            ]
            .into_iter()
            .map(|(corner_ra, corner_dec)| separation(ra, dec, corner_ra, corner_dec))
            .fold(0.0_f64, f64::max);
            cone(ra, dec, radius, depth, CellSelection::All)
        })
        .reduce(|left, right| left.union(&right))
        .unwrap_or_else(|| RangeMOC::new_full_domain(depth))
}

/// The angular distance between two positions in degrees, in degrees.
fn separation(ra: f64, dec: f64, other_ra: f64, other_dec: f64) -> f64 {
    let (dec, other_dec) = (dec.to_radians(), other_dec.to_radians());
    (dec.sin() * other_dec.sin() + dec.cos() * other_dec.cos() * (ra - other_ra).to_radians().cos())
        .clamp(-1.0, 1.0)
        .acos()
        .to_degrees()
}

/// The declination halfway between two, which is the middle of a box in declination —
/// unlike right ascension, where halfway depends on which way round the arc runs.
fn midpoint(from: f64, to: f64) -> f64 {
    from + 0.5 * (to - from)
}

impl Detail {
    /// The HEALPix depth to cover a shape whose boundary is `perimeter` degrees long.
    ///
    /// Choosing partitions ignores the shape: what the depth has to suit is the cells being
    /// classified, and those are the catalog's, whatever the region over them is.
    ///
    /// Covering rows takes the finer of what the shape asks for and what the partition does.
    /// The two are floors on each other rather than a compromise between them: a covering
    /// coarser than the shape wanted describes the shape badly, and one coarser than
    /// [`PARTITION_ROWS_DETAIL`] allows describes *this partition* badly, whatever it does
    /// for the shape. Both stay within the budget — the shape's depth by construction, the
    /// partition's because the region crosses a partition along a chord and not along its
    /// whole boundary.
    fn depth(self, perimeter: f64) -> u8 {
        match self {
            Self::Partitions { catalog_order } => catalog_order
                .saturating_add(PARTITION_DETAIL)
                .min(MAX_DEPTH),
            Self::Rows {
                partition_order: None,
            } => depth_for(perimeter),
            Self::Rows {
                partition_order: Some(order),
            } => depth_for(perimeter)
                .max(order.saturating_add(PARTITION_ROWS_DETAIL))
                .min(MAX_DEPTH),
        }
    }
}

/// The depth to describe a shape whose boundary is `perimeter` degrees long, in an
/// expression a file's every row will be evaluated against.
///
/// A covering follows the boundary, so the cells it spends are about the perimeter divided
/// by the side of one cell; solving that for the budget gives a depth that grows as the
/// shape shrinks — which is the point, since a cone of an arcsecond described at the depth
/// that suits a cone of a degree is one cell covering ten thousand times its area.
///
/// **The perimeter rather than the area, because what is being budgeted is ranges.** A cell
/// in the interior of a shape is one of four siblings all inside it, which merge into their
/// parent and merge again — the interior of the whole sky is a single range whatever the
/// depth. It is the boundary that stops cells merging, so the range count follows the
/// boundary's length and not the area inside it. For a round shape the two agree up to a
/// constant, since area and perimeter both follow the radius; where they part company is a
/// thin one, and a strip of declination a hundred degrees long and a hundredth wide is an
/// ordinary thing to ask for. Sized by its area it would be covered at a depth fine enough
/// to describe the width, and come back as tens of thousands of ranges.
fn depth_for(perimeter: f64) -> u8 {
    // A shape with no boundary to follow — a box of no extent, or one whose numbers left a
    // `NaN` behind — is described as finely as anything here ever is, and its rows are
    // decided by the geometry either way.
    if perimeter.is_nan() || perimeter <= 0.0 {
        return MAX_DEPTH;
    }
    // How many cells of the deepest order that fits in the budget span the boundary, squared
    // — so that the base cell's side stays a square root nobody has to take. Each order
    // quarters a cell's area, so the depth is a quarter logarithm of it, which is half of a
    // binary one.
    let budget = f64::from(ROW_RANGE_BUDGET);
    let cells_across_squared = BASE_CELL_AREA * budget * budget / (perimeter * perimeter);
    if cells_across_squared <= 1.0 {
        return 0;
    }
    // The deepest order whose cells still fit that many across the boundary — the largest
    // `d` with `4^d` at most the count, since each order quarters a cell's area. Walked
    // rather than taken as a logarithm, which would have to be truncated back to an integer
    // and there is no conversion from a float that says what truncation should mean.
    (0..MAX_DEPTH)
        .find(|order| cells_across_squared < 4.0_f64.powi(i32::from(*order) + 1))
        .unwrap_or(MAX_DEPTH)
}

#[cfg(test)]
mod tests {
    use super::*;

    use crate::hats::partitions::Source;

    /// The sky, as the covering of a circle that reaches every part of it.
    fn all_sky() -> Coverage {
        Coverage::of(
            &[Shape::Circle {
                ra: 0.0,
                dec: 0.0,
                radius: 180.0,
            }],
            Detail::Rows {
                partition_order: None,
            },
        )
    }

    /// One cone against one catalog, end to end: the sequence a HATS query runs, and the
    /// property that makes each step worth taking.
    ///
    /// A degree-wide cone over a catalog partitioned at order 5 — cells a degree and three
    /// quarters across, so the cone spans a few of them and contains none.
    ///
    /// The step this is really holding is the last: with the partition floor raising the
    /// covering above the depth the region's own size asked for, the covering of the whole
    /// region carries more ranges than a row expression may. Truncating it to a partition is
    /// what brings it back inside the budget, so the sequence is not optional — skip
    /// `within` and `prefilter` drops the covering rather than carry it.
    #[test]
    fn a_cone_over_a_catalog_narrows_at_every_step() {
        const CATALOG_ORDER: u8 = 5;

        let shapes = [Shape::Circle {
            ra: 45.0,
            dec: 20.0,
            radius: 1.0,
        }];

        // Which partitions to open at all.
        let choosing = Coverage::of(
            &shapes,
            Detail::Partitions {
                catalog_order: CATALOG_ORDER,
            },
        );
        let mut boundary = Vec::new();
        let mut inside = 0_usize;
        for pixel in 0..n_hash(CATALOG_ORDER) {
            match choosing.cover(CATALOG_ORDER, pixel) {
                Cover::Inside => inside += 1,
                Cover::Boundary => boundary.push(pixel),
                Cover::Outside => {}
            }
        }
        assert_eq!(inside, 0, "a one-degree cone cannot contain a partition");
        assert!(
            !boundary.is_empty() && boundary.len() < 20,
            "{} partitions to open out of {}",
            boundary.len(),
            n_hash(CATALOG_ORDER)
        );

        // Which rows of those partitions to read.
        let rows = Coverage::of(
            &shapes,
            Detail::Rows {
                partition_order: Some(CATALOG_ORDER),
            },
        );
        let index = resolve(DataType::Int64, MAX_ORDER).unwrap();
        let ranges = |coverage: &Coverage| coverage.outer.moc_ranges().0.0.len();
        for &pixel in &boundary {
            let within = rows.within(CATALOG_ORDER, pixel);
            assert!(
                u32::try_from(ranges(&within)).unwrap() <= ROW_RANGE_BUDGET,
                "Npix={pixel} kept {} ranges, over the budget a row expression allows",
                ranges(&within)
            );
            // Built from the whole-region covering, which carries more ranges than a row
            // expression may: cutting it to the partition is `prefilter`'s own doing.
            let predicate = format!(
                "{}",
                rows.prefilter(Some((CATALOG_ORDER, pixel)), &index, lit(true))
            );
            assert!(
                predicate.contains(DEFAULT_HEALPIX_COLUMN_NAME),
                "Npix={pixel} came back with no covering in front of the geometry"
            );
        }
        assert!(
            ranges(&rows)
                > boundary
                    .iter()
                    .map(|&p| ranges(&rows.within(CATALOG_ORDER, p)))
                    .max()
                    .unwrap(),
            "truncating to a partition should leave fewer ranges than the region has"
        );
    }

    /// Choosing partitions and testing rows are two different depths, and neither is the
    /// other's.
    ///
    /// The partition depth follows the catalog and not the region: a covering at the
    /// catalog's own order can only ever call a partition it touches a boundary, so it is
    /// taken finer, and how much finer is not the region's business. The row depth follows
    /// the region and not the catalog, since what it is spending is comparisons per row.
    #[test]
    fn the_two_jobs_ask_for_different_depths() {
        let small = 0.01;
        let large = 100.0;
        for order in [0_u8, 5, 11] {
            let detail = Detail::Partitions {
                catalog_order: order,
            };
            assert_eq!(detail.depth(small), order + PARTITION_DETAIL);
            assert_eq!(
                detail.depth(large),
                order + PARTITION_DETAIL,
                "the region's size should not move the partition depth"
            );
        }
        // A catalog partitioned finer than any covering is worth building at.
        assert_eq!(
            Detail::Partitions {
                catalog_order: MAX_DEPTH
            }
            .depth(small),
            MAX_DEPTH
        );
        // A file that is not a partition has only the shape to go on, and a smaller shape
        // asks for a finer covering.
        let alone = Detail::Rows {
            partition_order: None,
        };
        assert!(alone.depth(small) > alone.depth(large));

        // A partition puts a floor under that, and the floor is what a large region needs:
        // a covering sized for the region alone cannot tell one part of the partition from
        // another. A small region is already finer than the floor and keeps its own depth.
        for order in [0_u8, 5, 11] {
            let partition = Detail::Rows {
                partition_order: Some(order),
            };
            assert_eq!(
                partition.depth(large),
                order + PARTITION_ROWS_DETAIL,
                "a region larger than the partition should be described at the partition's \
                 scale, not its own"
            );
            assert_eq!(
                partition.depth(small),
                alone.depth(small).max(order + PARTITION_ROWS_DETAIL),
                "a region smaller than the partition should keep its own finer depth"
            );
            assert!(partition.depth(large) >= alone.depth(large));
        }
        assert_eq!(
            Detail::Rows {
                partition_order: Some(MAX_DEPTH)
            }
            .depth(large),
            MAX_DEPTH
        );
    }

    /// A region far larger than a partition still has to say something about that
    /// partition's rows, and only the partition's own order can make it.
    ///
    /// This is the case the floor exists for, so it is checked by removing the floor and
    /// watching it fail: covered at the depth its own size asks for, a thirty-degree region
    /// swallows an order-5 partition whole, and what survives `within` is a single range no
    /// row can fail — a filter that reads every row and skips no group. The same region
    /// covered at the partition's scale traces its boundary through the partition instead.
    #[test]
    fn a_region_larger_than_a_partition_is_covered_at_the_partitions_scale() {
        const ORDER: u8 = 5;

        // A partition the region's edge runs through, which is where a row filter is needed.
        let shapes = [Shape::Circle {
            ra: 40.0,
            dec: 10.0,
            radius: 30.0,
        }];
        let pixel = (0..n_hash(ORDER))
            .find(|&pixel| {
                Coverage::of(
                    &shapes,
                    Detail::Partitions {
                        catalog_order: ORDER,
                    },
                )
                .cover(ORDER, pixel)
                    == Cover::Boundary
            })
            .expect("the region should cross some partition of the sky");

        let ranges = |partition_order| {
            Coverage::of(&shapes, Detail::Rows { partition_order })
                .within(ORDER, pixel)
                .outer
                .moc_ranges()
                .0
                .0
                .len()
        };
        assert_eq!(
            ranges(None),
            1,
            "the region's own depth should have nothing to say about this partition, which \
             is what the floor is for"
        );
        assert!(
            ranges(Some(ORDER)) > 1,
            "the partition's own scale should divide it into ranges a row group can be \
             skipped by"
        );
    }

    /// Going finer than the catalog's own order is what makes a partition covering worth
    /// building: fewer partitions come back needing a row test.
    ///
    /// The comparison is against the same covering built at the catalog's order, which is
    /// what leaving the extra detail out would give. That one resolves far less — a partition
    /// the region merely touches is a single cell of it, so it can only be a boundary — and
    /// nothing it does resolve does the finer one disagree with, since a covering is not made
    /// looser by being made finer.
    #[test]
    fn going_finer_than_the_catalog_resolves_more_partitions() {
        const ORDER: u8 = 5;

        let shapes = [Shape::Circle {
            ra: 45.0,
            dec: 20.0,
            radius: 8.0,
        }];
        let covers = |catalog_order| {
            let coverage = Coverage::of(&shapes, Detail::Partitions { catalog_order });
            (0..n_hash(ORDER))
                .map(|pixel| coverage.cover(ORDER, pixel))
                .collect::<Vec<Cover>>()
        };
        // The same covering at the catalog's own order, which is what leaving the extra
        // detail out would give.
        let coarse = covers(ORDER - PARTITION_DETAIL);
        let fine = covers(ORDER);
        let boundaries =
            |covers: &[Cover]| covers.iter().filter(|&&c| c == Cover::Boundary).count();

        assert!(
            boundaries(&fine) < boundaries(&coarse),
            "{} partitions left to a row test, against {} at the catalog's own order",
            boundaries(&fine),
            boundaries(&coarse)
        );
        assert!(
            fine.contains(&Cover::Inside),
            "no partition came back wholly inside the region"
        );
        assert!(
            fine.contains(&Cover::Outside),
            "no partition came back outside it"
        );
        // Finer resolves more, and never resolves anything differently.
        for (pixel, (coarse, fine)) in coarse.iter().zip(&fine).enumerate() {
            assert!(
                *coarse == Cover::Boundary || coarse == fine,
                "partition {pixel} was {coarse:?} at the coarser depth and {fine:?} at the \
                 finer one"
            );
        }
    }

    /// A depth chosen for a shape has to fall as the shape grows, and stop at both ends.
    ///
    /// The middle of the range is what the policy is for; the two ends are where an
    /// unclamped formula would ask for a covering at order 29 — which is the thing this
    /// exists to prevent — or for a negative one.
    #[test]
    fn the_covering_depth_falls_as_the_shape_grows() {
        let sizes = [0.0, 1e-6, 1e-3, 0.1, 1.0, 10.0, 90.0, 360.0, 720.0];
        let depths: Vec<u8> = sizes.iter().map(|&size| depth_for(size)).collect();
        assert!(
            depths.windows(2).all(|pair| pair[0] >= pair[1]),
            "a larger shape asked for a finer covering: {sizes:?} gave {depths:?}"
        );
        assert!(
            depths.iter().all(|&depth| depth <= MAX_DEPTH),
            "a covering past the cap: {depths:?}"
        );
        assert_eq!(depth_for(0.0), MAX_DEPTH, "a shape with no extent");
        assert_eq!(depth_for(f64::INFINITY), 0, "a shape larger than the sky");
    }

    /// Shapes picked for the places a covering goes wrong: the meridian, both poles, a disk
    /// wider than a hemisphere, edges lying on the seams where base cells meet, and the whole
    /// sky written two ways.
    fn awkward_shapes() -> Vec<Shape> {
        vec![
            Shape::Circle {
                ra: 0.0,
                dec: 0.0,
                radius: 1.0,
            },
            Shape::Circle {
                ra: 359.9,
                dec: 0.0,
                radius: 0.5,
            },
            Shape::Circle {
                ra: 45.0,
                dec: 89.5,
                radius: 1.0,
            },
            Shape::Circle {
                ra: 45.0,
                dec: -89.5,
                radius: 2.0,
            },
            Shape::Circle {
                ra: 200.0,
                dec: -30.0,
                radius: 0.001,
            },
            // Wider than a hemisphere, which is covered as the complement of the disk
            // opposite it rather than as a cone of its own.
            Shape::Circle {
                ra: 30.0,
                dec: 40.0,
                radius: 120.0,
            },
            Shape::Circle {
                ra: 0.0,
                dec: 0.0,
                radius: 179.5,
            },
            Shape::Box {
                ra_from: 350.0,
                ra_span: 20.0,
                dec_from: -20.0,
                dec_to: -10.0,
            },
            Shape::Box {
                ra_from: 0.0,
                ra_span: 360.0,
                dec_from: 80.0,
                dec_to: 90.0,
            },
            Shape::Box {
                ra_from: 100.0,
                ra_span: 1.0,
                dec_from: 0.0,
                dec_to: 0.0,
            },
            // Edges on the seams where the base cells meet, which is where a walk along an
            // edge loses a wedge of the cell beyond it. Both of the polar caps, since the
            // four cells of one meet at multiples of 90 and the equatorial belt at 45.
            Shape::Box {
                ra_from: 0.0,
                ra_span: 90.0,
                dec_from: 80.0,
                dec_to: 90.0,
            },
            Shape::Box {
                ra_from: 90.0,
                ra_span: 90.0,
                dec_from: -90.0,
                dec_to: -60.0,
            },
            Shape::Box {
                ra_from: 315.0,
                ra_span: 45.0,
                dec_from: -50.0,
                dec_to: 50.0,
            },
            Shape::Box {
                ra_from: 270.0,
                ra_span: 90.0,
                dec_from: 45.0,
                dec_to: 89.0,
            },
            // Declinations either side of the equator, which are covered as two bands
            // measured from the pole each is nearer to.
            Shape::Box {
                ra_from: 20.0,
                ra_span: 200.0,
                dec_from: -70.0,
                dec_to: 70.0,
            },
            Shape::Box {
                ra_from: 0.0,
                ra_span: 360.0,
                dec_from: -1.0,
                dec_to: 1.0,
            },
            // The whole sky, written as a box.
            Shape::Box {
                ra_from: 0.0,
                ra_span: 360.0,
                dec_from: -90.0,
                dec_to: 90.0,
            },
        ]
    }

    /// A mixed-order tiling of the whole sky: every order-1 cell, one in three of them split
    /// into its sixteen order-3 children.
    ///
    /// Mixed orders are what make a search into the list a test rather than a coincidence. A
    /// coarse partition spans several of a fine covering's ranges, so a walk over the ranges
    /// meets it more than once — and a walk that resumes where the last search landed is the
    /// thing that has to return it exactly once.
    fn tiling() -> HatsPartitionList {
        let mut cells = Vec::new();
        for pixel in 0..n_hash(1) {
            match pixel % 3 {
                0 => cells.extend((pixel << 4..(pixel + 1) << 4).map(|c| HatsPartition::new(3, c))),
                _ => cells.push(HatsPartition::new(1, pixel)),
            }
        }
        HatsPartitionList::new(cells, Source::Listing)
    }

    /// A covering has to bracket the shape it covers, at every scale and everywhere on the
    /// sky — including the poles, where the cells are a different shape, and the meridian,
    /// where the longitudes wrap.
    ///
    /// Checked by asking where a point is twice: once of the coverings, and once of the
    /// geometry itself. A point the outer covering rejects must be outside; a point the
    /// inner covering accepts must be inside. Both errors are only allowed the other way.
    #[test]
    fn a_covering_brackets_its_shape() {
        for shape in awkward_shapes() {
            let coverage = Coverage::of(
                &[shape],
                Detail::Rows {
                    partition_order: None,
                },
            );
            for (ra, dec) in grid() {
                let index = cdshealpix::nested::hash(
                    MAX_ORDER,
                    ra.rem_euclid(360.0).to_radians(),
                    dec.to_radians(),
                );
                let inside = contains(&shape, ra, dec);
                if coverage.inner.contains_val(&index) {
                    assert!(inside, "{shape:?} claims ({ra}, {dec}) is wholly inside it");
                }
                if inside {
                    assert!(
                        coverage.outer.contains_val(&index),
                        "{shape:?} does not cover ({ra}, {dec}), which is inside it"
                    );
                }
            }
        }
    }

    /// Searching into the partition list finds exactly what a pass over it would.
    ///
    /// That is the whole claim [`Coverage::reaches`] makes. It is driven from the covering
    /// rather than from the catalog, it skips stretches of the list without looking at them,
    /// and each of its searches starts where the last one stopped — and none of that may
    /// change which partitions come back, what each is classified as, or the order they
    /// arrive in.
    #[test]
    fn a_search_into_the_partitions_finds_what_a_pass_would() {
        let partitions = tiling();
        for shape in awkward_shapes() {
            // Both sides of the catalog's own order, so the covering is coarser than some
            // partitions and finer than others.
            for catalog_order in [0_u8, 1, 3, 6] {
                let coverage = Coverage::of(&[shape], Detail::Partitions { catalog_order });
                let expected = partitions
                    .cells()
                    .iter()
                    .map(|cell| (cell, coverage.cover(cell.order, cell.pixel)))
                    .filter(|&(_, cover)| cover != Cover::Outside)
                    .collect::<Vec<_>>();
                let reached = coverage
                    .reaches(&partitions)
                    .into_iter()
                    .map(|reached| (reached.partition, reached.cover))
                    .collect::<Vec<_>>();
                assert_eq!(
                    reached, expected,
                    "{shape:?} against a catalog at order {catalog_order}"
                );
            }
        }
    }

    /// Doubling out and then searching lands where a search over the whole list would.
    ///
    /// Checked from every starting point, since the gap a search crosses is what the doubling
    /// is sized against and a resumed search starts anywhere.
    #[test]
    fn galloping_lands_where_a_plain_search_would() {
        let partitions = tiling();
        let cells = partitions.cells();
        for boundary in 0..=cells.len() {
            let still = |cell: &HatsPartition| {
                cell.span().start
                    < cells
                        .get(boundary)
                        .map_or(u64::MAX, |cell| cell.span().start)
            };
            let expected = cells.partition_point(&still);
            for from in 0..=expected {
                assert_eq!(
                    gallop(cells, from, still),
                    expected,
                    "resuming from {from} with the answer at {expected}"
                );
            }
        }
    }

    /// A partition wholly inside a region, one the boundary crosses and one outside are
    /// three different answers, and the two definite ones have to be right.
    #[test]
    fn a_partition_is_inside_outside_or_neither() {
        // A circle around the centre of one order-3 cell, wide enough to swallow it whole
        // and narrow enough to leave most of its neighbours alone.
        let pixel = 100_u64;
        let (lon, lat) = cdshealpix::nested::center(3, pixel);
        let coverage = Coverage::of(
            &[Shape::Circle {
                ra: lon.to_degrees(),
                dec: lat.to_degrees(),
                radius: 10.0,
            }],
            Detail::Partitions { catalog_order: 3 },
        );
        assert_eq!(coverage.cover(3, pixel), Cover::Inside);
        let antipode = (pixel + n_hash(3) / 2) % n_hash(3);
        assert_eq!(coverage.cover(3, antipode), Cover::Outside);
        // The cell containing the circle's edge is crossed by it, whichever cell that is.
        let edge = cdshealpix::nested::hash(3, lon, lat + 10.0_f64.to_radians());
        assert_eq!(coverage.cover(3, edge), Cover::Boundary);
    }

    /// Restricting a covering to a partition may only drop what is outside that partition.
    #[test]
    fn a_covering_within_a_cell_keeps_what_that_cell_holds() {
        let coverage = all_sky();
        let pixel = 7_u64;
        let within = coverage.within(2, pixel);
        assert_eq!(within.cover(2, pixel), Cover::Inside);
        assert_eq!(within.cover(2, (pixel + 1) % n_hash(2)), Cover::Outside);
        assert!(within.outer.n_depth_max_cells() < coverage.outer.n_depth_max_cells());

        // And the point of it: fewer ranges to put in front of a partition's rows. A region
        // spread over the sky keeps only the part of itself that this partition could hold,
        // so what the expression carries is bounded by the partition rather than by the
        // region.
        let scattered = Coverage::of(
            &[
                Shape::Circle {
                    ra: 30.0,
                    dec: 10.0,
                    radius: 2.0,
                },
                Shape::Circle {
                    ra: 200.0,
                    dec: -40.0,
                    radius: 2.0,
                },
                Shape::Circle {
                    ra: 300.0,
                    dec: 70.0,
                    radius: 2.0,
                },
            ],
            Detail::Rows {
                partition_order: None,
            },
        );
        let ranges = |coverage: &Coverage| coverage.outer.moc_ranges().0.0.len();
        let held = (0..n_hash(2))
            .map(|pixel| ranges(&scattered.within(2, pixel)))
            .collect::<Vec<usize>>();
        assert!(
            held.iter().sum::<usize>() >= ranges(&scattered),
            "truncating to every partition should account for every range"
        );
        assert!(
            held.iter().copied().max().unwrap() < ranges(&scattered),
            "no single partition should carry the whole region's {} ranges",
            ranges(&scattered)
        );
    }

    /// A covering has to say the same thing about a coarse HEALPix column as about a fine
    /// one: the same cells, in the column's own units.
    ///
    /// The two roundings go opposite ways. Every index the outer covering held has to
    /// survive being expressed at the coarser order, since a row the bounds drop is a row
    /// lost and no geometry behind them gets to say otherwise; nothing the inner covering
    /// did not hold may appear, since a row it adds is a row that skipped the geometry it
    /// needed.
    #[test]
    fn a_coarser_column_is_rounded_the_way_its_covering_may_be_wrong() {
        let shape = Shape::Circle {
            ra: 45.0,
            dec: 20.0,
            radius: 2.0,
        };
        let coverage = Coverage::of(
            &[shape],
            Detail::Rows {
                partition_order: None,
            },
        );
        for order in [4_u8, 8, 12, MAX_ORDER] {
            let index = spatial_index(order);
            let shift = 2 * u32::from(MAX_ORDER - order);
            let outer = index.bounds(&coverage.outer, Side::Outer);
            let inner = index.bounds(&coverage.inner, Side::Inner);
            for range in coverage.outer.moc_ranges().0.0.iter() {
                for edge in [range.start, range.end - 1] {
                    let at_order = edge >> shift;
                    assert!(
                        outer
                            .iter()
                            .any(|&(low, high)| (low..high).contains(&at_order)),
                        "order {order} dropped a cell the covering held"
                    );
                }
            }
            for &(low, high) in &inner {
                for value in [low, high - 1] {
                    assert!(
                        coverage.inner.contains_val(&(value << shift)),
                        "order {order} claims a cell the inner covering does not hold"
                    );
                }
            }
            // Rounding a subset inward can empty it; widening a superset never can.
            assert!(!outer.is_empty(), "order {order} covers nothing");
            assert!(
                outer.windows(2).all(|pair| pair[0].1 < pair[1].0),
                "order {order} left ranges that touch: {outer:?}"
            );
        }
    }

    /// A covering of the whole sky is one range over every cell, and the geometry behind it
    /// never runs — and a covering with nothing to say leaves the geometry as the whole
    /// predicate rather than putting a test in front of it that no row can fail.
    #[test]
    fn a_covering_of_everything_is_one_range() {
        let index = spatial_index(MAX_ORDER);
        let predicate = format!("{}", all_sky().prefilter(None, &index, lit(false)));
        assert!(
            predicate.contains(DEFAULT_HEALPIX_COLUMN_NAME) && predicate.contains("OR"),
            "the whole sky should be a range the geometry sits behind: {predicate}"
        );
        let nothing = Coverage::of(
            &[],
            Detail::Rows {
                partition_order: None,
            },
        );
        assert_eq!(
            format!("{}", nothing.prefilter(None, &index, lit(false))),
            format!("{}", lit(false)),
            "an empty covering should leave the predicate alone"
        );
    }

    /// The column and the order have to agree about what the column holds.
    ///
    /// Both halves matter. An order past what any 64-bit integer indexes is nothing a file
    /// could hold; an order past what *this* column holds is a request describing some other
    /// file, and answering it would compare every row against a bound the column cannot
    /// reach. The deepest order each type does hold is the case that says the arithmetic is
    /// the type's own rather than a guess at it.
    #[test]
    fn the_column_and_the_order_have_to_fit_each_other() {
        assert!(resolve(DataType::Int64, MAX_ORDER + 1).is_err());
        assert!(resolve(DataType::Int64, MAX_ORDER).is_ok());
        assert!(resolve(DataType::Float64, 5).is_err());

        // 12 * 4^k cells, against what each type holds: an unsigned type reaches one order
        // deeper than the signed type of the same size.
        for (cell_type, deepest) in [
            (DataType::Int8, 1_u8),
            (DataType::UInt8, 2),
            (DataType::Int16, 5),
            (DataType::UInt16, 6),
            (DataType::Int32, 13),
            (DataType::UInt32, 14),
        ] {
            assert!(
                resolve(cell_type.clone(), deepest).is_ok(),
                "{cell_type:?} should hold order {deepest}"
            );
            assert!(
                resolve(cell_type.clone(), deepest + 1).is_err(),
                "{cell_type:?} should not hold order {}",
                deepest + 1
            );
        }
    }

    /// A one-column schema of the type given, with the column resolved against it.
    fn resolve(cell_type: DataType, order: u8) -> Result<SpatialIndex, ApiError> {
        use datafusion::arrow::datatypes::{Field, Schema};

        let schema = DFSchema::try_from(Schema::new(vec![Field::new(
            DEFAULT_HEALPIX_COLUMN_NAME,
            cell_type,
            false,
        )]))
        .unwrap();
        SpatialIndex::resolve(&schema, DEFAULT_HEALPIX_COLUMN_NAME, order, "healpix_order")
    }

    fn spatial_index(order: u8) -> SpatialIndex {
        resolve(DataType::Int64, order).unwrap()
    }

    /// Whether a point is inside a shape, worked out from the definition rather than from
    /// anything the covering shares with it.
    fn contains(shape: &Shape, ra: f64, dec: f64) -> bool {
        match *shape {
            Shape::Circle {
                ra: centre_ra,
                dec: centre_dec,
                radius,
            } => {
                let separation = (dec.to_radians().sin() * centre_dec.to_radians().sin()
                    + dec.to_radians().cos()
                        * centre_dec.to_radians().cos()
                        * (ra - centre_ra).to_radians().cos())
                .clamp(-1.0, 1.0)
                .acos();
                separation <= radius.to_radians()
            }
            Shape::Box {
                ra_from,
                ra_span,
                dec_from,
                dec_to,
            } => (dec_from..=dec_to).contains(&dec) && (ra - ra_from).rem_euclid(360.0) <= ra_span,
        }
    }

    /// Points to ask about: a lattice fine enough to land inside the small shapes, and
    /// carried right up to both poles.
    fn grid() -> impl Iterator<Item = (f64, f64)> {
        (0..360).flat_map(|ra| {
            (-90..=90).map(move |dec| (f64::from(ra) + 0.37, f64::from(dec) * 0.999))
        })
    }
}
