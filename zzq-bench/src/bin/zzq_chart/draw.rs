//! SVG charts in the omq.rs style. Each row has one panel per QoS level,
//! with msg/s dashed on the left axis and GB/s solid on the right axis. A
//! legend goes below the rows. Broker or client CPU goes in the legend or
//! under each panel's x axis.

use std::collections::BTreeMap;
use std::error::Error;
use std::fmt::Write as _;
use std::ops::Range;
use std::path::{Path, PathBuf};

use plotters::chart::DualCoordChartContext;
use plotters::coord::CoordTranslate;
use plotters::coord::Shift;
use plotters::coord::ranged1d::{DefaultFormatting, KeyPointHint, Ranged};
use plotters::coord::types::RangedCoordf64;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

pub(crate) type Result<T = ()> = std::result::Result<T, Box<dyn Error>>;

pub(crate) const WIDTH: u32 = 1440;
/// Panels per row: QoS 0, 1 and 2.
const COLUMNS: u8 = 3;
/// Room for the title and hardware line above the first row.
pub(crate) const TOP: u32 = 56;
pub(crate) const PANEL_H: u32 = 260;
pub(crate) const ROW_GAP: u32 = 50;
const LEFT_LABELS: u32 = 62;
const RIGHT_LABELS: u32 = 56;
const LEGEND_ROW_H: i32 = 16;
pub(crate) const N_TICKS: usize = 6;
/// Height of one line of CPU values under a panel.
const CPU_LINE_H: u32 = 12;
/// X padding, in sizes, so the first and last points sit off the edges.
const X_PAD: f64 = 0.12;

pub(crate) const BACKGROUND: RGBColor = RGBColor(0, 0, 0);
pub(crate) const GRID: RGBColor = RGBColor(55, 65, 81);
pub(crate) const AXIS: RGBColor = RGBColor(156, 163, 175);
pub(crate) const TEXT: RGBColor = RGBColor(229, 231, 235);
pub(crate) const MUTED: RGBColor = RGBColor(156, 163, 175);
const TITLE_FILL: &str = "#F9FAFB";
const MUTED_FILL: &str = "#9CA3AF";

/// One implementation: a color and a legend row.
pub(crate) struct Impl {
    /// `impl` in the cache.
    pub(crate) key: &'static str,
    /// `config` in the cache; brokers only.
    pub(crate) config: &'static str,
    pub(crate) label: &'static str,
    pub(crate) runs_on: &'static str,
    pub(crate) color: RGBColor,
    /// zzq itself: no version in the legend.
    pub(crate) ours: bool,
}

/// Impl, QoS and message size.
pub(crate) type Key = (&'static str, u8, u64);

#[derive(Clone, Copy)]
pub(crate) struct Latency {
    pub(crate) p50: f64,
    pub(crate) p99: f64,
    pub(crate) p999: f64,
}

pub(crate) enum RowData {
    /// msg/s per cell.
    Throughput(BTreeMap<Key, f64>),
    /// Round trip per cell.
    Rtt(BTreeMap<Key, Latency>),
}

impl RowData {
    /// The QoS levels this row has panels for.
    pub(crate) const fn qos_levels(&self) -> u8 {
        match self {
            RowData::Throughput(_) => 3,
            RowData::Rtt(_) => 2,
        }
    }

    pub(crate) fn has(&self, key: &Key) -> bool {
        match self {
            RowData::Throughput(m) => m.contains_key(key),
            RowData::Rtt(m) => m.contains_key(key),
        }
    }
}

pub(crate) struct Row {
    pub(crate) title: &'static str,
    pub(crate) data: RowData,
    /// CPU percent per cell.
    pub(crate) cpu: BTreeMap<Key, f64>,
}

/// Where a chart shows CPU.
#[derive(Clone, Copy, PartialEq, Eq)]
pub(crate) enum CpuShow {
    /// A legend column with each impl's median over all cells.
    Legend,
    /// A line per impl under each panel, a value per size.
    BelowAxis,
}

pub(crate) struct Chart<'a> {
    pub(crate) out: PathBuf,
    pub(crate) title: &'a str,
    pub(crate) sizes: &'a [u64],
    pub(crate) impls: &'a [Impl],
    /// Version per impl key, from its newest run.
    pub(crate) versions: BTreeMap<&'static str, String>,
    pub(crate) rows: Vec<Row>,
    pub(crate) cpu: CpuShow,
    /// Whose CPU, for the legend: broker or client.
    pub(crate) cpu_of: &'a str,
    pub(crate) notes: &'a [&'a str],
}

impl Chart<'_> {
    /// The impls with CPU values anywhere in the chart.
    fn cpu_impls(&self) -> Vec<&Impl> {
        self.impls
            .iter()
            .filter(|i| self.rows.iter().any(|r| r.cpu.keys().any(|k| k.0 == i.key)))
            .collect()
    }

    /// Extra height under each panel for the CPU lines.
    fn cpu_h(&self) -> u32 {
        match self.cpu {
            CpuShow::BelowAxis => {
                u32::try_from(self.cpu_impls().len()).unwrap_or(0) * CPU_LINE_H + 6
            }
            CpuShow::Legend => 0,
        }
    }
}

pub(crate) type Area<'a> = DrawingArea<SVGBackend<'a>, Shift>;
type Coord = Cartesian2d<SizeAxis, RangedCoordf64>;
type DualChart<'a, 'b> = DualCoordChartContext<'a, SVGBackend<'b>, Coord, Coord>;

/// Message size index, padded, with one tick per size.
#[derive(Clone)]
pub(crate) struct SizeAxis {
    range: RangedCoordf64,
    n: usize,
}

impl SizeAxis {
    pub(crate) fn new(n: usize) -> SizeAxis {
        let last = n.saturating_sub(1) as f64;
        SizeAxis {
            range: (-X_PAD..last + X_PAD).into(),
            n,
        }
    }
}

impl Ranged for SizeAxis {
    type ValueType = f64;
    type FormatOption = DefaultFormatting;

    fn map(&self, value: &f64, limit: (i32, i32)) -> i32 {
        self.range.map(value, limit)
    }

    fn key_points<Hint: KeyPointHint>(&self, _hint: Hint) -> Vec<f64> {
        (0..self.n).map(|i| i as f64).collect()
    }

    fn range(&self) -> Range<f64> {
        self.range.range()
    }
}

/// Draws the chart and writes it to `chart.out`.
pub(crate) fn draw(chart: &Chart<'_>) -> Result {
    let n_rows = u32::try_from(chart.rows.len())?;
    let legend_h = legend_height(chart.impls.len(), chart.notes.len());
    let panel_h = PANEL_H + chart.cpu_h();
    let body_h = TOP + n_rows * panel_h + n_rows.saturating_sub(1) * ROW_GAP;
    let height = body_h + legend_h;
    if let Some(dir) = chart.out.parent() {
        std::fs::create_dir_all(dir)?;
    }

    let root = SVGBackend::new(&chart.out, (WIDTH, height)).into_drawing_area();
    root.fill(&BACKGROUND)?;
    let mut titles = Vec::new();
    for (i, row) in (0..).zip(&chart.rows) {
        let y = TOP + i * (panel_h + ROW_GAP);
        titles.push((y - 6, row.title));
        let area = root.clone().shrink((0, y), (WIDTH, panel_h));
        let panels = area.split_evenly((1, usize::from(COLUMNS)));
        for (qos, panel) in (0..).zip(&panels) {
            match &row.data {
                RowData::Throughput(msgs) => throughput_panel(panel, chart, row, msgs, qos)?,
                RowData::Rtt(lat) if qos < row.data.qos_levels() => {
                    rtt_panel(panel, chart, row, lat, qos)?;
                }
                RowData::Rtt(_) => not_measured(panel, qos)?,
            }
        }
    }
    let legend_area = root.clone().shrink((0, body_h), (WIDTH, legend_h));
    let cpu = (chart.cpu == CpuShow::Legend).then(|| median_cpu(chart));
    let spec = Legend {
        impls: chart.impls,
        versions: &chart.versions,
        notes: chart.notes,
        line_key: true,
        cpu: cpu.as_ref().map(|c| (format!("{} CPU", chart.cpu_of), c)),
    };
    legend(&legend_area, &spec)?;
    root.present()?;
    drop(root);

    let header = Header {
        width: WIDTH,
        height,
        title: chart.title,
        hardware: detect_hardware(),
    };
    postprocess(&chart.out, &header, &titles)
}

/// Each impl's median CPU over all cells of the chart.
fn median_cpu(chart: &Chart<'_>) -> BTreeMap<&'static str, f64> {
    let mut all: BTreeMap<&'static str, Vec<f64>> = BTreeMap::new();
    for row in &chart.rows {
        for (&(key, _, _), &v) in &row.cpu {
            all.entry(key).or_default().push(v);
        }
    }
    all.into_iter()
        .map(|(key, mut v)| {
            v.sort_by(f64::total_cmp);
            (key, v[v.len() / 2])
        })
        .collect()
}

/// Plot points of the sizes `value` has a value for.
fn points(sizes: &[u64], value: impl Fn(u64) -> Option<f64>) -> Vec<(f64, f64)> {
    sizes
        .iter()
        .enumerate()
        .filter_map(|(i, &size)| value(size).map(|v| (i as f64, v)))
        .collect()
}

fn gbs(size: u64, msgs_s: f64) -> f64 {
    msgs_s * size as f64 / 1e9
}

/// One throughput panel for one QoS level: msg/s dashed on the left axis,
/// GB/s solid on the right. The first impl draws last, on top.
fn throughput_panel(
    area: &Area<'_>,
    chart: &Chart<'_>,
    row: &Row,
    msgs: &BTreeMap<Key, f64>,
    qos: u8,
) -> Result {
    let sizes = chart.sizes;
    let cells = || msgs.iter().filter(|((_, q, _), _)| *q == qos);
    let peak_msgs = cells().map(|(_, &m)| m).fold(0.0, f64::max);
    let peak_gbs = cells()
        .map(|(&(_, _, size), &m)| gbs(size, m))
        .fold(0.0, f64::max);
    let (msgs_max, msgs_ticks) = nice_axis(peak_msgs, N_TICKS);
    let (gbs_max, gbs_ticks) = nice_axis(peak_gbs, N_TICKS);

    let mut cc = panel(area, &format!("QoS {qos}"), true, chart.cpu_h())
        .build_cartesian_2d(SizeAxis::new(sizes.len()), 0.0..msgs_max)?
        .set_secondary_coord(SizeAxis::new(sizes.len()), 0.0..gbs_max);
    configure_mesh(&mut cc, sizes, msgs_ticks, gbs_ticks)?;
    cpu_lines(area, &cc, chart, row, qos)?;

    for imp in chart.impls.iter().rev() {
        let color = imp.color;
        let get = |size| msgs.get(&(imp.key, qos, size)).copied();
        let pts = points(sizes, get);
        cc.draw_series(DashedLineSeries::new(
            pts.iter().copied(),
            6,
            3,
            color.stroke_width(2),
        ))?;
        cc.draw_series(pts.iter().map(|&p| Circle::new(p, 2, color.filled())))?;

        let pts = points(sizes, |size| get(size).map(|m| gbs(size, m)));
        cc.draw_secondary_series(LineSeries::new(pts.iter().copied(), color.stroke_width(2)))?;
        cc.draw_secondary_series(pts.iter().map(|&p| Circle::new(p, 2, color.filled())))?;
    }
    Ok(())
}

/// A captioned panel. Label areas are the same in every panel, so plots
/// line up; without `right_labels`, a margin takes the place of the right
/// axis labels. `below` leaves room under the x axis labels.
pub(crate) fn panel<'a, 'b>(
    area: &'a Area<'b>,
    caption: &str,
    right_labels: bool,
    below: u32,
) -> ChartBuilder<'a, 'b, SVGBackend<'b>> {
    let right = if right_labels { RIGHT_LABELS } else { 0 };
    let mut builder = ChartBuilder::on(area);
    builder
        .caption(caption, ("sans-serif", 12).into_font().color(&TEXT))
        .set_label_area_size(LabelAreaPosition::Bottom, 24 + below)
        .set_label_area_size(LabelAreaPosition::Left, LEFT_LABELS)
        .set_label_area_size(LabelAreaPosition::Right, right)
        .margin_top(2)
        .margin_left(6)
        .margin_right(6 + RIGHT_LABELS - right);
    builder
}

fn configure_mesh(
    cc: &mut DualChart<'_, '_>,
    sizes: &[u64],
    msgs_ticks: usize,
    gbs_ticks: usize,
) -> Result {
    cc.configure_mesh()
        .x_label_formatter(&|v| size_label(sizes, *v))
        .y_labels(msgs_ticks + 1)
        .y_label_formatter(&|v| fmt_msgs(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    cc.configure_secondary_axes()
        .y_labels(gbs_ticks + 1)
        .y_label_formatter(&|v| fmt_gbs(*v))
        .label_style(("sans-serif", 10).into_font().color(&TEXT))
        .axis_style(AXIS)
        .draw()?;
    Ok(())
}

/// One round-trip panel for one QoS level: p99 lines with whiskers from
/// p50 to p99.9. The axis fits the p99 lines; a whisker past the top ends
/// in a marker and a label with its p99.9.
fn rtt_panel(
    area: &Area<'_>,
    chart: &Chart<'_>,
    row: &Row,
    lat: &BTreeMap<Key, Latency>,
    qos: u8,
) -> Result {
    let sizes = chart.sizes;
    let peak = lat
        .iter()
        .filter(|((_, q, _), _)| *q == qos)
        .map(|(_, l)| l.p99)
        .fold(0.0, f64::max);
    let (top, ticks) = nice_axis(peak * 1.6, N_TICKS);

    let mut cc = panel(area, &format!("QoS {qos}"), false, chart.cpu_h())
        .build_cartesian_2d(SizeAxis::new(sizes.len()), 0.0..top)?;
    cc.configure_mesh()
        .x_label_formatter(&|v| size_label(sizes, *v))
        .y_labels(ticks + 1)
        .y_label_formatter(&|v| format!("{v:.0} \u{03bc}s"))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    cpu_lines(area, &cc, chart, row, qos)?;

    // Each label gets its own line, so labels at nearby sizes cannot meet.
    let mut labels = 0;
    for imp in chart.impls.iter().rev() {
        for (i, &size) in sizes.iter().enumerate() {
            let Some(l) = lat.get(&(imp.key, qos, size)) else {
                continue;
            };
            whisker(&mut cc, i as f64, l, top, imp.color)?;
            if l.p999 > top {
                let right = i + 1 < sizes.len();
                clip_label(&mut cc, (i as f64, top), l.p999, imp.color, (right, labels))?;
                labels += 1;
            }
        }
        let pts = points(sizes, |size| lat.get(&(imp.key, qos, size)).map(|l| l.p99));
        cc.draw_series(LineSeries::new(
            pts.iter().copied(),
            imp.color.stroke_width(2),
        ))?;
        cc.draw_series(pts.iter().map(|&p| Circle::new(p, 2, imp.color.filled())))?;
    }
    Ok(())
}

/// The slot of a QoS level the row has no results for.
fn not_measured(area: &Area<'_>, qos: u8) -> Result {
    let (w, h) = area.dim_in_pixel();
    let style = ("sans-serif", 12)
        .into_font()
        .color(&MUTED)
        .pos(Pos::new(HPos::Center, VPos::Center));
    let at = (i32::try_from(w / 2)?, i32::try_from(h / 2)?);
    area.draw_text(&format!("QoS {qos}: not measured"), &style, at)?;
    Ok(())
}

/// Under the x axis labels, a line of CPU values per impl in its color, one
/// under each size, when the chart shows CPU there.
fn cpu_lines<C: CoordTranslate<From = (f64, f64)>>(
    area: &Area<'_>,
    cc: &ChartContext<'_, SVGBackend<'_>, C>,
    chart: &Chart<'_>,
    row: &Row,
    qos: u8,
) -> Result {
    if chart.cpu != CpuShow::BelowAxis {
        return Ok(());
    }
    let base = area.get_base_pixel();
    let bottom = cc.plotting_area().get_pixel_range().1.end;
    let muted = ("sans-serif", 9)
        .into_font()
        .color(&MUTED)
        .pos(Pos::new(HPos::Right, VPos::Top));
    for (line, imp) in (0..).zip(chart.cpu_impls()) {
        let style = ("sans-serif", 9)
            .into_font()
            .color(&imp.color)
            .pos(Pos::new(HPos::Center, VPos::Top));
        for (i, &size) in chart.sizes.iter().enumerate() {
            let (x, _) = cc.as_coord_spec().translate(&(i as f64, 1.0));
            let at = (
                x - base.0,
                bottom - base.1 + 24 + line * CPU_LINE_H.cast_signed(),
            );
            if i == 0 && line == 0 {
                area.draw_text("CPU %", &muted, (at.0 - 24, at.1))?;
            }
            if let Some(v) = row.cpu.get(&(imp.key, qos, size)) {
                area.draw_text(&format!("{v:.0}"), &style, at)?;
            }
        }
    }
    Ok(())
}

/// A whisker from p50 to p99.9, cut at `top`.
pub(crate) fn whisker<C: CoordTranslate<From = (f64, f64)>>(
    cc: &mut ChartContext<'_, SVGBackend<'_>, C>,
    x: f64,
    l: &Latency,
    top: f64,
    color: RGBColor,
) -> Result {
    let style = color.mix(0.65).stroke_width(1);
    let high = l.p999.min(top);
    cc.draw_series([
        PathElement::new(vec![(x, l.p50), (x, high)], style),
        PathElement::new(vec![(x - 0.04, l.p50), (x + 0.04, l.p50)], style),
        PathElement::new(vec![(x - 0.04, high), (x + 0.04, high)], style),
    ])?;
    Ok(())
}

/// Marks a whisker cut at `at` and labels its p99.9, right of the point
/// unless `right` is false, in label row `row`.
pub(crate) fn clip_label<C: CoordTranslate<From = (f64, f64)>>(
    cc: &mut ChartContext<'_, SVGBackend<'_>, C>,
    at: (f64, f64),
    p999: f64,
    color: RGBColor,
    (right, row): (bool, i32),
) -> Result {
    cc.draw_series(std::iter::once(TriangleMarker::new(at, 4, color.filled())))?;
    let (dx, h) = if right {
        (8, HPos::Left)
    } else {
        (-8, HPos::Right)
    };
    let style = ("sans-serif", 10)
        .into_font()
        .color(&color)
        .pos(Pos::new(h, VPos::Top));
    cc.draw_series(std::iter::once(
        EmptyElement::at(at)
            + Text::new(
                format!("p99.9: {p999:.0} \u{03bc}s"),
                (dx, 2 + 12 * row),
                style,
            ),
    ))?;
    Ok(())
}

/// Header row, one row per impl, then the notes.
pub(crate) fn legend_height(impls: usize, notes: usize) -> u32 {
    let rows = u32::try_from(impls).unwrap_or(u32::MAX);
    let notes = u32::try_from(notes).unwrap_or(u32::MAX);
    24 + rows.saturating_mul(16) + 14 + notes.saturating_mul(13)
}

/// What a legend lists.
pub(crate) struct Legend<'a> {
    pub(crate) impls: &'a [Impl],
    pub(crate) versions: &'a BTreeMap<&'static str, String>,
    pub(crate) notes: &'a [&'a str],
    /// The key for the dashed msg/s and solid GB/s lines.
    pub(crate) line_key: bool,
    /// A column header and a CPU percent per impl.
    pub(crate) cpu: Option<(String, &'a BTreeMap<&'static str, f64>)>,
}

/// Impl swatches with versions and where each runs, optionally CPU and a
/// key for the line styles, then method notes.
pub(crate) fn legend(area: &Area<'_>, spec: &Legend<'_>) -> Result {
    let header = ("sans-serif", 11).into_font().color(&MUTED);
    let value = ("sans-serif", 11).into_font().color(&TEXT);
    let note = ("sans-serif", 10).into_font().color(&MUTED);
    let (col_swatch, col_runs, col_cpu, col_key) = (78, 290, 470, 620);

    area.draw_text("runs on", &header, (col_runs, 4))?;
    if let Some((title, _)) = &spec.cpu {
        area.draw_text(title, &header, (col_cpu, 4))?;
    }
    let mut y = 20;
    for imp in spec.impls {
        swatch(area, (col_swatch, y + 6), imp.color)?;
        let label = match spec.versions.get(imp.key) {
            Some(v) if !imp.ours => format!("{} v{v}", imp.label),
            _ => imp.label.to_owned(),
        };
        area.draw_text(&label, &value, (col_swatch + 20, y))?;
        area.draw_text(imp.runs_on, &header, (col_runs, y))?;
        if let Some(v) = spec.cpu.as_ref().and_then(|(_, c)| c.get(imp.key)) {
            area.draw_text(&format!("{v:.0}%"), &value, (col_cpu, y))?;
        }
        y += LEGEND_ROW_H;
    }

    if spec.line_key {
        let style = TEXT.stroke_width(2);
        for x in [col_key, col_key + 9] {
            area.draw(&PathElement::new(vec![(x, 26), (x + 6, 26)], style))?;
        }
        area.draw_text("msg/s (left axis)", &value, (col_key + 24, 20))?;
        swatch(area, (col_key, 42), TEXT)?;
        area.draw_text("GB/s (right axis)", &value, (col_key + 24, 36))?;
    }

    for (i, text) in (0..).zip(spec.notes.iter()) {
        area.draw_text(text, &note, (col_swatch, y + 8 + i * 13))?;
    }
    Ok(())
}

fn swatch(area: &Area<'_>, (x, y): (i32, i32), color: RGBColor) -> Result {
    area.draw(&PathElement::new(
        vec![(x, y), (x + 14, y)],
        color.stroke_width(2),
    ))?;
    Ok(())
}

fn size_label(sizes: &[u64], v: f64) -> String {
    let i = v.round();
    if i < 0.0 {
        return String::new();
    }
    sizes
        .get(i as usize)
        .map_or_else(String::new, |&s| fmt_size(s))
}

fn fmt_size(b: u64) -> String {
    if b >= 1 << 20 {
        format!("{} MiB", b >> 20)
    } else if b >= 1 << 10 {
        format!("{} KiB", b >> 10)
    } else {
        format!("{b} B")
    }
}

pub(crate) fn fmt_msgs(v: f64) -> String {
    if v >= 1e6 {
        let n = v / 1e6;
        if (n - n.round()).abs() < 0.05 {
            format!("{n:.0}M/s")
        } else {
            format!("{n:.1}M/s")
        }
    } else if v >= 1e3 {
        format!("{:.0}K/s", v / 1e3)
    } else {
        format!("{v:.0}/s")
    }
}

fn fmt_gbs(v: f64) -> String {
    if v >= 1.0 {
        if (v - v.round()).abs() < 0.05 {
            format!("{v:.0} GB/s")
        } else {
            format!("{v:.1} GB/s")
        }
    } else if v >= 0.01 {
        format!("{:.0} MB/s", v * 1000.0)
    } else if v > 0.0 {
        format!("{:.1} MB/s", v * 1000.0)
    } else {
        String::new()
    }
}

/// A round axis maximum at or above `max` and its tick count.
pub(crate) fn nice_axis(max: f64, target: usize) -> (f64, usize) {
    if max <= 0.0 {
        return (1.0, 1);
    }
    let raw = max / target as f64;
    let mag = 10f64.powf(raw.log10().floor());
    let step = [1.0, 2.0, 2.5, 5.0, 10.0]
        .into_iter()
        .map(|s| s * mag)
        .find(|step| max / step <= target as f64 + 1.0)
        .unwrap_or(mag * 10.0);
    let ticks = (max / step).ceil().max(1.0);
    (step * ticks, ticks as usize)
}

/// The hardware line under the title: `prefix` and `postfix` from the
/// ignored `.chart_hw` in the repo root, or `ZZQ_HW_PREFIX` and
/// `ZZQ_HW_POSTFIX`.
pub(crate) fn detect_hardware() -> Option<String> {
    let path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../.chart_hw");
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let get = |key: &str, env: &str| {
        std::env::var(env).ok().or_else(|| {
            text.lines()
                .map(str::trim)
                .filter(|line| !line.starts_with('#'))
                .filter_map(|line| line.split_once('='))
                .find(|(k, _)| k.trim() == key)
                .map(|(_, v)| v.trim().to_owned())
        })
    };
    let parts: Vec<String> = [
        get("prefix", "ZZQ_HW_PREFIX"),
        get("postfix", "ZZQ_HW_POSTFIX"),
    ]
    .into_iter()
    .flatten()
    .collect();
    (!parts.is_empty()).then(|| parts.join(", "))
}

pub(crate) struct Header<'a> {
    pub(crate) width: u32,
    pub(crate) height: u32,
    pub(crate) title: &'a str,
    pub(crate) hardware: Option<String>,
}

/// Makes the SVG scale, sets the font, and adds the title, the hardware
/// line and the row titles, which plotters cannot place.
pub(crate) fn postprocess(path: &Path, h: &Header<'_>, row_titles: &[(u32, &str)]) -> Result {
    let (width, height) = (h.width, h.height);
    let mut svg = std::fs::read_to_string(path)?;
    svg = svg.replacen(
        &format!("<svg width=\"{width}\" height=\"{height}\" viewBox=\"0 0 {width} {height}\""),
        &format!("<svg viewBox=\"0 0 {width} {height}\""),
        1,
    );
    svg = svg.replacen(
        "xmlns=\"http://www.w3.org/2000/svg\"",
        "xmlns=\"http://www.w3.org/2000/svg\" font-family=\"system-ui, -apple-system, sans-serif\"",
        1,
    );

    let mid = width / 2;
    let mut text = String::new();
    let mut line = |y: u32, size: u32, bold: bool, fill: &str, s: &str| {
        let weight = if bold { " font-weight=\"bold\"" } else { "" };
        let _ = write!(
            text,
            "\n<text x=\"{mid}\" y=\"{y}\" text-anchor=\"middle\" font-family=\"sans-serif\" \
             font-size=\"{size}\"{weight} fill=\"{fill}\">{}</text>",
            escape(s)
        );
    };
    line(17, 14, true, TITLE_FILL, h.title);
    if let Some(hw) = &h.hardware {
        line(31, 10, false, MUTED_FILL, hw);
    }
    for &(y, title) in row_titles {
        line(y, 13, true, TITLE_FILL, title);
    }
    svg = svg.replace("r=\"2\"", "r=\"2.5\"");
    let end = svg
        .rfind("</svg>")
        .ok_or("no </svg> in the plotters output")?;
    svg.insert_str(end, &text);
    std::fs::write(path, svg)?;
    Ok(())
}

fn escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn axes_round_up_and_labels_read_well() {
        assert_eq!(nice_axis(11.6e6, 6), (12e6, 6));
        assert_eq!(nice_axis(0.0, 6), (1.0, 1));
        assert_eq!(fmt_msgs(2.5e6), "2.5M/s");
        assert_eq!(fmt_gbs(0.004), "4.0 MB/s");
        assert_eq!(fmt_gbs(4.4), "4.4 GB/s");
        assert_eq!(size_label(&[16, 4096], 1.0), "4 KiB");
        assert_eq!(size_label(&[16, 4096], -0.1), "16 B");
    }
}
