//! The fixed-load chart: brokers with many connections at offered rates.
//! Each row is one scenario, with panels for latency, broker CPU and broker
//! memory over the offered rate. A failed step gets a cross, and its line
//! ends before it. A list right of the panels says why each step failed.

use std::collections::BTreeMap;
use std::path::PathBuf;

use plotters::coord::CoordTranslate;
use plotters::prelude::*;
use plotters::style::text_anchor::{HPos, Pos, VPos};

use crate::draw::{
    self, AXIS, BACKGROUND, GRID, Header, Impl, Latency, Legend, MUTED, N_TICKS, PANEL_H, ROW_GAP,
    Result, SizeAxis, TEXT, TOP, WIDTH,
};

/// One step of one impl.
pub(crate) struct Step {
    pub(crate) lat: Latency,
    pub(crate) cpu: Option<f64>,
    pub(crate) rss: Option<f64>,
    /// Delivered over offered, in percent.
    pub(crate) delivered_pct: f64,
    pub(crate) closed: u64,
    pub(crate) failed: bool,
}

pub(crate) struct Scenario {
    pub(crate) title: &'static str,
    /// Offered deliveries per second, one per step.
    pub(crate) rates: Vec<u64>,
    /// Per impl and offered rate.
    pub(crate) steps: BTreeMap<(&'static str, u64), Step>,
}

pub(crate) struct FixedChart<'a> {
    pub(crate) out: PathBuf,
    pub(crate) title: &'a str,
    pub(crate) impls: &'a [Impl],
    /// Version per impl key, from its newest run.
    pub(crate) versions: BTreeMap<&'static str, String>,
    pub(crate) rows: Vec<Scenario>,
    pub(crate) notes: &'a [&'a str],
}

/// The lowest latency on the axis, in microseconds.
const LAT_FLOOR: f64 = 10.0;

/// Width of the list of failed steps right of each row.
const FAILS_W: u32 = 230;

/// Height of one line in the list of failed steps.
const FAIL_LINE_H: i32 = 14;

/// The load generator fails a step that delivers less than this share of
/// the offered rate, in percent.
const MIN_DELIVERED_PCT: f64 = 95.0;

/// Draws the chart and writes it to `chart.out`.
pub(crate) fn draw(chart: &FixedChart<'_>) -> Result {
    let n_rows = u32::try_from(chart.rows.len())?;
    let legend_h = draw::legend_height(chart.impls.len(), chart.notes.len());
    let body_h = TOP + n_rows * PANEL_H + n_rows.saturating_sub(1) * ROW_GAP;
    let height = body_h + legend_h;
    if let Some(dir) = chart.out.parent() {
        std::fs::create_dir_all(dir)?;
    }

    let root = SVGBackend::new(&chart.out, (WIDTH, height)).into_drawing_area();
    root.fill(&BACKGROUND)?;
    let mut titles = Vec::new();
    for (i, row) in (0..).zip(&chart.rows) {
        let y = TOP + i * (PANEL_H + ROW_GAP);
        titles.push((y - 6, row.title));
        let area = root.clone().shrink((0, y), (WIDTH, PANEL_H));
        let (plots, fails) = area.split_horizontally(WIDTH - FAILS_W);
        let panels = plots.split_evenly((1, 3));
        latency_panel(&panels[0], chart, row)?;
        value_panel(&panels[1], chart, row, &CPU)?;
        value_panel(&panels[2], chart, row, &RSS)?;
        failure_list(&fails, chart, row)?;
    }
    let spec = Legend {
        impls: chart.impls,
        versions: &chart.versions,
        notes: chart.notes,
        line_key: false,
        cpu: None,
    };
    draw::legend(&root.clone().shrink((0, body_h), (WIDTH, legend_h)), &spec)?;
    root.present()?;
    drop(root);

    let header = Header {
        width: WIDTH,
        height,
        title: chart.title,
        hardware: draw::detect_hardware(),
    };
    draw::postprocess(&chart.out, &header, &titles)
}

fn rate_label(rates: &[u64], v: f64) -> String {
    let i = v.round();
    if i < 0.0 {
        return String::new();
    }
    rates
        .get(i as usize)
        .map_or_else(String::new, |&r| draw::fmt_msgs(r as f64))
}

/// p99 lines with whiskers from p50 to p99.9 on a log axis. A failed step
/// is a cross at its p99, or at the top of the axis.
fn latency_panel(area: &draw::Area<'_>, chart: &FixedChart<'_>, row: &Scenario) -> Result {
    let n = row.rates.len();
    let peak = row
        .steps
        .values()
        .filter(|s| !s.failed)
        .map(|s| s.lat.p999)
        .fold(1000.0, f64::max);
    let top = 10f64.powf(peak.log10().ceil());
    let mut cc = draw::panel(area, "latency: p99, whiskers p50 to p99.9", false, 0)
        .build_cartesian_2d(SizeAxis::new(n), (LAT_FLOOR..top).log_scale())?;
    cc.configure_mesh()
        .x_label_formatter(&|v| rate_label(&row.rates, *v))
        .y_label_formatter(&|v| draw::fmt_us(*v))
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;

    let clamp = |v: f64| v.clamp(LAT_FLOOR, top);
    for imp in chart.impls.iter().rev() {
        let mut line = Vec::new();
        for (i, &rate) in row.rates.iter().enumerate() {
            let Some(s) = row.steps.get(&(imp.key, rate)) else {
                continue;
            };
            let x = i as f64;
            if s.failed {
                // A step without samples has no latency to place it at.
                let mark = (x, Some(s.lat.p99).filter(|&v| v > 0.0).map_or(top, clamp));
                cross(&mut cc, mark, imp.color)?;
                continue;
            }
            let l = Latency {
                p50: clamp(s.lat.p50),
                p99: clamp(s.lat.p99),
                p999: clamp(s.lat.p999),
            };
            draw::whisker(&mut cc, x, &l, top, imp.color)?;
            line.push((x, l.p99));
        }
        polyline(&mut cc, &line, imp.color)?;
    }
    Ok(())
}

/// One line per impl with a failed step, in legend order: a mark in the
/// impl's color, then its name, the step and why it failed.
fn failure_list(area: &draw::Area<'_>, chart: &FixedChart<'_>, row: &Scenario) -> Result {
    let at = |color: &RGBColor, size| {
        ("sans-serif", size)
            .into_font()
            .color(color)
            .pos(Pos::new(HPos::Left, VPos::Center))
    };
    area.draw(&Text::new("failed steps", (0, 8), at(&TEXT, 12)))?;
    let mut y = 8 + 2 * FAIL_LINE_H;
    for imp in chart.impls {
        let failed = row.rates.iter().enumerate().find_map(|(i, &rate)| {
            let s = row.steps.get(&(imp.key, rate))?;
            s.failed.then_some((i, s))
        });
        let Some((i, s)) = failed else { continue };
        area.draw(&PathElement::new(
            [(0, y), (14, y)],
            imp.color.stroke_width(2),
        ))?;
        let name = if imp.ours {
            format!("{} ({})", imp.label, imp.runs_on)
        } else {
            imp.label.to_owned()
        };
        let text = format!(
            "{name}, {}: {}",
            rate_label(&row.rates, i as f64),
            failure(s)
        );
        area.draw(&Text::new(text, (20, y), at(&TEXT, 10)))?;
        y += FAIL_LINE_H;
    }
    if y == 8 + 2 * FAIL_LINE_H {
        area.draw(&Text::new("none", (0, y), at(&MUTED, 10)))?;
    }
    Ok(())
}

/// Why a step failed, in a few words.
fn failure(s: &Step) -> String {
    let mut why = Vec::new();
    if s.delivered_pct < MIN_DELIVERED_PCT {
        why.push(format!("{:.0}% delivered", s.delivered_pct));
    }
    if s.closed > 0 {
        why.push(format!("{} closed", s.closed));
    }
    if why.is_empty() {
        why.push(format!("p50 {}", draw::fmt_us(s.lat.p50)));
    }
    why.join(", ")
}

/// What a value panel shows: its caption, a step's value, the unit on its
/// axis, and whether the axis is log, for values orders of magnitude apart.
struct Metric {
    caption: &'static str,
    value: fn(&Step) -> Option<f64>,
    unit: &'static str,
    log: bool,
}

const CPU: Metric = Metric {
    caption: "broker CPU",
    value: |s| s.cpu,
    unit: "%",
    log: false,
};

const RSS: Metric = Metric {
    caption: "broker memory (RSS)",
    value: |s| s.rss,
    unit: " MiB",
    log: true,
};

/// One value per step: lines over the passed steps, a cross on a failed
/// one.
fn value_panel(
    area: &draw::Area<'_>,
    chart: &FixedChart<'_>,
    row: &Scenario,
    metric: &Metric,
) -> Result {
    let n = row.rates.len();
    let values = || row.steps.values().filter_map(metric.value);
    let peak = values().fold(0.0, f64::max);
    let mut panel = draw::panel(area, metric.caption, false, 0);
    let x_label = |v: &f64| rate_label(&row.rates, *v);
    let y_label = |v: &f64| format!("{v:.0}{}", metric.unit);
    if metric.log {
        let low = values().filter(|&v| v > 0.0).fold(f64::MAX, f64::min);
        let axis = draw::log_axis(low, peak);
        let mut cc = panel.build_cartesian_2d(SizeAxis::new(n), axis.log_scale())?;
        cc.configure_mesh()
            .x_label_formatter(&x_label)
            .y_label_formatter(&y_label)
            .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
            .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
            .light_line_style(TRANSPARENT)
            .bold_line_style(GRID)
            .axis_style(AXIS)
            .draw()?;
        return value_lines(&mut cc, chart, row, metric);
    }
    let (top, ticks) = draw::nice_axis(peak, N_TICKS);
    let mut cc = panel.build_cartesian_2d(SizeAxis::new(n), 0.0..top)?;
    cc.configure_mesh()
        .x_label_formatter(&x_label)
        .y_labels(ticks + 1)
        .y_label_formatter(&y_label)
        .y_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .x_label_style(("sans-serif", 10).into_font().color(&TEXT))
        .light_line_style(TRANSPARENT)
        .bold_line_style(GRID)
        .axis_style(AXIS)
        .draw()?;
    value_lines(&mut cc, chart, row, metric)
}

/// The values of `metric`, per impl. A log axis leaves out zeros.
fn value_lines<C: CoordTranslate<From = (f64, f64)>>(
    cc: &mut ChartContext<'_, SVGBackend<'_>, C>,
    chart: &FixedChart<'_>,
    row: &Scenario,
    metric: &Metric,
) -> Result {
    for imp in chart.impls.iter().rev() {
        let mut line = Vec::new();
        for (i, &rate) in row.rates.iter().enumerate() {
            let Some(s) = row.steps.get(&(imp.key, rate)) else {
                continue;
            };
            let Some(v) = (metric.value)(s).filter(|&v| !metric.log || v > 0.0) else {
                continue;
            };
            if s.failed {
                cross(cc, (i as f64, v), imp.color)?;
            } else {
                line.push((i as f64, v));
            }
        }
        polyline(cc, &line, imp.color)?;
    }
    Ok(())
}

/// A line through `points` with a dot on each.
fn polyline<C: CoordTranslate<From = (f64, f64)>>(
    cc: &mut ChartContext<'_, SVGBackend<'_>, C>,
    points: &[(f64, f64)],
    color: RGBColor,
) -> Result {
    cc.draw_series(LineSeries::new(
        points.iter().copied(),
        color.stroke_width(2),
    ))?;
    cc.draw_series(points.iter().map(|&p| Circle::new(p, 2, color.filled())))?;
    Ok(())
}

/// The mark of a failed step.
fn cross<C: CoordTranslate<From = (f64, f64)>>(
    cc: &mut ChartContext<'_, SVGBackend<'_>, C>,
    at: (f64, f64),
    color: RGBColor,
) -> Result {
    cc.draw_series(std::iter::once(Cross::new(at, 5, color.stroke_width(2))))?;
    Ok(())
}
