//! The Pine scripts applied to one chart, run in the background. The chart hands over its bars,
//! market and look each frame with the instances it carries; every visible instance gets a
//! persistent worker (`worker`) that runs it again only when something it reads changed, and the
//! chart draws the latest output it kept. An intrabar OHLCV change is a realtime tick, a new
//! opening time closes the prior bar, and a stale result never overwrites a newer request's.

#[cfg(test)]
mod tests;
mod worker;

use crate::model::pine::instance::{PineInstance, StoredInput, VisualOrder};
use crate::model::{Kline, Timeframe};
use crate::pine::compiler::program::{ChartTheme, SymbolInfo, VisualOutput};
use crate::pine::logs::LogHistory;
use crate::pine::outputs::style_overrides::{StyleOverrides, StyleRow, style_rows};
use crate::pine::syntax::token::Diagnostic;
use crate::pine::time::bucket::seconds_of;
use eframe::egui;
use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};
use worker::{PineWorker, WorkerConfig, WorkerRequest};

/// Whether a script asks where the chart is scrolled (`chart.left_visible_bar_time` and its
/// right-hand twin), the only thing about the viewport a run can depend on.
pub(crate) fn reads_visible_range(source: &str) -> bool {
    source.contains("visible_bar_time")
}

/// A hash of every field of every candle: two series with equal fingerprints run a script the same
/// way. The chart's run revision and the Pine Studio's profiler both identify their data by it.
pub(crate) fn candles_fingerprint(candles: &[Kline]) -> u64 {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    candles.len().hash(&mut hasher);
    for candle in candles {
        candle.open_time.hash(&mut hasher);
        candle.close_time.hash(&mut hasher);
        candle.open.to_bits().hash(&mut hasher);
        candle.high.to_bits().hash(&mut hasher);
        candle.low.to_bits().hash(&mut hasher);
        candle.close.to_bits().hash(&mut hasher);
        candle.volume.to_bits().hash(&mut hasher);
    }
    hasher.finish()
}

/// Whether the last candle has closed by `now_ms`: a forming candle runs a script as a realtime
/// tick, a closed one as a bar.
pub(crate) fn last_candle_is_closed(candles: &[Kline], now_ms: i64) -> bool {
    candles.last().is_some_and(|bar| bar.close_time <= now_ms)
}

/// What a run over a series of bars depends on besides the script: the bars (by fingerprint), the
/// visible range for a script that reads it, and whether the last bar has closed.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RunRevision {
    candles: u64,
    visible_range: Option<(i64, i64)>,
    last_is_closed: bool,
}

impl RunRevision {
    pub(crate) fn of(candles: &[Kline], visible_range: Option<(i64, i64)>, now_ms: i64) -> Self {
        Self {
            candles: candles_fingerprint(candles),
            visible_range,
            last_is_closed: last_candle_is_closed(candles, now_ms),
        }
    }
}

/// Whether two series are the same bars, field for field and bit for bit: what equal
/// [`candles_fingerprint`]s say, without hashing. Newest first, where a live series changes.
fn same_bars(a: &[Kline], b: &[Kline]) -> bool {
    a.len() == b.len()
        && a.iter().rev().zip(b.iter().rev()).all(|(a, b)| {
            a.open_time == b.open_time
                && a.close_time == b.close_time
                && a.open.to_bits() == b.open.to_bits()
                && a.high.to_bits() == b.high.to_bits()
                && a.low.to_bits() == b.low.to_bits()
                && a.close.to_bits() == b.close.to_bits()
                && a.volume.to_bits() == b.volume.to_bits()
        })
}

/// How long a run has to take before the chart says so. Live ticks run a script again on every
/// update, and a quick one must not flash an indicator each time.
const PINE_BUSY_AFTER: Duration = Duration::from_millis(400);

/// Whether a run requested at `since` is slow enough, at `now`, to show as busy.
fn is_slow(since: Option<Instant>, now: Instant) -> bool {
    since.is_some_and(|since| now.saturating_duration_since(since) >= PINE_BUSY_AFTER)
}

/// What the chart drawing this frame hands its scripts.
pub struct ChartFrame<'a> {
    /// The bars the chart is: the market's candles, or the bars a price-based type built.
    pub bars: &'a [Kline],
    pub symbol: &'a SymbolInfo,
    /// The length of the chart's bars, when their times cannot say (price-based charts).
    pub bar_seconds: Option<f64>,
    /// The selected timeframe, even when its nominal length matches another timeframe's.
    pub timeframe: Option<Timeframe>,
    /// The colours the chart is drawn on: what `chart.bg_color` and `chart.fg_color` read.
    pub theme: ChartTheme,
    /// The open times of the first and last bar on screen, for a script that reads them.
    pub visible_range: Option<(i64, i64)>,
}

/// One run's `log.*()` messages. The epoch tells a restarted run from the one before it, which
/// the instance id cannot.
pub struct PineLogs<'a> {
    pub epoch: u64,
    pub history: &'a LogHistory,
}

/// How one instance is doing, for the chart settings' Scripts tab.
pub struct InstanceStatus<'a> {
    pub output: &'a VisualOutput,
    pub error: Option<&'a Diagnostic>,
}

/// One instance's last run, cached so the chart does not recompile every frame.
struct Run {
    source: String,
    inputs: HashMap<String, StoredInput>,
    /// The chart colours the script ran against; a change of app theme runs it again.
    theme: ChartTheme,
    worker: PineWorker,
    requested: Option<RunRevision>,
    shown_revision: Option<RunRevision>,
    /// Returning to a series only shows cached output if it matches the bars now on the chart.
    awaiting_series: bool,
    generation: u64,
    /// Which run of this chart this is; a restart (new source, inputs, symbol) is a new epoch.
    epoch: u64,
    /// The `log.*()` messages the run wrote. They belong to the run, not to an output generation:
    /// a result whose output went stale still carries messages nothing else will deliver.
    logs: LogHistory,
    /// Whether `output.overlay` came from a compiled script (or from a previous valid run). Until
    /// then the default output must not claim a pane: `VisualOutput::default()` is non-overlay.
    placement_known: bool,
    output: Arc<VisualOutput>,
    /// The runtime failure that stopped this run early.
    error: Option<Diagnostic>,
    /// The last run that finished without a runtime failure: what stays on screen while a newer
    /// source fails to run.
    last_valid: Option<Arc<VisualOutput>>,
    /// `output` with the instance's style choices applied, and the choices it was made with
    /// (`None` until the first styling).
    styled: Arc<VisualOutput>,
    styled_with: Option<HashMap<String, String>>,
    /// When the run in flight was first requested; `None` while its latest result is in.
    busy_since: Option<Instant>,
}

impl Run {
    fn is_drawable(&self) -> bool {
        self.placement_known && !self.awaiting_series
    }
}

#[derive(PartialEq)]
struct ChartSeries {
    symbol: SymbolInfo,
    timeframe: Option<Timeframe>,
    bar_seconds: Option<u64>,
}

/// The most recent other timeframe on this chart. A quick switch back can resume its worker
/// through the same incremental update it would have used had the chart stayed there.
struct ParkedSeries {
    series: ChartSeries,
    runs: HashMap<u64, Run>,
    parked_at: Instant,
}

const PARKED_FOR: Duration = Duration::from_secs(30);
const MAX_PARKED_BARS: usize =
    crate::model::market::history::SCRIPT_DEPTH + crate::model::market::history::PAGE;

/// The bars the runs were last handed, shared by every request made from them, with their
/// fingerprint: worked out again only when the chart's bars differ from these.
struct HandedBars {
    bars: Arc<Vec<Kline>>,
    fingerprint: u64,
}

/// The runs of the scripts on one chart, by instance id.
#[derive(Default)]
pub struct ChartRuns {
    runs: HashMap<u64, Run>,
    series: Option<ChartSeries>,
    parked: Option<ParkedSeries>,
    /// A compiled non-overlay declaration can still reserve its pane while its data loads.
    pane_ids: HashSet<u64>,
    /// Counts the runs this chart started, so each gets an epoch of its own: an instance id is
    /// reused once its script is removed, a run is never.
    epochs: u64,
    /// The instances whose realtime messages are not kept (the pane's Disable logging). The script
    /// keeps running; only what it writes from now on is left out.
    logging_paused: HashSet<u64>,
    handed: Option<HandedBars>,
}

impl ChartRuns {
    /// Stops every run; the next frame starts the visible instances afresh. Logging choices stay.
    pub fn stop_all(&mut self) {
        self.runs.clear();
        self.parked = None;
        self.pane_ids.clear();
        self.handed = None;
    }

    /// Stops every run and forgets every instance's logging choice (the chart's scripts were
    /// replaced).
    pub fn reset(&mut self) {
        self.stop_all();
        self.logging_paused.clear();
    }

    /// Stops instance `id`'s run and forgets its logging choice (the script was removed).
    pub fn remove(&mut self, id: u64) {
        self.runs.remove(&id);
        if let Some(parked) = &mut self.parked {
            parked.runs.remove(&id);
        }
        self.pane_ids.remove(&id);
        self.logging_paused.remove(&id);
    }

    /// Hides data from the series being left immediately, including for read-only consumers
    /// between the switch and the next chart frame.
    pub fn suspend_series(&mut self) {
        for run in self.runs.values_mut() {
            run.awaiting_series = true;
        }
    }

    pub fn pane_pending(&self, id: u64) -> bool {
        self.pane_ids.contains(&id)
            && self.runs.get(&id).is_none_or(|run| {
                run.awaiting_series || !run.placement_known && run.error.is_none()
            })
    }

    /// What the user can restyle in an instance's output, as the script itself draws it. Empty
    /// until the script has run.
    pub fn style_rows(&self, id: u64) -> Vec<StyleRow> {
        self.runs
            .get(&id)
            .filter(|run| run.is_drawable())
            .map(|run| style_rows(&run.output))
            .unwrap_or_default()
    }

    /// The messages an instance's script wrote with `log.*()` in its current run.
    pub fn logs(&self, id: u64) -> Option<PineLogs<'_>> {
        self.runs
            .get(&id)
            .filter(|run| !run.awaiting_series)
            .map(|run| PineLogs {
                epoch: run.epoch,
                history: &run.logs,
            })
    }

    /// Forgets the messages kept for an instance's run. The run goes on and writes more.
    pub fn clear_logs(&mut self, id: u64) {
        if let Some(run) = self.runs.get_mut(&id) {
            run.logs.clear();
        }
    }

    /// Whether an instance keeps the messages it writes in realtime.
    pub fn logging(&self, id: u64) -> bool {
        !self.logging_paused.contains(&id)
    }

    /// Turns the keeping of an instance's realtime messages on or off. Messages already kept stay.
    pub fn set_logging(&mut self, id: u64, enabled: bool) {
        if enabled {
            self.logging_paused.remove(&id);
        } else {
            self.logging_paused.insert(id);
        }
    }

    /// The instance's output and failure, once its run has either.
    pub fn status(&self, id: u64) -> Option<InstanceStatus<'_>> {
        self.runs
            .get(&id)
            .filter(|run| !run.awaiting_series && (run.placement_known || run.error.is_some()))
            .map(|run| InstanceStatus {
                output: &run.output,
                error: run.error.as_ref(),
            })
    }

    /// Whether the instance's latest run has been going on long enough to show a busy state. A
    /// hidden instance has no run, so it is never busy.
    pub fn busy(&self, id: u64, now: Instant) -> bool {
        self.runs
            .get(&id)
            .is_some_and(|run| is_slow(run.busy_since, now))
    }

    /// The runtime failure that stopped the instance's run early.
    pub fn error(&self, id: u64) -> Option<&Diagnostic> {
        self.runs
            .get(&id)
            .filter(|run| !run.awaiting_series)?
            .error
            .as_ref()
    }

    /// Whether a run of the instance has finished cleanly, so a later failure leaves real output
    /// up.
    pub fn has_valid_output(&self, id: u64) -> bool {
        self.runs
            .get(&id)
            .is_some_and(|run| !run.awaiting_series && run.last_valid.is_some())
    }

    /// The instance's output as the script made it (strategy report, alerts), once its placement
    /// is known.
    pub fn output(&self, id: u64) -> Option<&VisualOutput> {
        self.runs
            .get(&id)
            .filter(|run| run.is_drawable())
            .map(|run| &*run.output)
    }

    /// The visible instances' last styled outputs, in order. Unlike [`Self::update`] it starts and
    /// requests nothing.
    pub fn last_outputs(
        &self,
        instances: &[PineInstance],
    ) -> Vec<(Arc<VisualOutput>, VisualOrder, u64)> {
        instances
            .iter()
            .filter(|instance| instance.visible)
            .filter_map(|instance| {
                let run = self
                    .runs
                    .get(&instance.id)
                    .filter(|run| run.is_drawable())?;
                Some((Arc::clone(&run.styled), instance.visual_order, instance.id))
            })
            .collect()
    }

    /// The visible instances' styled outputs in order, after taking in what their workers
    /// finished and asking for a run of each whose script, inputs, market, look or bars changed.
    pub fn update(
        &mut self,
        repaint: &egui::Context,
        chart: &ChartFrame<'_>,
        instances: &[PineInstance],
    ) -> Vec<(Arc<VisualOutput>, VisualOrder, u64)> {
        if instances.iter().all(|instance| !instance.visible) {
            self.stop_all();
            return Vec::new();
        }
        self.select_series(chart);
        if chart.bars.is_empty() {
            return Vec::new();
        }
        let now_ms = crate::clock::now_ms();
        let bars = self.handed_bars(chart.bars);
        let revision = RunRevision {
            candles: bars.fingerprint,
            visible_range: chart.visible_range,
            last_is_closed: last_candle_is_closed(chart.bars, now_ms),
        };
        let bars = Arc::clone(&bars.bars);
        let visible_ids: HashSet<u64> = instances
            .iter()
            .filter(|instance| instance.visible)
            .map(|instance| instance.id)
            .collect();
        self.runs.retain(|id, _| visible_ids.contains(id));
        if let Some(parked) = &mut self.parked {
            parked.runs.retain(|id, _| visible_ids.contains(id));
        }
        let mut outputs = Vec::new();
        for instance in instances.iter().filter(|i| i.visible) {
            let configured = self.runs.get(&instance.id).is_some_and(|run| {
                run.source == instance.source
                    && run.inputs == instance.inputs
                    && run.theme == chart.theme
            });
            if !configured {
                self.start(repaint, chart, instance);
            }
            let Some(run) = self.runs.get_mut(&instance.id) else {
                continue;
            };
            // Only a script that reads the visible range depends on where the chart is scrolled.
            let revision = if reads_visible_range(&instance.source) {
                revision
            } else {
                RunRevision {
                    visible_range: None,
                    ..revision
                }
            };
            while let Ok(result) = run.worker.results.try_recv() {
                let mut batch = result.logs;
                if self.logging_paused.contains(&instance.id) {
                    batch.events.retain(|event| !event.realtime);
                }
                run.logs.apply(batch);
                if result.generation != run.generation
                    || run.awaiting_series && run.requested != Some(revision)
                {
                    continue;
                }
                run.busy_since = None;
                if run.awaiting_series {
                    run.last_valid = None;
                }
                run.awaiting_series = false;
                run.shown_revision = run.requested.filter(|_| result.error.is_none());
                let output = Arc::new(result.output);
                let shown = match (&result.error, run.last_valid.clone()) {
                    (None, _) => {
                        run.last_valid = Some(Arc::clone(&output));
                        Arc::clone(&output)
                    }
                    (Some(_), Some(valid)) => valid,
                    (Some(_), None) => Arc::clone(&output),
                };
                run.output = shown;
                run.error = result.error;
                run.placement_known = result.placement_known || run.last_valid.is_some();
                run.styled_with = None;
                if run.placement_known && !run.output.overlay {
                    self.pane_ids.insert(instance.id);
                } else {
                    self.pane_ids.remove(&instance.id);
                }
            }
            if run.awaiting_series && run.shown_revision == Some(revision) {
                run.awaiting_series = false;
            }
            if run.requested != Some(revision) {
                run.generation = run.generation.wrapping_add(1);
                run.requested = Some(revision);
                run.worker.request(WorkerRequest {
                    generation: run.generation,
                    bars: Arc::clone(&bars),
                    visible_range: revision.visible_range,
                    now_ms,
                });
                run.busy_since.get_or_insert_with(Instant::now);
            }
            if let Some(since) = run.busy_since {
                // The chip shows the indicator from the first frame past the threshold.
                if let Some(left) = PINE_BUSY_AFTER.checked_sub(since.elapsed()) {
                    repaint.request_repaint_after(left);
                }
            }
            if !run.is_drawable() {
                continue;
            }
            if run.styled_with.as_ref() != Some(&instance.style_overrides) {
                run.styled = Arc::new(
                    StyleOverrides::new(instance.style_overrides.clone()).apply(&run.output),
                );
                run.styled_with = Some(instance.style_overrides.clone());
            }
            outputs.push((Arc::clone(&run.styled), instance.visual_order, instance.id));
        }
        outputs
    }

    fn select_series(&mut self, chart: &ChartFrame<'_>) {
        if self
            .parked
            .as_ref()
            .is_some_and(|parked| parked.parked_at.elapsed() >= PARKED_FOR)
        {
            self.parked = None;
        }
        let bar_seconds = chart.bar_seconds.map(f64::to_bits);
        if self.series.as_ref().is_some_and(|series| {
            series.symbol == *chart.symbol
                && series.timeframe == chart.timeframe
                && series.bar_seconds == bar_seconds
        }) {
            return;
        }
        let next = ChartSeries {
            symbol: chart.symbol.clone(),
            timeframe: chart.timeframe,
            bar_seconds,
        };
        self.suspend_series();
        let previous = self.series.take();
        let runs = std::mem::take(&mut self.runs);
        if let Some(parked) = self.parked.take().filter(|parked| parked.series == next) {
            self.runs = parked.runs;
        }
        if let Some(series) = previous
            && series.symbol == next.symbol
            && series.timeframe != next.timeframe
            && self
                .handed
                .as_ref()
                .is_some_and(|handed| handed.bars.len() <= MAX_PARKED_BARS)
            && !runs.is_empty()
        {
            self.parked = Some(ParkedSeries {
                series,
                runs,
                parked_at: Instant::now(),
            });
        }
        self.handed = None;
        self.series = Some(next);
    }

    /// Starts a new run of `instance`, in a new epoch, keeping the last valid output of the run it
    /// replaces on screen until the new one has its own.
    fn start(&mut self, repaint: &egui::Context, chart: &ChartFrame<'_>, instance: &PineInstance) {
        self.epochs += 1;
        let previous_valid = self
            .runs
            .remove(&instance.id)
            .filter(|run| !run.awaiting_series)
            .and_then(|run| run.last_valid);
        let placement_known = previous_valid.is_some();
        let shown = previous_valid
            .clone()
            .unwrap_or_else(|| Arc::new(VisualOutput::default()));
        let worker = PineWorker::new(
            WorkerConfig {
                theme: chart.theme,
                source: instance.source.clone(),
                inputs: instance.input_values(),
                symbol: chart.symbol.clone(),
                bar_seconds: chart.bar_seconds,
                timeframe_seconds: chart.timeframe.map(seconds_of),
            },
            repaint.clone(),
        );
        self.runs.insert(
            instance.id,
            Run {
                source: instance.source.clone(),
                inputs: instance.inputs.clone(),
                theme: chart.theme,
                worker,
                requested: None,
                shown_revision: None,
                awaiting_series: false,
                generation: 0,
                epoch: self.epochs,
                logs: LogHistory::default(),
                placement_known,
                styled: Arc::clone(&shown),
                styled_with: None,
                output: shown,
                error: None,
                last_valid: previous_valid,
                busy_since: None,
            },
        );
    }

    /// `bars` as the runs are handed them: the copy made last time while they are the same bars,
    /// else a new copy and fingerprint. Comparing is a cheaper pass over the series than hashing.
    fn handed_bars(&mut self, bars: &[Kline]) -> &HandedBars {
        if !self
            .handed
            .as_ref()
            .is_some_and(|handed| same_bars(&handed.bars, bars))
        {
            self.handed = Some(HandedBars {
                bars: Arc::new(bars.to_vec()),
                fingerprint: candles_fingerprint(bars),
            });
        }
        self.handed.as_ref().expect("bars were just handed")
    }

    /// Which request instance `id`'s run is on.
    #[cfg(test)]
    pub(crate) fn generation_for_tests(&self, id: u64) -> Option<u64> {
        self.runs.get(&id).map(|run| run.generation)
    }

    /// What instance `id`'s last run drew, with its style applied.
    #[cfg(test)]
    pub(crate) fn styled_for_tests(&self, id: u64) -> Option<&VisualOutput> {
        self.runs.get(&id).map(|run| &*run.styled)
    }
}
