//! One shared history of native USD-M seconds per contract, used by every custom timeframe.

use super::BINANCE_USDM;
use crate::lock::lock;
use crate::model::market::bucket::{self, FoldPlan};
use crate::model::{DataSource, Kline, Timeframe};
use crate::service::ProviderError;
use crate::service::cache::disk::{self, DiskCache, Kind};
use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{LazyLock, Mutex};
use std::time::{Duration, Instant};

const SECOND: i64 = 1_000;
// Leave the newest REST rows eligible for repair while the exchange publishes their final state.
const REST_SETTLE: i64 = 2 * SECOND;
// A full page stays below Binance's highest per-request weight.
pub(super) const PAGE: i64 = 999;
const RETAIN: i64 = 100_000 * SECOND;
const SAVE_EVERY: Duration = Duration::from_secs(60);
type SavedBar = (i64, [f64; 5]);

static BTC: LazyLock<History> = LazyLock::new(|| History::new("binance-usdm-btc-1s.json"));
static ETH: LazyLock<History> = LazyLock::new(|| History::new("binance-usdm-eth-1s.json"));

fn contract(symbol: &str) -> Result<(&'static str, &'static History), ProviderError> {
    let pair = BINANCE_USDM.perpetual_pair(symbol)?;
    let history = if pair == "BTCUSDT" { &*BTC } else { &*ETH };
    Ok((pair, history))
}

pub(super) fn clear() {
    for history in [&*BTC, &*ETH] {
        *lock(&history.book) = Book::default();
    }
}

pub(super) fn record_live(symbol: &str, bar: &Kline, closed: bool) {
    if let Ok((_, history)) = contract(symbol) {
        let disk = disk::global();
        let save_due = {
            let mut book = lock(&history.book);
            let book = book.for_generation(disk.generation());
            book.insert(bar.clone(), closed);
            closed && disk.is_enabled() && book.save_due()
        };
        if save_due && !history.save_queued.swap(true, Ordering::AcqRel) {
            crate::service::work_queue::submit_for(DataSource::BinanceUsdM, move || {
                let _request = lock(&history.requests);
                let now = crate::clock::now_ms();
                let generation = disk.generation();
                history.load_saved(disk, generation, now);
                history.save(disk, generation, now);
                history.save_queued.store(false, Ordering::Release);
            });
        }
    }
}

/// History plus the native bars used to make the newest open bucket. A rate-limited chart can
/// draw the data on hand while the shared HTTP cooldown blocks backfill.
pub(super) fn latest(
    symbol: &str,
    plan: FoldPlan,
    limit: usize,
    allow_cached: bool,
) -> Result<(Vec<Kline>, Vec<Kline>), ProviderError> {
    load(symbol, plan, 0, i64::MAX, limit, allow_cached)
}

pub(super) fn range(
    symbol: &str,
    plan: FoldPlan,
    start_ms: i64,
    end_ms: i64,
    limit: usize,
) -> Result<Vec<Kline>, ProviderError> {
    load(symbol, plan, start_ms, end_ms, limit, false).map(|(candles, _)| candles)
}

fn load(
    symbol: &str,
    plan: FoldPlan,
    start_ms: i64,
    end_ms: i64,
    limit: usize,
    allow_cached: bool,
) -> Result<(Vec<Kline>, Vec<Kline>), ProviderError> {
    let (pair, history) = contract(symbol)?;
    let now = crate::clock::now_ms();
    let end_ms = end_ms.min(now.saturating_add(1));
    let window = align_window(start_ms, end_ms, plan.base_count(limit));
    let raw = history.load(
        disk::global(),
        window.clone(),
        now,
        allow_cached,
        |missing| {
            let count = ((missing.end - missing.start) / SECOND) as usize;
            let url = BINANCE_USDM.candle_url(pair, Timeframe::SECOND_1, count)?;
            BINANCE_USDM.download(&format!(
                "{url}&startTime={}&endTime={}",
                missing.start,
                (missing.end - 1).min(end_ms - 1)
            ))
        },
    )?;
    Ok(fold(plan, raw, limit, now))
}

fn ceil_second(time: i64) -> i64 {
    (time.saturating_add(SECOND - 1) / SECOND) * SECOND
}

fn align_window(start: i64, end: i64, count: usize) -> Range<i64> {
    let end = end.max(0);
    let start = start.max(end.saturating_sub((count as i64).saturating_mul(SECOND)));
    ceil_second(start.max(0))..ceil_second(end)
}

fn fold(plan: FoldPlan, raw: Vec<Kline>, limit: usize, now: i64) -> (Vec<Kline>, Vec<Kline>) {
    if plan.is_native() {
        return (raw, Vec::new());
    }
    let timeframe = plan.target();
    let mut candles = Vec::new();
    let mut forming = Vec::new();
    for bars in raw.chunk_by(|a, b| {
        bucket::bucket_start_ms(timeframe, a.open_time)
            == bucket::bucket_start_ms(timeframe, b.open_time)
    }) {
        let start = bucket::bucket_start_ms(timeframe, bars[0].open_time);
        let end = bucket::next_bucket_start_ms(timeframe, start);
        let covered_through = bars.last().expect("nonempty bucket").close_time + 1;
        if bars[0].open_time != start
            || (end <= now && covered_through < end)
            || covered_through - start != bars.len() as i64 * SECOND
        {
            continue;
        }
        let mut candle = Kline {
            close_time: end - 1,
            ..bars[0].clone()
        };
        for bar in &bars[1..] {
            candle.absorb(bar);
        }
        candles.push(candle);
        if end > now {
            forming.extend_from_slice(bars);
        }
    }
    let skip = candles.len().saturating_sub(limit);
    candles.drain(..skip);
    (candles, forming)
}

#[derive(Default)]
struct Book {
    generation: u64,
    closed: BTreeMap<i64, Kline>,
    forming: Option<Kline>,
    loaded: bool,
    dirty: bool,
    saved: Option<Instant>,
}

impl Book {
    fn for_generation(&mut self, generation: u64) -> &mut Self {
        if self.generation != generation {
            *self = Self {
                generation,
                ..Self::default()
            };
        }
        self
    }

    fn insert(&mut self, bar: Kline, closed: bool) {
        if !valid(&bar) {
            return;
        }
        if closed {
            if self
                .closed
                .get(&bar.open_time)
                .is_none_or(|held| held != &bar && held.volume <= bar.volume)
            {
                self.closed.insert(bar.open_time, bar);
                self.dirty = true;
            }
        } else if self.forming.as_ref().is_none_or(|last| {
            last.open_time < bar.open_time
                || last.open_time == bar.open_time && last.volume <= bar.volume
        }) {
            self.forming = Some(bar);
        }
    }

    fn bars(&self, window: Range<i64>, now: i64) -> BTreeMap<i64, Kline> {
        let mut bars: BTreeMap<_, _> = self
            .closed
            .range(window.clone())
            .map(|(&time, bar)| (time, bar.clone()))
            .collect();
        if let Some(forming) = self.forming.as_ref()
            && window.contains(&forming.open_time)
            && forming.close_time >= now
        {
            bars.entry(forming.open_time)
                .or_insert_with(|| forming.clone());
        }
        bars
    }

    fn trim(&mut self, now: i64) {
        while self
            .closed
            .first_key_value()
            .is_some_and(|(&time, _)| time < now - RETAIN)
        {
            self.closed.pop_first();
        }
    }

    fn save_due(&self) -> bool {
        self.dirty && self.saved.is_none_or(|at| at.elapsed() >= SAVE_EVERY)
    }
}

fn valid(bar: &Kline) -> bool {
    bar.open_time > 0
        && bar.open_time % SECOND == 0
        && bar.close_time.checked_sub(bar.open_time) == Some(SECOND - 1)
        && [bar.open, bar.high, bar.low, bar.close, bar.volume]
            .into_iter()
            .all(f64::is_finite)
}

struct History {
    name: &'static str,
    book: Mutex<Book>,
    /// Two charts or a chart and a Pine request share a miss instead of downloading it twice.
    requests: Mutex<()>,
    save_queued: AtomicBool,
}

impl History {
    fn new(name: &'static str) -> Self {
        Self {
            name,
            book: Mutex::new(Book::default()),
            requests: Mutex::new(()),
            save_queued: AtomicBool::new(false),
        }
    }

    fn load(
        &self,
        disk: &DiskCache,
        window: Range<i64>,
        now: i64,
        allow_cached: bool,
        download: impl Fn(Range<i64>) -> Result<Vec<Kline>, ProviderError>,
    ) -> Result<Vec<Kline>, ProviderError> {
        if window.is_empty() {
            return Ok(Vec::new());
        }
        let _request = lock(&self.requests);
        let generation = disk.generation();
        self.load_saved(disk, generation, now);
        let mut result = lock(&self.book)
            .for_generation(disk.generation())
            .bars(window.clone(), now);
        let mut failure = None;
        for page in missing_pages(&result, window.clone()) {
            // Judge the finality of a REST row at the start of the request. The HTTP response
            // may arrive after the candle closed but still hold its earlier, forming state.
            let requested_at = crate::clock::now_ms();
            match download(page.clone()) {
                Ok(rows) => {
                    let mut book = lock(&self.book);
                    for row in rows {
                        if valid(&row) && page.contains(&row.open_time) {
                            if book.generation == generation && disk.generation() == generation {
                                book.insert(
                                    row.clone(),
                                    row.close_time < requested_at - REST_SETTLE,
                                );
                            }
                            result.entry(row.open_time).or_insert(row);
                        }
                    }
                }
                Err(error) => {
                    failure = Some(error);
                    break;
                }
            }
        }
        self.save(disk, generation, now);
        if let Some(error) = failure
            && (!allow_cached || !error.is_rate_limited() || result.is_empty())
        {
            return Err(error);
        }
        Ok(result.into_values().collect())
    }

    fn load_saved(&self, disk: &DiskCache, generation: u64, now: i64) {
        {
            let mut book = lock(&self.book);
            if disk.generation() != generation || book.for_generation(generation).loaded {
                return;
            }
        }
        let saved = disk
            .read(Kind::Lookups, self.name)
            .and_then(|entry| serde_json::from_slice::<Vec<SavedBar>>(&entry.bytes).ok())
            .unwrap_or_default();
        let mut data = lock(&self.book);
        if data.generation != generation || disk.generation() != generation {
            return;
        }
        for (open_time, [open, high, low, close, volume]) in saved {
            if (now - RETAIN..now).contains(&open_time) {
                let bar = Kline {
                    open_time,
                    close_time: open_time + SECOND - 1,
                    open,
                    high,
                    low,
                    close,
                    volume,
                };
                if valid(&bar) {
                    data.closed.entry(open_time).or_insert(bar);
                }
            }
        }
        data.loaded = true;
    }

    fn save(&self, disk: &DiskCache, generation: u64, now: i64) {
        let saved: Vec<SavedBar> = {
            let mut data = lock(&self.book);
            if data.generation != generation || disk.generation() != generation {
                return;
            }
            data.trim(now);
            if !disk.is_enabled() || !data.save_due() {
                return;
            }
            data.dirty = false;
            data.saved = Some(Instant::now());
            data.closed
                .values()
                .map(|bar| {
                    (
                        bar.open_time,
                        [bar.open, bar.high, bar.low, bar.close, bar.volume],
                    )
                })
                .collect()
        };
        if let Ok(bytes) = serde_json::to_vec(&saved) {
            disk.write(Kind::Lookups, self.name, &bytes, generation);
        }
    }
}

/// Skip covered pages and trim each missing page to its first and last absent second. Isolated
/// misses within one page are batched into a single request.
fn missing_pages(bars: &BTreeMap<i64, Kline>, window: Range<i64>) -> Vec<Range<i64>> {
    let mut pages = Vec::new();
    let mut end = window.end;
    while end > window.start {
        let start = (end - PAGE * SECOND).max(window.start);
        let mut missing = (start..end)
            .step_by(SECOND as usize)
            .filter(|time| !bars.contains_key(time));
        if let Some(first) = missing.next() {
            let last = missing.last().unwrap_or(first);
            pages.push(first..last + SECOND);
        }
        end = start;
    }
    pages
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::service::cache::disk::testing::TempCache;
    use crate::service::net::http;
    use std::cell::RefCell;

    fn bars(window: Range<i64>) -> Vec<Kline> {
        window
            .step_by(SECOND as usize)
            .map(|open_time| Kline {
                open_time,
                close_time: open_time + SECOND - 1,
                open: 10.0,
                high: 14.0,
                low: 8.0,
                close: 12.0,
                volume: 1.0,
            })
            .collect()
    }

    fn plan(timeframe: &str) -> FoldPlan {
        DataSource::BinanceUsdM
            .plan(Timeframe::parse(timeframe).unwrap())
            .unwrap()
    }

    fn limited() -> ProviderError {
        http::Error::UsdMCooldown {
            resume_at: Instant::now() + Duration::from_secs(120),
        }
        .into()
    }

    #[test]
    fn switching_timeframes_reuses_closed_seconds_from_disk_and_only_requests_the_gap() {
        let temp = TempCache::new();
        let name = "binance-usdm-test.json";
        let history = History::new(name);
        let first = history
            .load(&temp.cache, 15_000..60_000, 60_000, false, |window| {
                Ok(bars(window))
            })
            .unwrap();
        assert_eq!(fold(plan("15s"), first, 3, 60_000).0.len(), 3);

        let restarted = History::new(name);
        let requests = RefCell::new(Vec::new());
        let second = restarted
            .load(&temp.cache, 35_000..65_000, 65_000, false, |window| {
                requests.borrow_mut().push(window.clone());
                Ok(bars(window))
            })
            .unwrap();
        let requests = requests.into_inner();
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0], 60_000..65_000);
        let (candles, _) = fold(plan("5s"), second, 6, 65_000);
        assert_eq!(candles.len(), 6);
        assert!(candles.iter().all(|bar| bar.volume == 5.0));
    }

    #[test]
    fn a_rate_limited_chart_can_fold_saved_data_and_a_live_bar_but_not_a_hole() {
        let disk = DiskCache::disabled();
        let history = History::new("binance-usdm-test.json");
        {
            let mut book = lock(&history.book);
            for bar in bars(10_000..17_000).into_iter().chain(bars(18_000..21_000)) {
                book.insert(bar, true);
            }
            book.insert(bars(21_000..22_000).remove(0), false);
        }
        let raw = history
            .load(&disk, 10_000..22_000, 21_200, true, |window| {
                assert_eq!(window, 17_000..18_000);
                Err(limited())
            })
            .unwrap();
        let (candles, forming) = fold(plan("5s"), raw, 3, 21_200);
        let opens: Vec<_> = candles.iter().map(|bar| bar.open_time).collect();
        assert_eq!(opens, [10_000, 20_000]);
        assert_eq!(forming, bars(20_000..22_000));
        assert!(
            history
                .load(&disk, 10_000..22_000, 21_200, false, |_| Err(limited()))
                .is_err()
        );
    }

    #[test]
    fn a_forming_bar_cannot_stand_in_for_a_missing_closed_second() {
        let mut book = Book::default();
        let bar = bars(10_000..11_000).remove(0);
        book.insert(bar.clone(), false);
        assert!(book.bars(10_000..11_000, 11_200).is_empty());
        book.insert(bar.clone(), true);
        assert_eq!(
            book.bars(10_000..11_000, 11_200)
                .into_values()
                .collect::<Vec<_>>(),
            [bar]
        );
    }
}
