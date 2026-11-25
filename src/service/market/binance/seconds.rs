//! Closed USD-M seconds candles from the recent trade endpoint and daily Binance Data ZIPs.

use super::USD_M_REST;
use crate::format::civil_from_days;
use crate::lock::lock;
use crate::model::market::bucket::{bucket_start_ms, fold, next_bucket_start_ms};
use crate::model::market::usdm_contract;
use crate::model::{Kline, Timeframe};
use crate::service::ProviderError;
use crate::service::cache::disk::{self, Kind};
use crate::service::net::http::{self, Retry};
use serde::Deserialize;
use std::io::{BufRead, BufReader, Cursor};
use std::sync::Mutex;
use std::time::{Duration, Instant};

const DAY_MS: i64 = 86_400_000;
const RECENT_MS: i64 = 15 * 60 * 1000;
const ARCHIVE_DAYS: i64 = 6;
const PAGE: u64 = 1_000;
const MAX_ZIP_BYTES: u64 = 128 * 1024 * 1024;
const MAX_CSV_BYTES: u64 = 2 * 1024 * 1024 * 1024;
const ARCHIVE_ROOT: &str = "https://data.binance.vision/data/futures/um/daily/aggTrades";
static NEXT_REST: Mutex<Option<Instant>> = Mutex::new(None);

#[derive(Deserialize)]
struct Trade {
    #[serde(rename = "a")]
    id: u64,
    #[serde(rename = "T")]
    time: i64,
    #[serde(rename = "p")]
    price: String,
    #[serde(rename = "q")]
    quantity: String,
}

struct Bar {
    candle: Kline,
    first: (i64, u64),
    last: (i64, u64),
}

struct Seconds {
    first_second: i64,
    bars: Vec<Option<Bar>>,
}

impl Seconds {
    fn new(start: i64, end: i64) -> Self {
        let first_second = start.div_euclid(1000);
        let count = (end.div_euclid(1000) - first_second).max(0) as usize;
        Self {
            first_second,
            bars: std::iter::repeat_with(|| None).take(count).collect(),
        }
    }

    fn add(
        &mut self,
        time: i64,
        id: u64,
        price: &str,
        quantity: &str,
    ) -> Result<(), ProviderError> {
        let number = |s: &str| s.parse::<f64>().ok().filter(|n| n.is_finite() && *n > 0.0);
        let (Some(price), Some(quantity)) = (number(price), number(quantity)) else {
            return Err(ProviderError::data(
                "invalid Binance trade price or quantity",
            ));
        };
        let second = time.div_euclid(1000) * 1000;
        let order = (time, id);
        let index = (second.div_euclid(1000) - self.first_second) as usize;
        let slot = self
            .bars
            .get_mut(index)
            .ok_or_else(|| ProviderError::data("trade outside the requested seconds"))?;
        let bar = slot.get_or_insert_with(|| Bar {
            candle: Kline {
                open_time: second,
                close_time: second + 999,
                open: price,
                high: price,
                low: price,
                close: price,
                volume: 0.0,
            },
            first: order,
            last: order,
        });
        if order < bar.first {
            bar.candle.open = price;
            bar.first = order;
        }
        if order > bar.last {
            bar.candle.close = price;
            bar.last = order;
        }
        bar.candle.high = bar.candle.high.max(price);
        bar.candle.low = bar.candle.low.min(price);
        bar.candle.volume += quantity;
        Ok(())
    }

    fn finish(self) -> Vec<Kline> {
        self.bars
            .into_iter()
            .flatten()
            .map(|bar| bar.candle)
            .collect()
    }
}

fn market(symbol: &str) -> Result<&'static str, ProviderError> {
    usdm_contract(symbol).ok_or_else(|| ProviderError::data("unknown USD-M seconds market"))
}

pub(super) fn newest(
    symbol: &str,
    timeframe: Timeframe,
    limit: usize,
) -> Result<Vec<Kline>, ProviderError> {
    recent(
        market(symbol)?,
        timeframe,
        0,
        crate::clock::now_ms(),
        limit,
        if limit <= 100 { 1 } else { 4 },
    )
}

pub(super) fn older(
    symbol: &str,
    timeframe: Timeframe,
    start: i64,
    end: i64,
    limit: usize,
) -> Result<Vec<Kline>, ProviderError> {
    let symbol = market(symbol)?;
    let now = crate::clock::now_ms();
    if end > now - RECENT_MS {
        let bars = recent(symbol, timeframe, start, end, limit, 8)?;
        if !bars.is_empty() {
            return Ok(bars);
        }
    }

    // Start at the last completed UTC day and skip dates Binance has yet to publish.
    let today = now.div_euclid(DAY_MS);
    let first_day = (end - 1).div_euclid(DAY_MS).min(today - 1);
    for day in ((today - ARCHIVE_DAYS)..=first_day).rev() {
        if (day + 1) * DAY_MS <= start {
            break;
        }
        if let Some(bars) = day_bars(symbol, day)? {
            let page = window(&bars, timeframe, start, end, limit);
            if !page.is_empty() {
                return Ok(page);
            }
        }
    }
    Ok(Vec::new())
}

fn rest(symbol: &str, query: &str) -> Result<Vec<Trade>, ProviderError> {
    // A request costs 20 weight. Keep this pool comfortably below 2,400 weight/minute.
    let delay = {
        let mut next = lock(&NEXT_REST);
        let now = Instant::now();
        let at = next.unwrap_or(now).max(now);
        *next = Some(at + Duration::from_millis(650));
        at.saturating_duration_since(now)
    };
    std::thread::sleep(delay);
    let url = format!("{USD_M_REST}/aggTrades?symbol={symbol}&limit={PAGE}&{query}");
    let text = http::get_text(&url, &[], Retry::Never)?;
    serde_json::from_str(&text).map_err(ProviderError::data)
}

fn recent(
    symbol: &str,
    timeframe: Timeframe,
    start: i64,
    end: i64,
    limit: usize,
    pages: usize,
) -> Result<Vec<Kline>, ProviderError> {
    recent_with(timeframe, start, end, limit, pages, |query| {
        rest(symbol, query)
    })
}

fn recent_with(
    timeframe: Timeframe,
    start: i64,
    end: i64,
    limit: usize,
    pages: usize,
    mut fetch: impl FnMut(&str) -> Result<Vec<Trade>, ProviderError>,
) -> Result<Vec<Kline>, ProviderError> {
    let mut trades = fetch(&format!("endTime={}", end - 1))?;
    if trades.is_empty() {
        return Ok(Vec::new());
    }
    validate_page(&trades)?;
    if trades
        .iter()
        .any(|trade| !(end.saturating_sub(2 * DAY_MS)..end).contains(&trade.time))
    {
        return Err(ProviderError::data(
            "Binance trades outside the requested window",
        ));
    }
    for _ in 1..pages {
        let first = &trades[0];
        let last = &trades[trades.len() - 1];
        if first.id < PAGE
            || first.time <= start
            || last.time.saturating_sub(first.time) >= (limit as i64 + 2) * timeframe.nominal_ms()
        {
            break;
        }
        let preceding = fetch(&format!("fromId={}", first.id - PAGE));
        let Ok(mut preceding) = preceding else { break };
        validate_page(&preceding)?;
        if preceding
            .iter()
            .any(|trade| !(end.saturating_sub(2 * DAY_MS)..end).contains(&trade.time))
        {
            break;
        }
        if preceding
            .last()
            .is_none_or(|last| last.id.checked_add(1) != Some(first.id))
        {
            break;
        }
        preceding.append(&mut trades);
        trades = preceding;
    }
    let earliest = trades.iter().map(|trade| trade.time).min().unwrap_or(end);
    let latest = trades.iter().map(|trade| trade.time).max().unwrap_or(end);
    // Both edge buckets can be partial. The last trade is proof only of earlier buckets.
    let start = start.max(next_bucket_start_ms(timeframe, earliest));
    let end = end.min(bucket_start_ms(timeframe, latest));
    let mut seconds = Seconds::new(start, end);
    for trade in trades
        .into_iter()
        .filter(|trade| (start..end).contains(&trade.time))
    {
        seconds.add(trade.time, trade.id, &trade.price, &trade.quantity)?;
    }
    Ok(window(&seconds.finish(), timeframe, start, end, limit))
}

fn validate_page(trades: &[Trade]) -> Result<(), ProviderError> {
    if trades
        .windows(2)
        .any(|pair| pair[0].id.checked_add(1) != Some(pair[1].id))
    {
        return Err(ProviderError::data("incomplete Binance trade page"));
    }
    Ok(())
}

fn window(bars: &[Kline], timeframe: Timeframe, start: i64, end: i64, limit: usize) -> Vec<Kline> {
    if start >= end {
        return Vec::new();
    }
    // Slice before folding, so accessing one page does not repeatedly fold a whole archive.
    let from = bars.partition_point(|bar| bar.open_time < start);
    let last = bars.partition_point(|bar| bar.open_time < end);
    let first = from.max(
        last.saturating_sub(
            limit
                .saturating_add(2)
                .saturating_mul(usize::from(timeframe.count())),
        ),
    );
    if first >= last {
        return Vec::new();
    }
    let mut folded = fold(&bars[first..last], timeframe, false);
    let lower = if first == from {
        start
    } else {
        bars[first].open_time.max(start)
    };
    folded.retain(|bar| bar.open_time >= lower && bar.close_time < end);
    if folded.len() > limit {
        folded.drain(..folded.len() - limit);
    }
    folded
}

// The cache stores converted 1s bars. Rolling slots bound disk use even without periodic pruning.
type CacheBar = (i64, f64, f64, f64, f64, f64);

fn day_bars(symbol: &str, day: i64) -> Result<Option<Vec<Kline>>, ProviderError> {
    let cache = disk::global();
    let key = format!(
        "binance-usdm-seconds-v1-{symbol}-{}.json",
        day.rem_euclid(ARCHIVE_DAYS)
    );
    if let Some(stored) = cache.read(Kind::Lookups, &key)
        && let Ok((cached_day, bars)) =
            serde_json::from_slice::<(i64, Vec<CacheBar>)>(&stored.bytes)
        && day == cached_day
    {
        return Ok(Some(
            bars.into_iter()
                .map(|(open_time, open, high, low, close, volume)| Kline {
                    open_time,
                    close_time: open_time + 999,
                    open,
                    high,
                    low,
                    close,
                    volume,
                })
                .collect(),
        ));
    }
    let generation = cache.generation();
    let (year, month, date) = civil_from_days(day);
    let name = format!("{symbol}-aggTrades-{year:04}-{month:02}-{date:02}");
    let url = format!("{ARCHIVE_ROOT}/{symbol}/{name}.zip");
    let bytes = match http::get_archive_bytes(&url, MAX_ZIP_BYTES) {
        Ok(bytes) => bytes,
        Err(http::Error::Status(404)) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let bars = decode_archive(bytes, &name, day)?;
    if cache.is_enabled() {
        let compact: Vec<CacheBar> = bars
            .iter()
            .map(|bar| {
                (
                    bar.open_time,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.volume,
                )
            })
            .collect();
        if let Ok(bytes) = serde_json::to_vec(&(day, compact)) {
            cache.write(Kind::Lookups, &key, &bytes, generation);
        }
    }
    Ok(Some(bars))
}

fn decode_archive(bytes: Vec<u8>, name: &str, day: i64) -> Result<Vec<Kline>, ProviderError> {
    let mut archive = zip::ZipArchive::new(Cursor::new(bytes)).map_err(ProviderError::data)?;
    let file = archive
        .by_name(&format!("{name}.csv"))
        .map_err(ProviderError::data)?;
    if file.size() > MAX_CSV_BYTES {
        return Err(ProviderError::data(
            "Binance trade archive exceeds the size limit",
        ));
    }
    read_csv(BufReader::new(file), day * DAY_MS)
}

fn read_csv(mut reader: impl BufRead, start: i64) -> Result<Vec<Kline>, ProviderError> {
    let end = start + DAY_MS;
    let mut seconds = Seconds::new(start, end);
    let mut line = String::new();
    let mut last_id: Option<u64> = None;
    loop {
        line.clear();
        if reader.read_line(&mut line).map_err(ProviderError::data)? == 0 {
            break;
        }
        if last_id.is_none() && line.starts_with("agg_trade_id,") {
            continue;
        }
        let mut fields = line.trim_end().split(',');
        let row = (fields.next(), fields.next(), fields.next(), fields.nth(2));
        let (Some(id), Some(price), Some(quantity), Some(time)) = row else {
            return Err(ProviderError::data("invalid Binance archive row"));
        };
        let id: u64 = id.parse().map_err(ProviderError::data)?;
        let time: i64 = time.parse().map_err(ProviderError::data)?;
        if !(start..end).contains(&time)
            || last_id.is_some_and(|last| last.checked_add(1) != Some(id))
        {
            return Err(ProviderError::data(
                "incomplete or misdated Binance archive",
            ));
        }
        seconds.add(time, id, price, quantity)?;
        last_id = Some(id);
    }
    if last_id.is_none() {
        return Err(ProviderError::data("empty Binance trade archive"));
    }
    Ok(seconds.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn archive_rows_build_ohlcv_using_trade_time_and_id() {
        let csv = "agg_trade_id,price,quantity,first_trade_id,last_trade_id,transact_time,is_buyer_maker\n\
                   1,10,1,1,1,4100,false\n2,12,2,2,2,4200,false\n3,8,3,3,3,4000,true\n\
                   4,11,4,4,4,5000,false\n5,14,5,5,5,6000,false\n";
        let seconds = read_csv(csv.as_bytes(), 0).unwrap();
        assert_eq!(seconds.len(), 3);
        assert_eq!(
            seconds[0],
            Kline {
                open_time: 4000,
                close_time: 4999,
                open: 8.,
                high: 12.,
                low: 8.,
                close: 12.,
                volume: 6.,
            }
        );
        let interval = Timeframe::parse("5s").unwrap();
        let bars = window(&seconds, interval, 0, 10_000, 2);
        assert_eq!(
            bars.iter()
                .map(|bar| (bar.open_time, bar.volume))
                .collect::<Vec<_>>(),
            [(0, 6.), (5000, 9.)]
        );
        assert!(window(&seconds, interval, 8000, 9000, 2).is_empty());
        assert!(window(&seconds, interval, 5005, 9000, 2).is_empty());
        assert_eq!(window(&seconds, interval, 0, 10_000, 1), bars[1..]);
    }

    #[test]
    fn recent_pages_join_by_id_and_leave_partial_edge_candles_out() {
        let trade = |id, time, quantity: &str| Trade {
            id,
            time,
            price: "10".into(),
            quantity: quantity.into(),
        };
        let mut requests = Vec::new();
        let bars = recent_with(Timeframe::SECOND_1, 0, 5500, 20, 2, |query| {
            requests.push(query.to_owned());
            Ok(match query {
                "endTime=5499" => vec![
                    trade(1003, 3200, "3"),
                    trade(1004, 4100, "1"),
                    trade(1005, 5100, "1"),
                ],
                "fromId=3" => vec![
                    trade(1000, 1100, "1"),
                    trade(1001, 2200, "2"),
                    trade(1002, 3100, "4"),
                ],
                _ => panic!("unexpected request"),
            })
        })
        .unwrap();
        assert_eq!(requests, ["endTime=5499", "fromId=3"]);
        assert_eq!(
            bars.iter()
                .map(|bar| (bar.open_time, bar.volume))
                .collect::<Vec<_>>(),
            [(2000, 2.), (3000, 7.), (4000, 1.)]
        );
    }

    #[test]
    fn archives_with_an_id_gap_or_an_out_of_day_trade_are_rejected() {
        let bad = "1,10,1,1,1,4100,false\n3,12,2,3,3,4200,false\n";
        assert!(read_csv(bad.as_bytes(), 0).is_err());
        let bad = "1,10,1,1,1,86400000,false\n";
        assert!(read_csv(bad.as_bytes(), 0).is_err());
    }

    #[test]
    #[ignore = "uses locally downloaded ETH and BTC Binance Data files"]
    fn both_official_day_archives_can_be_read_and_paged() {
        let path = std::env::var("DEGENVIEW_BINANCE_ARCHIVE_TEST_DIR").expect("archive directory");
        let day = crate::format::days_from_civil(2026, 11, 23);
        let end = (day + 1) * DAY_MS;
        let five_seconds = Timeframe::parse("5s").unwrap();
        for symbol in ["BTCUSDT", "ETHUSDT"] {
            let name = format!("{symbol}-aggTrades-2026-11-23");
            let bytes =
                std::fs::read(std::path::Path::new(&path).join(format!("{name}.zip"))).unwrap();
            let bars = decode_archive(bytes, &name, day).unwrap();
            assert!(bars.len() > 80_000, "{symbol}");
            assert_eq!(bars[0].open_time, day * DAY_MS);
            let page = window(&bars, five_seconds, day * DAY_MS, end, 1000);
            assert_eq!(page.len(), 1000);
            assert_eq!(page.last().unwrap().close_time, end - 1);
            let previous = window(&bars, five_seconds, day * DAY_MS, page[0].open_time, 1000);
            assert_eq!(previous.last().unwrap().close_time + 1, page[0].open_time);
        }
    }
}
