//! Binance combined spot and USD-M kline streams.

use crate::model::market::{USD_M_CONTRACTS, usdm_contract};
use crate::model::{Kline, Timeframe};
use crate::service::net::ws::{Keepalive, Session};
use serde_json::Value;
use std::time::Duration;

const SPOT_HOST: &str = "stream.binance.com";
const USD_M_HOST: &str = "fstream.binance.com";

pub(super) fn session(symbols: &[String], base: Timeframe) -> Session {
    combined_session(SPOT_HOST, "/stream", symbols, base)
}

pub(super) fn usdm_session(symbols: &[String], base: Timeframe) -> Option<Session> {
    let supported: Vec<_> = USD_M_CONTRACTS
        .into_iter()
        .filter(|&id| {
            symbols
                .iter()
                .any(|symbol| usdm_contract(symbol) == Some(id))
        })
        .map(str::to_owned)
        .collect();
    if supported.is_empty() {
        return None;
    }
    if base == Timeframe::SECOND_1 {
        let streams = supported.iter().map(|symbol| {
            format!(
                "{}_perpetual@continuousKline_1s",
                symbol.to_ascii_lowercase()
            )
        });
        Some(stream_session(USD_M_HOST, "/market/stream", streams))
    } else {
        Some(combined_session(
            USD_M_HOST,
            "/market/stream",
            &supported,
            base,
        ))
    }
}

fn combined_session(host: &str, path: &str, symbols: &[String], base: Timeframe) -> Session {
    let interval = base.interval();
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@kline_{interval}", symbol.to_lowercase()));
    stream_session(host, path, streams)
}

fn stream_session(host: &str, path: &str, streams: impl Iterator<Item = String>) -> Session {
    let streams = streams.collect::<Vec<_>>().join("/");
    Session {
        url: format!("wss://{host}{path}?streams={streams}"),
        host: host.to_owned(),
        port: 443,
        subscribe: Vec::new(),
        max_data_silence: Duration::from_secs(60),
        keepalive: Keepalive::Frame,
    }
}

/// `{"stream":"btcusdt@kline_1h","data":{"k":{"t","o","h","l","c","v",…}}}`.
pub(super) fn parse_kline(text: &str) -> Option<(String, Kline)> {
    parse(text, |_, _, _| {})
}

pub(super) fn parse_usdm_kline(text: &str) -> Option<(String, Kline)> {
    parse(text, crate::service::market::binance::record_usdm_second)
}

fn parse(text: &str, record_second: impl FnOnce(&str, &Kline, bool)) -> Option<(String, Kline)> {
    let value: Value = serde_json::from_str(text).ok()?;
    let stream = value.get("stream")?.as_str()?;
    let (symbol, topic) = stream.split_once('@')?;
    let symbol = if topic.starts_with("continuousKline_") {
        symbol.strip_suffix("_perpetual")?
    } else {
        symbol
    }
    .to_uppercase();
    let k = value.pointer("/data/k")?;
    let t = k.get("t").and_then(Value::as_i64)?;
    // `T` is the bar's last millisecond, as in the REST rows. Without it the forming bar would read
    // as already closed (`close_time <= now`) and Pine would rebuild on every tick.
    let close_time = k.get("T").and_then(Value::as_i64).unwrap_or(t);
    let open = k.get("o").and_then(Value::as_str)?.parse().ok()?;
    let high = k.get("h").and_then(Value::as_str)?.parse().ok()?;
    let low = k.get("l").and_then(Value::as_str)?.parse().ok()?;
    let close = k.get("c").and_then(Value::as_str)?.parse().ok()?;
    let volume = k.get("v").and_then(Value::as_str)?.parse().ok()?;
    let candle = Kline {
        open_time: t,
        close_time,
        open,
        high,
        low,
        close,
        volume,
    };
    if topic == "continuousKline_1s" {
        record_second(
            &symbol,
            &candle,
            k.get("x").and_then(Value::as_bool) == Some(true),
        );
    }
    Some((symbol, candle))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_symbols_and_interval_are_in_the_url() {
        let session = session(&["BTCUSDT".into(), "ETHUSDT".into()], Timeframe::HOUR_1);
        assert_eq!(
            session.url,
            "wss://stream.binance.com/stream?streams=btcusdt@kline_1h/ethusdt@kline_1h"
        );
        assert!(session.subscribe.is_empty());
    }

    #[test]
    fn usdm_streams_use_the_futures_market_route_and_only_the_supported_contract() {
        let symbols = [
            "BTCUSDT".to_owned(),
            "btcusdt.p".to_owned(),
            "ETHUSDT.P".to_owned(),
            "SOLUSDT".to_owned(),
        ];
        let session = usdm_session(&symbols, Timeframe::MINUTE_1).unwrap();
        assert_eq!(
            session.url,
            "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m/ethusdt@kline_1m"
        );
        let base = crate::model::DataSource::BinanceUsdM
            .plan(Timeframe::parse("7s").unwrap())
            .unwrap()
            .base();
        let seconds = usdm_session(&symbols, base).unwrap();
        assert_eq!(
            seconds.url,
            "wss://fstream.binance.com/market/stream?streams=btcusdt_perpetual@continuousKline_1s/ethusdt_perpetual@continuousKline_1s"
        );
        assert!(usdm_session(&symbols[3..], Timeframe::MINUTE_1).is_none());
    }

    #[test]
    #[ignore = "needs the Binance USD-M public WebSocket"]
    fn usdm_continuous_stream_delivers_second_candles_for_both_markets() {
        use crate::model::DataSource;
        use crate::service::market::live::LiveFeed;
        use crate::service::net::proxy::{AppliedProxy, ProxyConfig};
        use std::collections::HashSet;
        use std::time::Instant;

        let _proxy = AppliedProxy::new(ProxyConfig::default());
        let feed = LiveFeed::connect(
            DataSource::BinanceUsdM,
            USD_M_CONTRACTS.into_iter().map(str::to_owned).collect(),
            Timeframe::SECOND_1,
            eframe::egui::Context::default(),
        )
        .expect("futures continuous kline feed");
        let deadline = Instant::now() + Duration::from_secs(15);
        let mut seen = HashSet::new();
        while seen.len() < USD_M_CONTRACTS.len() {
            for (symbol, candle) in feed.inbox.drain() {
                assert_eq!(candle.open_time % 1000, 0);
                assert_eq!(candle.close_time - candle.open_time, 999);
                assert!(candle.volume >= 0.);
                assert!(candle.high >= candle.open.max(candle.close));
                assert!(candle.low <= candle.open.min(candle.close));
                assert!(USD_M_CONTRACTS.contains(&symbol.as_str()));
                seen.insert(symbol);
            }
            assert!(
                Instant::now() < deadline,
                "no futures seconds candles for every contract: {seen:?}"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    #[ignore = "needs the Binance public endpoints"]
    fn usdm_live_chart_has_real_klines_and_older_history() {
        use crate::model::{DataSource, InstrumentId};
        use crate::service::net::proxy::{AppliedProxy, ProxyConfig};
        use crate::service::{fetch_klines, fetch_klines_range, market::live::LiveFeed};
        use std::time::Instant;

        let _proxy = AppliedProxy::new(ProxyConfig::default());
        let instrument = InstrumentId::new(DataSource::BinanceUsdM, "BTCUSDT.P");
        let id = instrument.api_symbol();
        let timeframe = Timeframe::MINUTE_1;
        let feed = LiveFeed::connect(
            instrument.source,
            vec![id.clone()],
            timeframe,
            eframe::egui::Context::default(),
        )
        .expect("futures feed");
        let bars = fetch_klines(instrument.source, &id, timeframe, 4).expect("recent bars");
        assert_eq!(bars.len(), 4);
        let end = bars[0].open_time;
        let older = fetch_klines_range(instrument.source, &id, timeframe, end - 240_000, end, 4)
            .expect("older page");
        assert_eq!(older.len(), 4);
        assert_eq!(older[3].close_time + 1, end);

        let deadline = Instant::now() + Duration::from_secs(15);
        loop {
            if let Some((symbol, candle)) = feed.inbox.drain().pop() {
                assert_eq!(symbol, id);
                assert!(candle.close > 0.0);
                assert!(candle.high >= candle.close && candle.low <= candle.close);
                assert_eq!(candle.close_time - candle.open_time, 59_999);
                break;
            }
            assert!(
                Instant::now() < deadline,
                "no futures candle from the socket"
            );
            std::thread::sleep(Duration::from_millis(50));
        }
    }

    #[test]
    fn a_second_chart_streams_the_one_second_kline() {
        let session = session(&["BTCUSDT".into()], Timeframe::SECOND_1);
        assert_eq!(
            session.url,
            "wss://stream.binance.com/stream?streams=btcusdt@kline_1s"
        );
    }

    #[test]
    fn parses_a_continuous_candle_under_the_futures_api_symbol() {
        let text = r#"{"stream":"ethusdt_perpetual@continuousKline_1s","data":{"e":"continuous_kline","ps":"ETHUSDT","ct":"PERPETUAL","k":{"t":1700000000000,"T":1700000000999,"i":"1s","o":"10","h":"12","l":"9","c":"11","v":"42.5"}}}"#;
        let (symbol, candle) = parse_kline(text).expect("continuous kline");
        assert_eq!(symbol, "ETHUSDT");
        assert_eq!(candle.close_time - candle.open_time, 999);
        assert_eq!(
            (
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume
            ),
            (10.0, 12.0, 9.0, 11.0, 42.5)
        );
    }

    #[test]
    fn parses_kline_stream_message() {
        let text = r#"{"stream":"btcusdt@kline_1h","data":{"k":{"t":1700000000000,"T":1700003599999,"o":"10","h":"12","l":"9","c":"11","v":"42.5","q":"0","x":false}}}"#;
        let (symbol, kline) = parse_kline(text).expect("parse");
        assert_eq!(symbol, "BTCUSDT");
        assert_eq!(kline.open_time, 1_700_000_000_000);
        assert_eq!(kline.close_time, 1_700_003_599_999);
        assert_eq!(kline.high, 12.0);
        assert_eq!(kline.low, 9.0);
        assert_eq!(kline.close, 11.0);
        assert_eq!(kline.volume, 42.5);
    }

    #[test]
    fn a_message_without_a_close_time_falls_back_to_the_open_time() {
        let text = r#"{"stream":"btcusdt@kline_1h","data":{"k":{"t":1700000000000,"o":"10","h":"12","l":"9","c":"11","v":"1"}}}"#;
        let (_, kline) = parse_kline(text).expect("parse");
        assert_eq!(kline.close_time, 1_700_000_000_000);
    }
}
