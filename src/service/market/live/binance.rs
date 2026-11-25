//! Binance's combined kline stream: the symbols and interval are in the URL, nothing is sent.

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
    let symbol = symbols.iter().find(|symbol| symbol.as_str() == "BTCUSDT")?;
    Some(combined_session(
        USD_M_HOST,
        "/market/stream",
        std::slice::from_ref(symbol),
        base,
    ))
}

fn combined_session(host: &str, path: &str, symbols: &[String], base: Timeframe) -> Session {
    let interval = base.interval();
    let streams = symbols
        .iter()
        .map(|symbol| format!("{}@kline_{interval}", symbol.to_lowercase()))
        .collect::<Vec<_>>()
        .join("/");
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
    let value: Value = serde_json::from_str(text).ok()?;
    let stream = value.get("stream")?.as_str()?;
    let symbol = stream.split('@').next()?.to_uppercase();
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
    Some((
        symbol,
        Kline {
            open_time: t,
            close_time,
            open,
            high,
            low,
            close,
            volume,
        },
    ))
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
            "BTCUSDT".to_owned(),
            "ETHUSDT".to_owned(),
        ];
        let session = usdm_session(&symbols, Timeframe::MINUTE_1).unwrap();
        assert_eq!(
            session.url,
            "wss://fstream.binance.com/market/stream?streams=btcusdt@kline_1m"
        );
        assert!(usdm_session(&symbols[2..], Timeframe::MINUTE_1).is_none());
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
