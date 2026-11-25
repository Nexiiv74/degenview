//! Build candles from one ordered aggregate-trade stream.

use super::{CandleUpdate, bucket_start_ms, next_bucket_start_ms};
use crate::model::{Kline, Timeframe};

#[derive(Clone, Copy, Debug)]
pub(crate) struct AggregateTrade {
    pub id: u64,
    pub time_ms: i64,
    pub price: f64,
    pub quantity: f64,
}

#[derive(Default)]
pub(crate) struct TradeBars {
    last_id: Option<u64>,
    /// The first bucket we can observe from its start after joining or losing trades.
    covered_from: i64,
    current: Option<Forming>,
}

struct Forming {
    candle: Kline,
    first: (i64, u64),
    last: (i64, u64),
}

impl TradeBars {
    pub fn push(&mut self, timeframe: Timeframe, trade: AggregateTrade) -> Option<CandleUpdate> {
        let open_time = bucket_start_ms(timeframe, trade.time_ms);
        if self.last_id.is_some_and(|id| trade.id <= id) {
            return None;
        }
        let continuous = self.last_id.is_some_and(|id| trade.id == id + 1);
        self.last_id = Some(trade.id);
        if !continuous {
            self.covered_from = next_bucket_start_ms(
                timeframe,
                self.current
                    .as_ref()
                    .map_or(open_time, |bar| bar.candle.open_time.max(open_time)),
            )
            .max(self.covered_from);
            return self
                .current
                .take()
                .map(|bar| CandleUpdate::Remove(bar.candle.open_time));
        }
        if open_time < self.covered_from {
            return None;
        }

        let order = (trade.time_ms, trade.id);
        let forming = match &mut self.current {
            Some(bar) if bar.candle.open_time > open_time => {
                // A late trade for a closed bucket can no longer update that bucket in place.
                return Some(CandleUpdate::Remove(open_time));
            }
            Some(bar) if bar.candle.open_time == open_time => {
                if order < bar.first {
                    bar.first = order;
                    bar.candle.open = trade.price;
                }
                if order > bar.last {
                    bar.last = order;
                    bar.candle.close = trade.price;
                }
                bar.candle.high = bar.candle.high.max(trade.price);
                bar.candle.low = bar.candle.low.min(trade.price);
                bar.candle.volume += trade.quantity;
                bar
            }
            slot => slot.insert(Forming {
                candle: Kline {
                    open_time,
                    close_time: next_bucket_start_ms(timeframe, open_time) - 1,
                    open: trade.price,
                    high: trade.price,
                    low: trade.price,
                    close: trade.price,
                    volume: trade.quantity,
                },
                first: order,
                last: order,
            }),
        };
        Some(CandleUpdate::Replace(forming.candle.clone()))
    }

    /// The latest published candle is provisional until a trade arrives in a later bucket.
    pub fn disconnect(&mut self) -> Option<CandleUpdate> {
        let previous = std::mem::take(self);
        previous
            .current
            .map(|bar| CandleUpdate::Remove(bar.candle.open_time))
    }
}
