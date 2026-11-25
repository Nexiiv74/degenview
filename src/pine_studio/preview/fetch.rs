//! The candles: when to fetch and refresh them, taking a fetch's answer, and switching the
//! market or timeframe.

use super::{
    CANDLE_LIMIT, MARKET_KEY, PreviewMarket, PreviewState, RECENTS_KEY, RECENTS_LIMIT,
    TIMEFRAME_KEY,
};
use crate::format;
use crate::i18n::tr;
use crate::model::market::history::{MAX_CANDLES, with_older};
use crate::model::market::live::LiveMark;
use crate::model::{DataSource, InstrumentId, Timeframe};
use crate::service::market::live::LiveFeed;
use crate::service::work_queue;
use crate::service::{self, ProviderError};
use eframe::egui;
use std::time::Instant;

impl PreviewState {
    pub(super) fn want_fetch(&self) -> bool {
        let key = (self.market.clone(), self.timeframe);
        self.fetched_for.as_ref() != Some(&key) && self.inbox.is_none()
    }

    pub(super) fn fetch(&mut self, ctx: &egui::Context) {
        // Unit tests draw the pane headlessly and must not touch the network.
        if cfg!(test) {
            self.fetched_for = Some((self.market.clone(), self.timeframe));
            return;
        }
        let market = self.market.clone();
        let timeframe = self.timeframe;
        let live = self.streams_seconds();
        self.inbox = Some(work_queue::start_for(market.source, Some(ctx), move || {
            if live {
                service::fetch_klines_live(market.source, &market.ticker, timeframe, CANDLE_LIMIT)
            } else {
                service::fetch_klines(market.source, &market.ticker, timeframe, CANDLE_LIMIT)
                    .map(Into::into)
            }
        }));
        self.loading = true;
        self.last_fetch = Some(Instant::now());
        self.fetched_for = Some((self.market.clone(), self.timeframe));
    }

    fn streams_seconds(&self) -> bool {
        self.market.source == DataSource::BinanceUsdM && self.timeframe.is_subminute()
    }

    pub(super) fn stream_seconds(&mut self, ctx: &egui::Context) {
        if cfg!(test) || !self.streams_seconds() {
            return;
        }
        let Some(plan) = self.market.source.plan(self.timeframe) else {
            return;
        };
        let base = plan.base();
        if self.live_feed.is_none() {
            let instrument = InstrumentId::new(self.market.source, &self.market.ticker);
            self.live_feed = LiveFeed::connect(
                instrument.source,
                vec![instrument.api_symbol()],
                base,
                ctx.clone(),
            );
        }
        let Some(feed) = &self.live_feed else { return };
        let updates = feed.inbox.drain();
        if updates.is_empty() {
            return;
        }
        let mut candles = std::mem::take(&mut self.candles);
        for (_, candle) in updates {
            if let Some(open_time) =
                self.live_fold
                    .apply(self.timeframe, base, &mut candles, candle)
            {
                self.live_mark = Some(LiveMark::new(open_time, Instant::now()));
            }
        }
        let skip = candles.len().saturating_sub(MAX_CANDLES);
        candles.drain(..skip);
        self.set_candles(candles);
    }

    pub(super) fn receive_candles(&mut self) {
        let Some(fetch) = &mut self.inbox else {
            return;
        };
        let Some(result) = fetch.poll_or(|| {
            Err(ProviderError::other(tr!(
                "The candles could not be loaded."
            )))
        }) else {
            return;
        };
        self.inbox = None;
        self.loading = false;
        match result {
            Ok(fresh) => {
                let candles = if self.streams_seconds() {
                    let live_open = self
                        .last_fetch
                        .and_then(|at| self.live_mark.and_then(|mark| mark.guard_for(at)));
                    let candles = self.live_fold.merge_fetched(
                        self.timeframe,
                        fresh.candles,
                        fresh.bars,
                        &self.candles,
                        live_open,
                        CANDLE_LIMIT as usize,
                    );
                    with_older(&self.candles, candles, true)
                } else {
                    fresh.candles
                };
                self.set_candles(candles);
                self.error = None;
            }
            Err(error) => self.error = Some(error),
        }
    }

    pub(super) fn select_market(&mut self, market: PreviewMarket) {
        if market != self.market {
            self.set_candles(Vec::new());
            self.show_newest_bars();
            self.market = market.clone();
            self.reset_fetch();
        }
        self.recents.retain(|m| m != &market);
        self.recents.insert(0, market);
        self.recents.truncate(RECENTS_LIMIT);
        self.save(MARKET_KEY, &self.market);
        self.save(RECENTS_KEY, &self.recents);
    }

    pub(in crate::pine_studio) fn select_timeframe(&mut self, timeframe: Timeframe) {
        if timeframe != self.timeframe {
            self.timeframe = timeframe;
            self.set_candles(Vec::new());
            self.show_newest_bars();
            self.reset_fetch();
            self.save(TIMEFRAME_KEY, &timeframe);
        }
    }

    fn reset_fetch(&mut self) {
        self.inbox = None;
        self.live_feed = None;
        self.live_fold.reset();
        self.live_mark = None;
        self.fetched_for = None;
        self.last_fetch = None;
        self.loading = false;
        self.error = None;
    }

    /// Back to the newest bars with the price axis on autoscale, as the workspace does when a
    /// chart's market or timeframe changes. A price range placed by dragging is in the old
    /// market's prices and would leave the new one off screen. The candle count and price zoom
    /// are the person's saved choices, so they stay.
    fn show_newest_bars(&mut self) {
        let (candles, zoom) = (self.card.visible_candles(), self.card.y_zoom());
        self.card.reset_view();
        self.card.set_visible_candles(candles);
        self.card.set_y_zoom(zoom);
    }

    /// The last close and the change across the candles on screen. The warm-up candles off the
    /// left edge are not counted: they would report a move nobody can see.
    pub(super) fn price_line(&self) -> Option<(String, f64)> {
        let last = self.candles.last()?;
        let change = self.card.visible_change(&self.candles)?;
        Some((format::price(last.close), change))
    }
}
