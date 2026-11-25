//! Markets and their data: instruments, sources, timeframes, candles and prediction-market series.

pub mod bucket;
pub mod cmc;
pub mod feed;
pub mod history;
pub(in crate::model) mod instrument;
pub(in crate::model) mod kline;
pub mod line;
pub mod live;
pub mod natives;
pub(in crate::model) mod pm_series;
pub mod samples;
pub(in crate::model) mod source;
pub(crate) use source::{USD_M_CONTRACTS, is_usdm_perpetual, usdm_contract};
pub(in crate::model) mod timeframe;
pub mod trend;
pub mod venue;
