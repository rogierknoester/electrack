use axum::async_trait;
use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, TimeZone, Utc};
use domain::PricePoint;
use log::{error, info};
use price_repository::PriceRepository;
use serde_derive::{Deserialize, Serialize};
use sqlx::PgPool;
use thiserror::Error;
use tracing_subscriber::fmt::format::FmtSpan;

use crate::http::start_http_server;

mod domain;
mod http;
mod nordpool;
mod price_repository;
mod setup;
mod tibber;

const APP_NAME: &str = "electrack";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    info!("starting {}", APP_NAME);

    dotenv::dotenv().ok();

    start_http_server().await.unwrap();
    info!("shutting down {}", APP_NAME);
}
