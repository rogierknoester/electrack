use axum::async_trait;
use chrono::{DateTime, FixedOffset, TimeZone, Utc};
use serde::Serialize;
use sqlx::FromRow;
use thiserror::Error;

/// A representation of a price starting at a certain moment in time.
#[derive(Serialize, Debug, Clone, FromRow)]
pub(crate) struct PricePoint {
    pub(crate) moment: DateTime<Utc>,
    pub(crate) monetary_amount: f64,
}

#[derive(Debug, Clone, FromRow, Serialize)]
pub(crate) struct PriceWindow {
    pub(crate) starts_at: DateTime<FixedOffset>,
    pub(crate) ends_at: DateTime<FixedOffset>,
    pub(crate) average_price: String,
}

impl PriceWindow {
    pub(crate) fn with_timezone<Tz: TimeZone>(&self, timezone: Tz) -> PriceWindow {
        PriceWindow {
            starts_at: self.starts_at.with_timezone(&timezone).fixed_offset(),
            ends_at: self.ends_at.with_timezone(&timezone).fixed_offset(),
            average_price: self.average_price.clone(),
        }
    }
}

#[async_trait]
pub(crate) trait ElectricityPriceProvider: Send + Sync {
    fn name(&self) -> &'static str;

    async fn fetch_prices(&self) -> Result<Vec<PricePoint>, ElectricityProviderError>;
}

#[derive(Debug, Clone, Error)]
pub enum ElectricityProviderError {
    #[error("failed to fetch prices: {0}")]
    FetchPrices(String),
}
