use axum::async_trait;
use chrono::{DateTime, NaiveDate, Utc};
use sqlx::{FromRow, PgPool, QueryBuilder};
use thiserror::Error;
use tracing::{error, info, instrument};

use crate::domain::{PricePoint, PriceWindow};

#[derive(Debug, Clone, Error)]
pub(crate) enum PriceRepositoryError {
    #[error("the prices could not be persisted: {0}")]
    PersistenceError(String),
}

#[async_trait]
pub(crate) trait PriceRepository: Send + Sync {
    async fn fetch_prices_of_date(&self, date: NaiveDate) -> Result<Vec<PricePoint>, String>;

    async fn persist_prices(
        &self,
        prices: &[PricePoint],
        provider_name: &str,
    ) -> Result<(), PriceRepositoryError>;

    async fn fetch_optimal_price_window_of_window_for_durations(
        &self,
        start_moment: DateTime<Utc>,
        end_moment: DateTime<Utc>,
        durations: &[i32],
    ) -> Result<Vec<PriceWindow>, String>;

    async fn fetch_optimal_upcoming_window(
        &self,
        duration: i32,
    ) -> Result<Vec<PriceWindow>, String>;
}

#[derive(Clone, Debug)]
pub(crate) struct PostgresPriceRepository {
    db: PgPool,
}

impl PostgresPriceRepository {
    pub fn new(db: PgPool) -> Self {
        Self { db }
    }
}

#[async_trait]
impl PriceRepository for PostgresPriceRepository {
    async fn fetch_prices_of_date(&self, date: NaiveDate) -> Result<Vec<PricePoint>, String> {
        let rows = sqlx::query_as::<_, PricePoint>(
            "SELECT moment, monetary_amount FROM prices WHERE moment::date = $1",
        )
        .bind(date)
        .fetch_all(&self.db)
        .await
        .map_err(|e| e.to_string())?;

        Ok(rows)
    }

    async fn persist_prices(
        &self,
        prices: &[PricePoint],
        provider_name: &str,
    ) -> Result<(), PriceRepositoryError> {
        let provider: Provider =
            sqlx::query_as("select id, name from providers where name = $1 limit 1")
                .bind(provider_name)
                .fetch_one(&self.db)
                .await
                .map_err(|e| PriceRepositoryError::PersistenceError(e.to_string()))?;

        info!("Persisting {} prices for {}", prices.len(), provider.name);

        let mut query_builder =
            QueryBuilder::new("insert into prices (moment, price, provider_id)");

        query_builder.push_values(prices, |mut builder, price| {
            builder
                .push_bind(price.moment)
                .push_bind(price.monetary_amount)
                .push_bind(provider.id);
        });

        let query = query_builder.build();

        query
            .execute(&self.db)
            .await
            .map(|_| ())
            .map_err(|e| PriceRepositoryError::PersistenceError(e.to_string()))
    }

    #[instrument(skip(self))]
    async fn fetch_optimal_price_window_of_window_for_durations(
        &self,
        start_moment: DateTime<Utc>,
        end_moment: DateTime<Utc>,
        durations: &[i32],
    ) -> Result<Vec<PriceWindow>, String> {
        let mut windows: Vec<PriceWindow> = Vec::new();

        for duration in durations.iter() {
            let mut duration: i32 = *duration;

            duration -= 1;
            duration = duration.clamp(0, 23);

            let row = sqlx::query_as::<_, PriceWindow>(r#"
            select moment                                                                        as starts_at,
            round((avg(prices.price) over price_window)::numeric, 3)::varchar                    as average_price,
            ((max(moment) over price_window) + interval '59 minutes 59 seconds') as ends_at
            from prices
            where moment::timestamptz >= $1 and moment::timestamptz <= $2
            window price_window as ( partition by moment::date order by moment rows between current row and $3 following )
            order by average_price
            limit 1
            "#
            )
                .bind(start_moment)
                .bind(end_moment)
                .bind(duration)
                .fetch_one(&self.db)
                .await
                .map_err(|e| e.to_string())?;

            windows.push(row)
        }

        Ok(windows)
    }

    async fn fetch_optimal_upcoming_window(
        &self,
        duration: i32,
    ) -> Result<Vec<PriceWindow>, String> {
        let duration = duration.clamp(0, 23);

        let _row = sqlx::query_as::<_, PriceWindow>(r#"
            select moment                                                                        as starts_at,
            round((avg(prices.price) over price_window)::numeric, 3)::varchar                    as average_price,
            ((max(moment) over price_window) + interval '59 minutes 59 seconds') as ends_at
            from prices
            where moment::timestamptz >= $1 and moment::timestamptz <= $2
            window price_window as ( partition by moment::date order by moment rows between current row and $3 following )
            order by average_price
            limit 1
            "#
            )
                .bind(Utc::now())
                .bind(duration)
                .fetch_one(&self.db)
                .await
                .map_err(|e| e.to_string())?;

        return Ok(vec![]);
    }
}

#[derive(FromRow)]
struct Provider {
    id: i64,
    name: String,
}
