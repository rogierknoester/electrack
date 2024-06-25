use std::sync::Arc;

use axum::{async_trait, Json, Router, serve};
use axum::extract::{Query, State};
use axum::routing::get;
use chrono::{DateTime, Local, NaiveDate, NaiveDateTime, Utc};
use log::{error, info};
use reqwest::StatusCode;
use serde_derive::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, QueryBuilder};
use sqlx::postgres::{PgPoolOptions};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

mod tibber;

const APP_NAME: &str = "electricity-price-optimiser";

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::CLOSE)
        .init();
    tracing::trace!("This is a trace message");

    info!("Starting {}", APP_NAME);

    let api_key = std::env::var("TIBBER_API_KEY").expect("TIBBER_API_KEY must be set");
    let port = std::env::var("PORT").unwrap_or("8080".to_string());

    let db_dsn = std::env::var("DB_URL").expect("DB_URL must be set");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_dsn)
        .await
        .expect("Failed to create pool");


    let tibber = tibber::Tibber::new(api_key.clone());
    let price_repository = PostgresPriceRepository::new(pool.clone());

    let app_state = AppState::new(
        pool,
        Arc::new(Box::new(tibber)),
        Arc::new(Box::new(price_repository)),
    );

    let router = Router::new()
        .route("/time-slots", get(get_time_slots))
        .with_state(app_state.clone())
        ;

    let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await.unwrap();

    info!("Now listening on port {}", port);

    serve(listener, router).await.unwrap();

    info!("Shutting down {}", APP_NAME);
}

#[derive(Clone)]
struct AppState {
    db: PgPool,
    electricity_provider: Arc<Box<dyn ElectricityProvider>>,
    price_repository: Arc<Box<dyn PriceRepository>>,
}

impl AppState {
    fn new(
        db: PgPool,
        electricity_provider: Arc<Box<dyn ElectricityProvider>>,
        price_repository: Arc<Box<dyn PriceRepository>>,
    ) -> Self {
        Self { db, electricity_provider, price_repository }
    }
}


#[derive(Debug, Clone, Deserialize)]
struct Windows {
    durations: String,
}

impl Default for Windows {
    fn default() -> Self {
        Self {
            durations: "".to_string()
        }
    }
}

#[instrument]
async fn has_prices_of_date(db: PgPool, date: NaiveDate) -> Result<bool, String> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM prices WHERE moment::date = $1")
        .bind(date)
        .fetch_one(&db)
        .await
        .map_err(|e| e.to_string())?;

    Ok(row.0 > 0)
}

#[instrument(skip(state))]
async fn get_time_slots(State(state): State<AppState>, windows: Option<Query<Windows>>) -> axum::response::Result<(StatusCode, Json<Vec<PriceWindow>>)> {
    if has_prices_of_date(state.db.clone(), Local::now().date_naive()).await.unwrap() {
        info!("Prices for today already fetched");
    } else {
        info!("Prices for today not yet fetched");
        let prices = state.electricity_provider.fetch_prices().await;

        let persisting_result = match prices {
            Ok(prices) => state.price_repository.persist_prices(prices).await,
            Err(_) => return Err((StatusCode::INTERNAL_SERVER_ERROR, "Failed to fetch prices from provider".to_string()).into())
        };

        if let Err(e) = persisting_result {
            error!("Failed to persist prices: {}", e);
            return Err((StatusCode::INTERNAL_SERVER_ERROR, "Failed to persist prices".to_string()).into());
        }
    }

    let durations = windows
        .unwrap_or_default()
        .0
        .durations
        .split(",")
        .map(|s| s.parse::<i32>().ok())
        .filter(|o| o.is_some())
        .map(|o| o.unwrap())
        .collect::<Vec<i32>>();

    let optimal_windows = state.price_repository.fetch_optimal_price_window_of_date_for_durations(Local::now().date_naive(), durations).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, Json(optimal_windows)))
}


/// A representation of a price starting at a certain moment in time.
#[derive(Serialize, Debug, Clone, FromRow)]
struct PricePoint {
    moment: DateTime<Utc>,
    monetary_amount: f64,
}


#[derive(Debug, Clone)]
enum ElectricityProviderError {
    FetchPrices(String)
}

#[derive(Debug, Clone, Error)]
enum PriceRepositoryError {
    #[error("the prices could not be persisted: {0}")]
    PersistenceError(String),
}

#[async_trait]
trait ElectricityProvider: Send + Sync {
    async fn fetch_prices(&self) -> Result<Vec<PricePoint>, ElectricityProviderError>;
}

#[derive(Debug, Clone, FromRow, Serialize)]
struct PriceWindow {
    starts_at: NaiveDateTime,
    ends_at: NaiveDateTime,
    average_price: String,
}

#[async_trait]
trait PriceRepository: Send + Sync {
    async fn fetch_prices_of_date(&self, date: NaiveDate) -> Result<Vec<PricePoint>, String>;

    async fn persist_prices(&self, prices: Vec<PricePoint>) -> Result<(), PriceRepositoryError>;

    async fn fetch_optimal_price_window_of_date_for_durations(&self, date: NaiveDate, durations: Vec<i32>) -> Result<Vec<PriceWindow>, String>;
}

#[derive(Clone, Debug)]
struct PostgresPriceRepository {
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
        let rows = sqlx::query_as::<_, PricePoint>("SELECT moment, monetary_amount FROM prices WHERE moment::date = $1")
            .bind(date)
            .fetch_all(&self.db)
            .await
            .map_err(|e| e.to_string())?;

        Ok(rows)
    }

    async fn persist_prices(&self, prices: Vec<PricePoint>) -> Result<(), PriceRepositoryError> {
        info!("Persisting {} prices", prices.len());
        let mut query_builder = QueryBuilder::new("insert into prices (moment, price)");

        query_builder.push_values(prices, |mut builder, price| {
            builder
                .push_bind(price.moment)
                .push_bind(price.monetary_amount);
        });

        let query = query_builder.build();

        query
            .execute(&self.db)
            .await
            .map(|_| ())
            .map_err(|e| PriceRepositoryError::PersistenceError(e.to_string()))
    }

    #[instrument]
    async fn fetch_optimal_price_window_of_date_for_durations(&self, date: NaiveDate, durations: Vec<i32>) -> Result<Vec<PriceWindow>, String> {

        let mut windows: Vec<PriceWindow> = Vec::new();

        for mut duration in durations {

            duration = duration - 1;
            duration = duration.clamp(0, 23);

            let row = sqlx::query_as::<_, PriceWindow>(r#"
            select moment                                                               as starts_at,
            round((avg(prices.price) over price_window)::numeric, 3)::varchar                    as average_price,
            ((max(moment) over price_window) + interval '1 hour' - interval '1 second') as ends_at
            from prices
            where moment::date = $1
            window price_window as ( partition by moment::date order by moment rows between current row and $2 following )
            order by average_price
            limit 1
            "#
            )
                .bind(date)
                .bind(duration)
                .fetch_one(&self.db)
                .await
                .map_err(|e| e.to_string())?;

            windows.push(row)
        }


        Ok(windows)
    }
}