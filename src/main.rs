use std::sync::Arc;

use axum::{async_trait, Json, Router, serve};
use axum::extract::{Query, State};
use axum::routing::get;
use axum_macros::debug_handler;
use chrono::{DateTime, FixedOffset, Local, NaiveDate, NaiveTime, TimeZone, Utc};
use log::{error, info};
use reqwest::StatusCode;
use serde_derive::{Deserialize, Serialize};
use sqlx::{FromRow, PgPool, QueryBuilder, Row};
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgPoolOptions};
use thiserror::Error;
use tokio::net::TcpListener;
use tracing::instrument;
use tracing_subscriber::fmt::format::FmtSpan;

mod tibber;
mod nordpool;

const APP_NAME: &str = "electricity-price-optimiser";

static MIGRATOR: Migrator = sqlx::migrate!();

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::TRACE)
        .with_span_events(FmtSpan::CLOSE)
        .init();

    info!("Starting {}", APP_NAME);

    let api_key = std::env::var("TIBBER_API_KEY").expect("TIBBER_API_KEY must be set");
    let port = std::env::var("PORT").unwrap_or("8080".to_string());

    let db_dsn = std::env::var("DATABASE_URL").expect("DB_URL must be set");

    let db_pool = setup_db(&db_dsn).await;


    let tibber = tibber::Tibber::new(api_key.clone());
    let price_repository = PostgresPriceRepository::new(db_pool.clone());

    let app_state = AppState::new(
        db_pool,
        Arc::new(tibber),
        Arc::new(price_repository),
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

async fn setup_db(db_dsn: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_dsn)
        .await
        .expect("Failed to create database pool");

    MIGRATOR.run(&pool).await.expect("Failed to run migrations");

    pool
}

#[derive(Clone)]
struct AppState {
    db: PgPool,
    electricity_provider: Arc<dyn ElectricityProvider>,
    price_repository: Arc<dyn PriceRepository>,
}

impl AppState {
    fn new(
        db: PgPool,
        electricity_provider: Arc<dyn ElectricityProvider>,
        price_repository: Arc<dyn PriceRepository>,
    ) -> Self {
        Self { db, electricity_provider, price_repository }
    }
}


#[derive(Debug, Clone, Deserialize)]
struct TimeslotParameters {
    durations: String,
    moment_start: DateTime<FixedOffset>,
    moment_end: DateTime<FixedOffset>,
}

impl TimeslotParameters {
    fn get_durations(&self) -> Vec<i32> {
        self.durations
            .split(",")
            .map(|s| s.parse::<i32>().ok())
            .filter(|o| o.is_some())
            .map(|o| o.unwrap())
            .collect::<Vec<i32>>()
    }
}

impl Default for TimeslotParameters {
    fn default() -> Self {
        Self {
            durations: "".to_string(),
            moment_start: Utc::now().with_time(NaiveTime::from_hms_opt(0, 0, 0).unwrap()).unwrap().fixed_offset(),
            moment_end: Utc::now().with_time(NaiveTime::from_hms_opt(23, 59, 59).unwrap()).unwrap().fixed_offset(),
        }
    }
}

async fn has_prices_of_date(db: PgPool, date: NaiveDate) -> Result<bool, String> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM prices WHERE moment::date = $1")
        .bind(date)
        .fetch_one(&db)
        .await
        .map_err(|e| e.to_string())?;

    Ok(row.0 > 0)
}

#[debug_handler(state = AppState)]
async fn get_time_slots(State(state): State<AppState>, parameters: Query<TimeslotParameters>) -> axum::response::Result<(StatusCode, Json<Vec<PriceWindow>>)> {
    if !has_prices_of_date(state.db.clone(), Local::now().date_naive()).await.unwrap() {
        let price_fetching_result = fetch_prices_of_today_from_provider(
            &*state.electricity_provider,
            &*state.price_repository,
        ).await;

        if let Err(e) = price_fetching_result {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into());
        }
    }

    let durations = parameters.get_durations();

    let timezone_date_start = parameters.moment_start.timezone();

    let optimal_windows: Vec<PriceWindow> = state.price_repository
        .fetch_optimal_price_window_of_window_for_durations(parameters.moment_start.to_utc(), parameters.moment_end.to_utc(), durations.as_slice())
        .await
        .map(|windows| windows.into_iter().map(|window| window.with_timezone(timezone_date_start)).collect::<Vec<PriceWindow>>())
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, Json(optimal_windows)))
}


/// Fetch the prices of the provider for the current day
async fn fetch_prices_of_today_from_provider(electricity_provider: &dyn ElectricityProvider, price_repository: &dyn PriceRepository) -> Result<Vec<PricePoint>, ElectricityProviderError> {
    info!("Prices for today not yet fetched");
    let fetch_result = electricity_provider.fetch_prices().await;


    let persisting_result = match fetch_result {
        Ok(fetched_prices) => {
            info!("Fetched {} prices", fetched_prices.len());
            price_repository.persist_prices(&fetched_prices, electricity_provider.name()).await
                .and(Ok(fetched_prices))
        }
        Err(error) => {
            error!("{}", error);
            return Err(error.clone());
        }
    };

    match persisting_result {
        Ok(prices) => Ok(prices),
        Err(error) => {
            error!("{}", error);
            Err(ElectricityProviderError::FetchPrices(error.to_string()))
        }
    }
}


/// A representation of a price starting at a certain moment in time.
#[derive(Serialize, Debug, Clone, FromRow)]
struct PricePoint {
    moment: DateTime<Utc>,
    monetary_amount: f64,
}


#[derive(Debug, Clone, Error)]
enum ElectricityProviderError {
    #[error("failed to fetch prices: {0}")]
    FetchPrices(String)
}

#[derive(Debug, Clone, Error)]
enum PriceRepositoryError {
    #[error("the prices could not be persisted: {0}")]
    PersistenceError(String),
}

#[async_trait]
trait ElectricityProvider: Send + Sync {
    fn name(&self) -> &'static str;

    async fn fetch_prices(&self) -> Result<Vec<PricePoint>, ElectricityProviderError>;
}

#[derive(Debug, Clone, FromRow, Serialize)]
struct PriceWindow {
    starts_at: DateTime<FixedOffset>,
    ends_at: DateTime<FixedOffset>,
    average_price: String,
}

impl PriceWindow {
    fn with_timezone<Tz: TimeZone>(&self, timezone: Tz) -> PriceWindow {
        PriceWindow {
            starts_at: self.starts_at.with_timezone(&timezone).fixed_offset(),
            ends_at: self.ends_at.with_timezone(&timezone).fixed_offset(),
            average_price: self.average_price.clone(),
        }
    }
}

#[async_trait]
trait PriceRepository: Send + Sync {
    async fn fetch_prices_of_date(&self, date: NaiveDate) -> Result<Vec<PricePoint>, String>;

    async fn persist_prices(&self, prices: &[PricePoint], provider_name: &str) -> Result<(), PriceRepositoryError>;

    async fn fetch_optimal_price_window_of_window_for_durations(&self, start_moment: DateTime<Utc>, end_moment: DateTime<Utc>, durations: &[i32]) -> Result<Vec<PriceWindow>, String>;
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

    async fn persist_prices(&self, prices: &[PricePoint], provider_name: &str) -> Result<(), PriceRepositoryError> {
        info!("Persisting {} prices", prices.len());

        let provider: Provider = sqlx::query_as("select id, name from providers where name = $1 limit 1")
            .bind(provider_name)
            .fetch_one(&self.db)
            .await
            .map_err(|e| PriceRepositoryError::PersistenceError(e.to_string()))?;


        let mut query_builder = QueryBuilder::new("insert into prices (moment, price, provider_id)");

        query_builder.push_values(prices, |mut builder, price| {
            builder
                .push_bind(price.moment)
                .push_bind(price.monetary_amount)
                .push_bind(provider.id)
            ;
        });

        let query = query_builder.build();

        query
            .execute(&self.db)
            .await
            .map(|_| ())
            .map_err(|e| PriceRepositoryError::PersistenceError(e.to_string()))
    }

    #[instrument(skip(self))]
    async fn fetch_optimal_price_window_of_window_for_durations(&self, start_moment: DateTime<Utc>, end_moment: DateTime<Utc>, durations: &[i32]) -> Result<Vec<PriceWindow>, String> {
        let mut windows: Vec<PriceWindow> = Vec::new();

        for duration in durations.into_iter() {
            let mut duration: i32 = *duration;

            duration = duration - 1;
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
}

#[derive(FromRow)]
struct Provider {
    id: i64,
    name: String,
}