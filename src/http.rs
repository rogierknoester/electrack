use axum::{
    extract::{Query, State},
    routing::get,
    serve, Json, Router,
};
use axum_macros::debug_handler;

use chrono::{DateTime, FixedOffset, Local, NaiveDate};
use reqwest::StatusCode;
use serde::Deserialize;
use sqlx::PgPool;
use tokio::net::TcpListener;
use tracing::{error, info, instrument};

use crate::{
    domain::{ElectricityPriceProvider, PriceWindow},
    price_repository::PriceRepository,
};
use crate::{
    domain::{ElectricityProviderError, PricePoint},
    setup::{setup_app_state, AppState},
};

/// The main entry point for the http app.
/// It creates the state that is passed to endpoints
pub(crate) async fn start_http_server() -> Result<(), std::io::Error> {
    let router = Router::new()
        .route("/time-slots", get(get_time_slots))
        .route("/upcoming", get(get_upcoming_windows))
        .with_state(setup_app_state().await);

    let port = std::env::var("PORT").unwrap_or("8080".to_string());
    let listener = TcpListener::bind(format!("0.0.0.0:{}", port))
        .await
        .unwrap();

    info!("now listening on port {}", port);

    serve(listener, router).await
}

#[derive(Debug, Clone, Deserialize)]
struct TimeslotParameters {
    durations: Durations,
    moment_start: DateTime<FixedOffset>,
    moment_end: DateTime<FixedOffset>,
}

/// Fetch the timeslots between a start and end moment that are the cheapest for the given
/// durations. Every duration results in a `PriceWindow`
#[debug_handler(state = AppState)]
#[instrument(skip(state))]
async fn get_time_slots(
    State(state): State<AppState>,
    parameters: Query<TimeslotParameters>,
) -> axum::response::Result<(StatusCode, Json<Vec<PriceWindow>>)> {
    if !has_prices_of_date(state.db.clone(), Local::now().date_naive())
        .await
        .unwrap()
    {
        let price_fetching_result = fetch_prices_of_today_from_provider(
            &*state.electricity_provider,
            &*state.price_repository,
        )
        .await;

        if let Err(e) = price_fetching_result {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into());
        }
    }

    let durations = parameters.durations.parse();

    let timezone_date_start = parameters.moment_start.timezone();

    let optimal_windows: Vec<PriceWindow> = state
        .price_repository
        .fetch_optimal_price_window_of_window_for_durations(
            parameters.moment_start.to_utc(),
            parameters.moment_end.to_utc(),
            durations.as_slice(),
        )
        .await
        .map(|windows| {
            windows
                .into_iter()
                .map(|window| window.with_timezone(timezone_date_start))
                .collect::<Vec<PriceWindow>>()
        })
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;

    Ok((StatusCode::OK, Json(optimal_windows)))
}

async fn has_prices_of_date(db: PgPool, date: NaiveDate) -> Result<bool, String> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM prices WHERE moment::date = $1")
        .bind(date)
        .fetch_one(&db)
        .await
        .map_err(|e| e.to_string())?;

    Ok(row.0 > 0)
}

/// Fetch the prices of the provider for the current day
/// @todo move into provider itself
async fn fetch_prices_of_today_from_provider(
    electricity_provider: &dyn ElectricityPriceProvider,
    price_repository: &dyn PriceRepository,
) -> Result<Vec<PricePoint>, ElectricityProviderError> {
    info!("prices for today not yet fetched");
    let fetch_result = electricity_provider.fetch_prices().await;

    let persisting_result = match fetch_result {
        Ok(fetched_prices) => {
            info!("Fetched {} prices", fetched_prices.len());
            price_repository
                .persist_prices(&fetched_prices, electricity_provider.name())
                .await
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

/// The query parameters for the `/upcoming` endpoint.
/// Contains only the requested durations
#[derive(Debug, Clone, Deserialize)]
struct UpcomingParameters {
    durations: Durations,
}

#[derive(Debug, Clone, Deserialize)]
struct Durations(Option<String>);

impl Durations {
    fn parse(&self) -> Vec<i32> {
        self.0
            .clone()
            .unwrap_or("1".to_owned())
            .split(',')
            .filter_map(|s| s.parse::<i32>().ok())
            .collect::<Vec<i32>>()
    }
}

/// Fetch (multiple) upcoming windows. Passing window durations is optional, if none is given a
/// window of 1 hour is assumed.
/// The current time will be used for fetching
async fn get_upcoming_windows(
    State(state): State<AppState>,
    parameters: Query<UpcomingParameters>,
) -> axum::response::Result<(StatusCode, Json<Vec<PriceWindow>>)> {
    if !has_prices_of_date(state.db.clone(), Local::now().date_naive())
        .await
        .unwrap()
    {
        let price_fetching_result = fetch_prices_of_today_from_provider(
            &*state.electricity_provider,
            &*state.price_repository,
        )
        .await;

        if let Err(e) = price_fetching_result {
            return Err((StatusCode::INTERNAL_SERVER_ERROR, e.to_string()).into());
        }
    }

    let durations = parameters.durations.parse();

    let mut windows: Vec<PriceWindow> = vec![];

    for duration in durations.iter() {
        let found_window = state
            .price_repository
            .fetch_optimal_upcoming_window(duration.to_owned())
            .await;

        match found_window {
            Ok(window) => windows.push(window),
            Err(error) => error!("cannot find window for duration of {}. {}", duration, error),
        };
    }

    Ok((StatusCode::OK, Json(windows)))
}
