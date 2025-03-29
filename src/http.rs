use axum::{
    extract::{Query, State},
    routing::get,
    serve, Json, Router,
};
use axum_macros::debug_handler;

use chrono::{DateTime, FixedOffset};
use reqwest::StatusCode;
use serde::Deserialize;
use tokio::net::TcpListener;
use tracing::{error, info, instrument};

use crate::domain::PriceWindow;
use crate::setup::{setup_app_state, AppState};

/// The main entry point for the http app.
/// It creates the state that is passed to endpoints
pub(crate) async fn start_http_server() -> Result<(), std::io::Error> {
    let state = setup_app_state().await;

    let _monitor_handle = state
        .electricity_provider
        .monitor_prices(state.price_repository.clone())
        .await;

    let router = Router::new()
        .route("/time-slots", get(get_time_slots))
        .route("/upcoming", get(get_upcoming_windows))
        .with_state(state);

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
