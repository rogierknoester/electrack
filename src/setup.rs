use core::panic;
use log::debug;
use sqlx::migrate::Migrator;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;
use std::process;
use std::sync::Arc;
use tracing::error;

use crate::{
    domain::ElectricityPriceProvider, price_repository::PostgresPriceRepository, tibber,
    PriceRepository,
};

static MIGRATOR: Migrator = sqlx::migrate!();

/// Setup the app state that is given to every route handler
/// Contains things such as the DB connection pool, ElectricityProvider instance
/// and a price repository
pub(crate) async fn setup_app_state() -> AppState {
    let electricity_provider_dsn = std::env::var("ELECTRICITY_PRICE_PROVIDER_DSN")
        .expect("ELECTRICITY_PRICE_PROVIDER_DSN is missing, you need to configure it");

    let db_dsn = std::env::var("DATABASE_URL").expect("DATABASE_URL must be set");

    let db_pool = setup_db(&db_dsn).await;

    let price_repository = PostgresPriceRepository::new(db_pool.clone());

    let electricity_provider = resolve_electricity_provider(electricity_provider_dsn.as_str());

    AppState::new(
        db_pool,
        Arc::new(electricity_provider),
        Arc::new(price_repository),
    )
}

/// Build an `ElectricityProvider` instance from the provided instance
/// Requires that a `ELECTRICITY_PRICE_PROVIDER_DSN` is present in the environment
/// Currently only a tibber implementation exists
fn resolve_electricity_provider(dsn: &str) -> impl ElectricityPriceProvider {
    let dsn = dsn::parse(dsn).unwrap_or_else(|e| {
        error!("unable to parse ELECTRICITY_PRICE_PROVIDER_DSN, {}", e);
        process::exit(1);
    });

    debug!("trying to resolve provider \"{}\"", dsn.driver);
    return match dsn.driver.as_str() {
        "tibber" => tibber::Tibber::new(
            dsn.username
                .expect("cannot create a tibber instance from the provided dsn"),
        ),
        _ => panic!(
            "the provided ELECTRICITY_PRICE_PROVIDER_DSN does not match any supported provider"
        ),
    };
}

async fn setup_db(db_dsn: &str) -> sqlx::PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(db_dsn)
        .await
        .expect("failed to create database pool");

    MIGRATOR.run(&pool).await.expect("failed to run migrations");

    pool
}

#[derive(Clone)]
pub(crate) struct AppState {
    pub(crate) db: PgPool,
    pub(crate) electricity_provider: Arc<dyn ElectricityPriceProvider>,
    pub(crate) price_repository: Arc<dyn PriceRepository>,
}

impl AppState {
    fn new(
        db: PgPool,
        electricity_provider: Arc<dyn ElectricityPriceProvider>,
        price_repository: Arc<dyn PriceRepository>,
    ) -> Self {
        Self {
            db,
            electricity_provider,
            price_repository,
        }
    }
}
