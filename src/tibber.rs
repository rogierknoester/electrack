use std::sync::Arc;
use std::time::Duration;

use axum::async_trait;
use chrono::DateTime;
use chrono::Local;
use chrono::Utc;
use log::info;
use reqwest::Client;
use serde_derive::{Deserialize, Serialize};
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::error;

use crate::domain::ElectricityPriceProvider;
use crate::domain::ElectricityProviderError;
use crate::domain::PricePoint;
use crate::PriceRepository;

const TIBBER_NAME: &str = "tibber";

#[derive(Clone, Debug)]
pub(crate) struct Tibber {
    api_key: String,
}

impl Tibber {
    pub(crate) fn new(api_key: String) -> Self {
        Self { api_key }
    }
}

#[async_trait]
impl ElectricityPriceProvider for Tibber {
    async fn monitor_prices(&self, price_repository: Arc<dyn PriceRepository>) -> JoinHandle<()> {
        monitor_tibber_prices(&self.api_key, price_repository)
    }
}

fn monitor_tibber_prices(
    api_key: &str,
    price_repository: Arc<dyn PriceRepository>,
) -> JoinHandle<()> {
    let api_key = api_key.to_owned();
    let price_repository = price_repository.clone();
    tokio::task::spawn(async move {
        loop {
            let today = Local::now().date_naive();
            let tomorrow = today + chrono::Duration::days(1);

            if !price_repository
                .has_prices_of_date(today)
                .await
                .unwrap_or(false)
            {
                info!("prices for today have not been fetched yet, doing that now");
                match fetch_prices_for_moment(api_key.as_str(), Moment::Today).await {
                    Ok(fetched_prices) => {
                        match price_repository
                            .persist_prices(&fetched_prices, TIBBER_NAME)
                            .await
                        {
                            Ok(_) => info!("fetched and persisted prices for today for tibber"),
                            Err(err) => error!("unable to persist prices: {}", err),
                        }
                    }
                    Err(error) => error!("unable to fetch prices: {}", error),
                };
            }
            if !price_repository
                .has_prices_of_date(tomorrow)
                .await
                .unwrap_or(false)
            {
                info!("prices for tomorrow have not been fetched yet, doing that now");
                match fetch_prices_for_moment(api_key.as_str(), Moment::Tomorrow).await {
                    Ok(fetched_prices) => {
                        match price_repository
                            .persist_prices(&fetched_prices, TIBBER_NAME)
                            .await
                        {
                            Ok(_) => info!("fetched and persisted prices for tomorrow for tibber"),
                            Err(err) => error!("unable to persist prices: {}", err),
                        }
                    }
                    Err(error) => error!("unable to fetch prices: {}", error),
                };
            }

            // Let's check again in an 30 minutes
            sleep(Duration::from_secs(1800)).await;
        }
    })
}

const TODAY: &str = r#"{ "query": "{ viewer { homes { currentSubscription { priceInfo { prices: today { total startsAt }  }}}}}" }"#;
const TOMORROW: &str = r#"{ "query": "{ viewer { homes { currentSubscription { priceInfo { prices: tomorrow { total startsAt } }}}}}" }"#;

enum Moment {
    Today,
    Tomorrow,
}

/// Fetch the prices for either today or tomorrow
async fn fetch_prices_for_moment(
    api_key: &str,
    moment: Moment,
) -> Result<Vec<PricePoint>, ElectricityProviderError> {
    fetch_tibber_prices(api_key, moment)
        .await
        .map_err(|e| ElectricityProviderError::FetchPrices(e.to_string()))
        .map(|prices| {
            prices
                .into_iter()
                .map(PricePoint::from)
                .collect::<Vec<PricePoint>>()
        })
}

/// Actually fetch the prices from Tibber's api and return their data structure
async fn fetch_tibber_prices(
    api_key: &str,
    moment: Moment,
) -> reqwest::Result<Vec<TibberPricePoint>> {
    info!("Fetching prices from tibber");

    let query = match moment {
        Moment::Today => TODAY,
        Moment::Tomorrow => TOMORROW,
    };

    let client = Client::new();

    let response = client
        .post("https://api.tibber.com/v1-beta/gql")
        .header("Authorization", api_key)
        .header("Content-Type", "application/json")
        .body(query.to_owned())
        .send()
        .await?;

    let body = response.text().await?;

    let prices = parse_prices_json(&body);

    info!("Fetched {} prices from tibber", prices.len());

    Ok(prices)
}

fn parse_prices_json(json: &str) -> Vec<TibberPricePoint> {
    let data = serde_json::from_str::<Response>(json).expect("Failed to parse tibber's response");

    data.data.viewer.homes[0]
        .current_subscription
        .price_info
        .prices
        .clone()
}

#[derive(Deserialize, Debug)]
struct Response {
    data: Data,
}

#[derive(Deserialize, Debug)]
struct Data {
    viewer: Viewer,
}

#[derive(Deserialize, Debug)]
struct Viewer {
    homes: Vec<Home>,
}

#[derive(Deserialize, Debug)]
struct Home {
    #[serde(rename = "currentSubscription")]
    current_subscription: CurrentSubscription,
}

#[derive(Deserialize, Debug)]
struct CurrentSubscription {
    #[serde(rename = "priceInfo")]
    price_info: PriceInfo,
}

#[derive(Deserialize, Debug)]
struct PriceInfo {
    prices: Vec<TibberPricePoint>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TibberPricePoint {
    total: f64,
    #[serde(rename = "startsAt")]
    starts_at: DateTime<Utc>,
}

impl From<TibberPricePoint> for PricePoint {
    fn from(value: TibberPricePoint) -> PricePoint {
        PricePoint {
            moment: value.starts_at.with_timezone(&Utc),
            monetary_amount: value.total,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_prices_json() {
        let json = r#"
            {"data":{"viewer":{"homes":[{"currentSubscription":{"priceInfo":{"today":[{"total":0.2821,"startsAt":"2024-06-15T00:00:00.000+02:00"},{"total":0.2787,"startsAt":"2024-06-15T01:00:00.000+02:00"},{"total":0.2666,"startsAt":"2024-06-15T02:00:00.000+02:00"},{"total":0.2581,"startsAt":"2024-06-15T03:00:00.000+02:00"},{"total":0.2213,"startsAt":"2024-06-15T04:00:00.000+02:00"},{"total":0.1769,"startsAt":"2024-06-15T05:00:00.000+02:00"},{"total":0.1547,"startsAt":"2024-06-15T06:00:00.000+02:00"},{"total":0.1529,"startsAt":"2024-06-15T07:00:00.000+02:00"},{"total":0.1528,"startsAt":"2024-06-15T08:00:00.000+02:00"},{"total":0.1528,"startsAt":"2024-06-15T09:00:00.000+02:00"},{"total":0.1406,"startsAt":"2024-06-15T10:00:00.000+02:00"},{"total":0.1177,"startsAt":"2024-06-15T11:00:00.000+02:00"},{"total":0.0985,"startsAt":"2024-06-15T12:00:00.000+02:00"},{"total":0.0736,"startsAt":"2024-06-15T13:00:00.000+02:00"},{"total":0.056,"startsAt":"2024-06-15T14:00:00.000+02:00"},{"total":0.0849,"startsAt":"2024-06-15T15:00:00.000+02:00"},{"total":0.1175,"startsAt":"2024-06-15T16:00:00.000+02:00"},{"total":0.1474,"startsAt":"2024-06-15T17:00:00.000+02:00"},{"total":0.1528,"startsAt":"2024-06-15T18:00:00.000+02:00"},{"total":0.1917,"startsAt":"2024-06-15T19:00:00.000+02:00"},{"total":0.2375,"startsAt":"2024-06-15T20:00:00.000+02:00"},{"total":0.2348,"startsAt":"2024-06-15T21:00:00.000+02:00"},{"total":0.2294,"startsAt":"2024-06-15T22:00:00.000+02:00"},{"total":0.2021,"startsAt":"2024-06-15T23:00:00.000+02:00"}]}}}]}}}
            "#;

        let prices = parse_prices_json(json);

        assert_eq!(prices.len(), 24);
        assert_eq!(prices[0].total, 0.2821);
        assert_eq!(
            prices[0].starts_at,
            DateTime::parse_from_rfc3339("2024-06-14T22:00:00.000+00:00").unwrap()
        );

        assert_eq!(prices[23].total, 0.2021);
        assert_eq!(
            prices[23].starts_at,
            DateTime::parse_from_rfc3339("2024-06-15T21:00:00.000+00:00").unwrap()
        );
    }
}
