use axum::async_trait;
use chrono::{DateTime, Local, Utc};
use log::info;
use reqwest::Client;
use serde_derive::{Deserialize, Serialize};
use tracing::instrument;

use crate::{ElectricityProvider, ElectricityProviderError, PricePoint};

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
impl ElectricityProvider for Tibber {
    async fn fetch_prices(&self) -> Result<Vec<PricePoint>, ElectricityProviderError> {
        get_prices(&self.api_key)
            .await
            .map_err(|e| ElectricityProviderError::FetchPrices(e.to_string()))
            .and_then(|prices| prices.into_iter().map(PricePoint::try_from).collect::<Result<Vec<PricePoint>, ElectricityProviderError>>())
    }
}

async fn get_prices(api_key: &str) -> reqwest::Result<Vec<TibberPricePoint>> {
    info!("Fetching prices from tibber");

    let query = r#"{ "query": "{ viewer { homes { currentSubscription { priceInfo { today { total startsAt } }}}}}" }"#;

    let client = Client::new();

    let response = client
        .post("https://api.tibber.com/v1-beta/gql")
        .header("Authorization", api_key)
        .header("Content-Type", "application/json")
        .body(query)
        .send()
        .await?;

    let body = response.text().await?;

    let prices = parse_prices_json(&body);

    info!("Fetched {} prices from tibber", prices.len());

    Ok(prices)
}

fn parse_prices_json(json: &str) -> Vec<TibberPricePoint> {
    let data = serde_json::from_str::<Response>(json).expect("Failed to parse tibber's response");

    return data.data.viewer.homes[0].current_subscription.price_info.today.clone();
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
    today: Vec<TibberPricePoint>,
}

#[derive(Serialize, Deserialize, Debug, Clone)]
struct TibberPricePoint {
    total: f64,
    #[serde(rename = "startsAt")]
    starts_at: String,
}


impl TryFrom<TibberPricePoint> for PricePoint {
    type Error = ElectricityProviderError;

    fn try_from(value: TibberPricePoint) -> Result<Self, Self::Error> {
        DateTime::parse_from_rfc3339(value.starts_at.as_str())
            .map_err(|e| ElectricityProviderError::FetchPrices(e.to_string()))
            .map(|dt| dt.with_timezone(&Utc))
            .map(|dt| PricePoint {
                monetary_amount: value.total,
                moment: dt,
            })
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
        assert_eq!(prices[0].starts_at, "2024-06-15T00:00:00.000+02:00");

        assert_eq!(prices[23].total, 0.2021);
        assert_eq!(prices[23].starts_at, "2024-06-15T23:00:00.000+02:00");
    }
}