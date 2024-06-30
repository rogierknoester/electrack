use chrono::NaiveDateTime;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
pub struct NordpoolPrice {
    pub price: f64,
    pub moment: NaiveDateTime,
}


