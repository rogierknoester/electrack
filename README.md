## Goal

The goal of electrack for me is to have a historic view on electricity prices, and what the optimal windows are during the day to use electricity. I use it to decide when to run my dishwasher and laundry.




## Getting started
The only way right now to run electrack is by building from source. 

### Configuration
Some configuration is required:

- Postgres instance with TimescaleDB
- access to Tibber's API

Configure them with
```env
ELECTRICITY_PRICE_PROVIDER_DSN=tibber://{api_key}
DATABASE_URL=postgres://username:password@hostname/db_name
```

Database migrations will be executed on startup.

#### Tibber API
Tibber has an API that any customer can request access to. You can find that [here](https://developer.tibber.com/). Your API key can be seen [here](https://developer.tibber.com/settings/access-token).



### Endpoints

#### Time-slots
The time-slots endpoint provides the cheapest windows for provided durations between a start and ending moment. Don't forget to url encode the parameters.

For example, to get a 2 and 3 hour window on June 30th 2024.  
```http
GET /time-slots?durations=2,3&moment_start=2024-06-30t09%3A52%3A07%2B02%3A00&moment_end=2024-06-30t23%3A52%3A07%2B02%3A00
```


