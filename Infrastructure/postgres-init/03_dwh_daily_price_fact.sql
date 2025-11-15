CREATE SCHEMA IF NOT EXISTS dwh;

CREATE TABLE IF NOT EXISTS dwh.daily_price_fact (
    asset_id        int     NOT NULL REFERENCES dwh.asset_dim(asset_id),
    price_date      date    NOT NULL,
    close_price     numeric,
    volume          numeric,
    volume_usd      numeric,
    return_1d       numeric,
    PRIMARY KEY (asset_id, price_date)
);
