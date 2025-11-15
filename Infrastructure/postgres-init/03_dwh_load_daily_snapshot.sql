CREATE OR REPLACE FUNCTION dwh.load_daily_snapshot()
RETURNS void
LANGUAGE plpgsql
AS $$
BEGIN
    ----------------------------------------------------------------
    -- 1) Crypto dim upsert
    ----------------------------------------------------------------
    INSERT INTO dwh.asset_dim (asset_type, symbol, name)
    SELECT 'crypto' AS asset_type,
           c.symbol,
           c.name
    FROM stage.crypto_tickers c
    GROUP BY c.symbol, c.name
    ON CONFLICT (asset_type, symbol)
    DO UPDATE SET name = EXCLUDED.name;

    ----------------------------------------------------------------
    -- 2) Stock dim upsert
    ----------------------------------------------------------------
    INSERT INTO dwh.asset_dim (asset_type, symbol, name)
    SELECT 'stock' AS asset_type,
           s.symbol,
           s.name
    FROM stage.stock_ohlcv s
    GROUP BY s.symbol, s.name
    ON CONFLICT (asset_type, symbol)
    DO UPDATE SET name = EXCLUDED.name;

    ----------------------------------------------------------------
    -- 3) Crypto fact (aktuális nap)
    ----------------------------------------------------------------
    INSERT INTO dwh.daily_price_fact (
        asset_id,
        price_date,
        close_price,
        volume,
        volume_usd,
        return_1d
    )
    SELECT ad.asset_id,
           (c.ingestion_time AT TIME ZONE 'UTC')::date       AS price_date,
           c.price_usd                                       AS close_price,
           CASE
               WHEN c.price_usd IS NULL OR c.price_usd = 0
                   THEN NULL
               ELSE c.volume_24h_usd / c.price_usd
           END                                               AS volume,
           c.volume_24h_usd                                  AS volume_usd,
           c.percent_change_24h / 100.0                      AS return_1d
    FROM stage.crypto_tickers c
    JOIN dwh.asset_dim ad
      ON ad.asset_type = 'crypto'
     AND ad.symbol     = c.symbol
    WHERE (c.ingestion_time AT TIME ZONE 'UTC')::date = (now() AT TIME ZONE 'UTC')::date
    ON CONFLICT (asset_id, price_date)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        volume_usd  = EXCLUDED.volume_usd,
        return_1d   = EXCLUDED.return_1d;

    ----------------------------------------------------------------
    -- 4) Stock fact (aktuális nap)
    ----------------------------------------------------------------
    INSERT INTO dwh.daily_price_fact (
        asset_id,
        price_date,
        close_price,
        volume,
        volume_usd,
        return_1d
    )
    SELECT ad.asset_id,
           s.datetime::date      AS price_date,
           s.close_price         AS close_price,
           s.volume::numeric     AS volume,
           NULL::numeric         AS volume_usd,
           NULL::numeric         AS return_1d
    FROM stage.stock_ohlcv s
    JOIN dwh.asset_dim ad
      ON ad.asset_type = 'stock'
     AND ad.symbol     = s.symbol
    WHERE s.datetime::date = (now() AT TIME ZONE 'UTC')::date
    ON CONFLICT (asset_id, price_date)
    DO UPDATE SET
        close_price = EXCLUDED.close_price,
        volume      = EXCLUDED.volume,
        volume_usd  = EXCLUDED.volume_usd,
        return_1d   = EXCLUDED.return_1d;

END;
$$;
