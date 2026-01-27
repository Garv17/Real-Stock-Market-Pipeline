{{ config(materialized='view') }}

SELECT
    RAW:"c"::FLOAT        AS current_price,
    RAW:"d"::FLOAT        AS change_amount,
    RAW:"dp"::FLOAT       AS change_percent,
    RAW:"h"::FLOAT        AS day_high,
    RAW:"l"::FLOAT        AS day_low,
    RAW:"o"::FLOAT        AS day_open,
    RAW:"pc"::FLOAT       AS prev_close,
    RAW:"t"::TIMESTAMP    AS market_timestamp,
    RAW:"symbol"::STRING AS symbol,
    RAW:"fetched_at"::TIMESTAMP AS fetched_at
FROM {{ source('raw', 'bronze_stock_quotes_raw') }}
