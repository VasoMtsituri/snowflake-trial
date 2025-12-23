SELECT
    TRADE_DATE,
    LAST_REFRESHED,
    CLOSE_PRICE,
    VOLUME,
    COMPANY
FROM {{ ref("alpha_vantage_daily") }}
WHERE COMPANY = 'IBM'