SELECT
    RAW_METADATA:"2. Symbol"::VARCHAR AS COMPANY,
    RAW_METADATA:"3. Last Refreshed"::DATE AS LAST_REFRESHED,
    f.key::DATE as TRADE_DATE,
    f.value:"1. open"::NUMBER(19,4) as OPEN_PRICE,
    f.value:"2. high"::NUMBER(19,4) as HIGH_PRICE,
    f.value:"3. low"::NUMBER(19,4) as LOW_PRICE,
    f.value:"4. close"::NUMBER(19,4) as CLOSE_PRICE,
    f.value:"5. volume"::NUMBER(38,0) as VOLUME
FROM {{ ref("alpha_vantage_stg_events") }},
LATERAL FLATTEN(input => RAW_SERIES) f