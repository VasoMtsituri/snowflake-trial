{{
  config(
    materialized='incremental',
    unique_key='unique_id',
    incremental_strategy='merge',
    cluster_by=['TRADE_DATE']
  )
}}

WITH source_data AS (
    SELECT 
        -- Create a unique key to prevent duplicates (Ticker + Date)
        md5(concat(COMPANY, TRADE_DATE)) as unique_id,
        TRADE_DATE,
        LAST_REFRESHED,
        CLOSE_PRICE,
        VOLUME,
        'NVIDIA' AS COMPANY
    FROM {{ ref("alpha_vantage_daily") }}
    WHERE COMPANY = 'NVDA'
),

deduplicated AS (
    SELECT *
    FROM source_data
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY UNIQUE_ID
        ORDER BY LAST_REFRESHED DESC
    ) = 1
)

SELECT * FROM deduplicated

{% if is_incremental() %}
  -- This filter ensures we only scan the incoming 100 days from staging
  -- while comparing it against what is already in our Gold table
  WHERE TRADE_DATE >= (SELECT MAX(TRADE_DATE) FROM {{ this }})
     OR LAST_REFRESHED > (SELECT MAX(LAST_REFRESHED) FROM {{ this }})
{% endif %}