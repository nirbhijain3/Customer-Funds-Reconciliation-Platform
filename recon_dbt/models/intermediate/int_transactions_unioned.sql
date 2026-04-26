WITH stripe AS (
    SELECT
        transaction_id,
        amount,
        currency,
        customer_id,
        status,
        created_at,
        'stripe' AS source
    FROM {{ ref('stg_stripe') }}
),

adyen AS (
    SELECT
        transaction_id,
        amount,
        currency,
        customer_id,
        status,
        created_at,
        'adyen' AS source
    FROM {{ ref('stg_adyen') }}
),

unioned AS (
    SELECT * FROM stripe
    UNION ALL
    SELECT * FROM adyen
)

SELECT * FROM unioned