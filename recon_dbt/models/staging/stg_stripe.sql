WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_stripe') }}
),

renamed AS (
    SELECT
        transaction_id,
        stripe_transaction_id,
        amount,
        currency,
        customer_id,
        status,
        fee,
        net_amount,
        created_at,
        settled_at
    FROM source
)

SELECT * FROM renamed