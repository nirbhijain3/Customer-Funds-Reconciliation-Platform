WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_adyen') }}
),

renamed AS (
    SELECT
        transaction_id,
        adyen_psp_reference,
        amount,
        currency,
        customer_id,
        status,
        value_in_cents,
        merchant_account,
        created_at,
        booking_date  AS settled_at
    FROM source
)

SELECT * FROM renamed