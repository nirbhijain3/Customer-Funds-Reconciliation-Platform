WITH source AS (
    SELECT * FROM {{ source('raw', 'raw_ledger') }}
),

renamed AS (
    SELECT
        transaction_id,
        ledger_entry_id,
        amount,
        currency,
        customer_id,
        status,
        gl_account,
        created_at,
        posted_at
    FROM source
)

SELECT * FROM renamed