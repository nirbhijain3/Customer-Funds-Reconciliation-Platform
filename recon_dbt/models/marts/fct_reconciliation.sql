WITH stripe AS (
    SELECT * FROM {{ ref('stg_stripe') }}
),

ledger AS (
    SELECT * FROM {{ ref('stg_ledger') }}
),

reconciled AS (
    SELECT
        s.transaction_id,
        s.amount                        AS stripe_amount,
        l.amount                        AS ledger_amount,
        s.currency,
        s.customer_id,
        s.status                        AS stripe_status,
        l.status                        AS ledger_status,
        s.created_at,
        s.settled_at,

        CASE
            WHEN l.transaction_id IS NULL
                THEN 'MISSING_IN_LEDGER'
            WHEN ABS(s.amount - l.amount) > 0.01
                THEN 'AMOUNT_MISMATCH'
            WHEN l.ledger_entry_id IN (
                SELECT ledger_entry_id
                FROM {{ ref('stg_ledger') }}
                GROUP BY ledger_entry_id
                HAVING COUNT(*) > 1
            )
                THEN 'DUPLICATE'
            ELSE 'MATCHED'
        END                             AS recon_status,

        ABS(COALESCE(s.amount, 0) - COALESCE(l.amount, 0)) AS amount_variance

    FROM stripe s
    LEFT JOIN ledger l ON s.transaction_id = l.transaction_id
)

SELECT * FROM reconciled