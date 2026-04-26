import pandas as pd
import numpy as np
import snowflake.connector
from datetime import datetime

# ── Snowflake connection ───────────────────────────────────────────────────────
conn = snowflake.connector.connect(
    user='NIRBHIJAIN03',
    password=input('Snowflake password: '),
    account='os89070.ap-southeast-1',
    warehouse='COMPUTE_WH',
    database='RECON_DB',
    schema='STAGING_MARTS'
)

# ── Pull daily summary from Snowflake ─────────────────────────────────────────
query = """
    SELECT
        DATE(created_at)        AS settlement_date,
        recon_status,
        COUNT(*)                AS transaction_count,
        SUM(stripe_amount)      AS total_amount
    FROM STAGING_MARTS.FCT_RECONCILIATION
    GROUP BY DATE(created_at), recon_status
    ORDER BY settlement_date
"""

df = pd.read_sql(query, conn)
conn.close()

# ── Focus on matched transactions daily total ──────────────────────────────────
daily = (
    df[df['RECON_STATUS'] == 'MATCHED']
    .groupby('SETTLEMENT_DATE')['TOTAL_AMOUNT']
    .sum()
    .reset_index()
    .rename(columns={'TOTAL_AMOUNT': 'net_settlement'})
    .sort_values('SETTLEMENT_DATE')
)

# ── Z-score anomaly detection ──────────────────────────────────────────────────
WINDOW    = 7
THRESHOLD = 2.0

daily['rolling_mean'] = daily['net_settlement'].rolling(WINDOW).mean()
daily['rolling_std']  = daily['net_settlement'].rolling(WINDOW).std()
daily['z_score']      = (
    (daily['net_settlement'] - daily['rolling_mean']) / daily['rolling_std']
)

# ── Flag anomalies ─────────────────────────────────────────────────────────────
anomalies = daily[daily['z_score'].abs() > THRESHOLD].copy()
anomalies['flag'] = anomalies['z_score'].apply(
    lambda z: 'HIGH_VOLUME' if z > 0 else 'LOW_VOLUME'
)

# ── Output ─────────────────────────────────────────────────────────────────────
print(f"\n{'='*55}")
print(f"  ANOMALY DETECTION REPORT — {datetime.today().strftime('%Y-%m-%d')}")
print(f"{'='*55}")
print(f"  Days analysed:    {len(daily)}")
print(f"  Anomalies found:  {len(anomalies)}")
print(f"{'='*55}\n")

if len(anomalies) > 0:
    for _, row in anomalies.iterrows():
        print(f"  DATE:    {row['SETTLEMENT_DATE']}")
        print(f"  AMOUNT:  ${row['net_settlement']:,.2f}")
        print(f"  Z-SCORE: {row['z_score']:.2f}  [{row['flag']}]")
        print(f"  {'─'*40}")
else:
    print("  No anomalies detected.")

# ── Save report ───────────────────────────────────────────────────────────────
report = daily.copy()
report['anomaly'] = report['z_score'].abs() > THRESHOLD
report.to_csv('scripts/break_report.csv', index=False)
print(f"\n  Break report saved to scripts/break_report.csv")