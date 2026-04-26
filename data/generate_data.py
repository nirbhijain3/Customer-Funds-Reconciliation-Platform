import pandas as pd
import numpy as np
import random
import uuid
from datetime import datetime, timedelta

random.seed(42)
np.random.seed(42)

NUM_TRANSACTIONS = 10000
START_DATE = datetime(2024, 1, 1)

def random_date():
    return START_DATE + timedelta(days=random.randint(0, 364),
                                   hours=random.randint(0, 23),
                                   minutes=random.randint(0, 59))

def generate_base_transactions(n):
    records = []
    for _ in range(n):
        records.append({
            "transaction_id": "TXN-" + str(uuid.uuid4())[:8].upper(),
            "amount":         round(random.uniform(10, 5000), 2),
            "currency":       random.choice(["USD", "USD", "USD", "EUR", "GBP"]),
            "customer_id":    "CUST-" + str(random.randint(1000, 9999)),
            "created_at":     random_date(),
            "status":         random.choice(["settled", "settled", "settled", "refunded", "failed"]),
        })
    return pd.DataFrame(records)

base = generate_base_transactions(NUM_TRANSACTIONS)

# ── STRIPE ────────────────────────────────────────────────────────────────────
stripe = base.copy()
stripe["stripe_transaction_id"] = ["STR-" + i for i in stripe["transaction_id"]]
stripe["fee"]                    = (stripe["amount"] * 0.029 + 0.30).round(2)
stripe["net_amount"]             = (stripe["amount"] - stripe["fee"]).round(2)
stripe["settled_at"]             = stripe["created_at"] + timedelta(days=2)
stripe = stripe.rename(columns={"created_at": "created_at"})

# inject ~3% amount mismatches
mismatch_idx = stripe.sample(frac=0.03).index
stripe.loc[mismatch_idx, "amount"] = (stripe.loc[mismatch_idx, "amount"] * 1.01).round(2)

stripe.to_csv("data/stripe_transactions.csv", index=False)
print(f"Stripe:  {len(stripe):,} rows")

# ── ADYEN ─────────────────────────────────────────────────────────────────────
adyen = base.sample(frac=0.4, random_state=1).copy()   # Adyen handles 40% of volume
adyen["adyen_psp_reference"] = ["ADYEN-" + str(random.randint(100000, 999999))
                                 for _ in range(len(adyen))]
adyen["merchant_account"]    = random.choice(["MerchantAccountUSD", "MerchantAccountEUR"])
adyen["value_in_cents"]      = (adyen["amount"] * 100).astype(int)
adyen["booking_date"]        = adyen["created_at"] + timedelta(days=1)

adyen.to_csv("data/adyen_transactions.csv", index=False)
print(f"Adyen:   {len(adyen):,} rows")

# ── INTERNAL LEDGER ───────────────────────────────────────────────────────────
ledger = base.copy()

# drop ~4% — these become MISSING_IN_LEDGER breaks
drop_idx = ledger.sample(frac=0.04, random_state=2).index
ledger   = ledger.drop(drop_idx)

# inject ~2% duplicates
dup = ledger.sample(frac=0.02, random_state=3)
ledger = pd.concat([ledger, dup], ignore_index=True)

ledger["gl_account"]      = random.choice(["2010-CUST-FUNDS", "2011-CUST-FUNDS-EUR"])
ledger["posted_at"]       = ledger["created_at"] + timedelta(days=1)
ledger["ledger_entry_id"] = ["GL-" + str(uuid.uuid4())[:8].upper() for _ in range(len(ledger))]

ledger.to_csv("data/ledger_entries.csv", index=False)
print(f"Ledger:  {len(ledger):,} rows")

print("\nDone. Files saved to data/")
print(f"Intentional breaks injected:")
print(f"  ~{len(mismatch_idx)} amount mismatches in Stripe vs Ledger")
print(f"  ~{len(drop_idx)} transactions missing from Ledger")
print(f"  ~{len(dup)} duplicate ledger entries")