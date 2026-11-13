"""Generate 3 fresh-domain demo datasets with seeded defects for DQ Accelerator.

Domains (no overlap with existing sample data):
  demo_ecommerce_orders.csv  ~300K rows — orders domain
  demo_telco_events.csv      ~1M rows  — telecom CDR-like events
  demo_insurance_claims.csv  ~20K rows  — insurance claims

Each plants ~1-3% defects: nulls, bad formats, duplicates, out-of-range,
invalid categories, PII-ish values — so profiling/rules visibly catch them.
Deterministic seed -> reproducible. Run:  python scripts/gen_demo_datasets.py
"""
import csv
import random
import sys
from pathlib import Path

random.seed(42)
OUT = Path(__file__).resolve().parent.parent / "data" / "uploads"
OUT.mkdir(parents=True, exist_ok=True)

FIRST = ["James", "Mary", "John", "Patricia", "Robert", "Jennifer", "Michael", "Linda",
         "William", "Elizabeth", "David", "Barbara", "Richard", "Susan", "Joseph", "Jessica"]
LAST = ["Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
        "Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas"]
CITIES = ["New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia",
          "San Antonio", "San Diego", "Dallas", "San Jose"]


def wcsv(name, header, rows):
    p = OUT / name
    n = 0
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(header)
        for row in rows:
            w.writerow(row)
            n += 1
    print(f"{name}: {n:,} rows -> {p} ({p.stat().st_size/1e6:.1f} MB)")


def gen_ecommerce(n=300_000):
    """Orders: null emails, bad dates, negative amounts, dup ids, bad status, PII emails."""
    hdr = ["order_id", "customer_email", "order_date", "amount_usd", "status", "category", "qty"]
    cats = ["electronics", "apparel", "grocery", "books", "home", "toys"]
    stats = ["PENDING", "PAID", "SHIPPED", "DELIVERED", "CANCELLED", "REFUNDED"]
    bad_stats = ["paid", "Shipped ", "UNKNOWN", "", "delivered!"]
    for i in range(1, n + 1):
        oid = f"EO-{i:07d}"
        r = random.random()
        email = (f"customer{i % 50000}@example.com" if r > 0.03 else
                 ("" if r < 0.012 else f"INVALID@@{i}" if r < 0.02 else f"user{i}@test"))
        dt = (f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if r > 0.025 else
              (f"{random.randint(1,12)}/{random.randint(1,28)}/{random.randint(2024,2026)}" if r < 0.02 else "not-a-date"))
        amt = round(random.uniform(5, 2000), 2) if r > 0.015 else round(random.uniform(-500, -1), 2)
        st = random.choice(stats) if r > 0.04 else random.choice(bad_stats)
        cat = random.choice(cats) if r > 0.015 else "misc_unknown"
        qty = random.randint(1, 10) if r > 0.01 else random.randint(-5, 0)
        yield [oid, email, dt, amt, st, cat, qty]


def gen_telco(n=1_000_000):
    """CDR-like events: null msisdn, bad ts, negative duration, dup ids, huge bytes, PII-ish."""
    hdr = ["event_id", "msisdn", "event_ts", "duration_sec", "bytes", "cell_id", "event_type"]
    ets = ["VOICE", "DATA", "SMS", "MMS"]
    for i in range(1, n + 1):
        eid = f"TE-{i:08d}"
        r = random.random()
        msisdn = (f"{random.randint(2000000000, 9999999999)}" if r > 0.025 else
                  ("" if r < 0.015 else f"BAD-{i}"))
        ts = (f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}:00Z" if r > 0.02 else
              (f"{random.randint(1,12)}/{random.randint(1,28)}/2025 {random.randint(0,23)}h" if r < 0.015 else "timestamp-x"))
        dur = random.randint(0, 3600) if r > 0.015 else random.randint(-100, -1)
        byt = random.randint(0, 5_000_000) if r > 0.01 else random.randint(10**9, 10**10)
        cell = f"CELL-{random.randint(1000,9999)}" if r > 0.01 else f"cell{i}"
        et = random.choice(ets) if r > 0.015 else "ROAMING"
        yield [eid, msisdn, ts, dur, byt, cell, et]


def gen_insurance(n=20_000):
    """Claims: null claimant, bad dates, amount outliers, dup claim ids, PII names."""
    hdr = ["claim_id", "policy_no", "claimant_name", "claim_date", "amount_claimed", "status", "adjuster"]
    stats = ["OPEN", "APPROVED", "DENIED", "PENDING_REVIEW", "SETTLED"]
    for i in range(1, n + 1):
        cid = f"CLM-{i:05d}" if random.random() > 0.008 else f"CLM-{i-1:05d}"  # dup ids
        r = random.random()
        name = f"{random.choice(FIRST)} {random.choice(LAST)}" if r > 0.02 else (
            "" if r < 0.012 else f"{random.choice(FIRST)}0{random.choice(LAST)}")
        pol = f"POL-{random.randint(100000, 999999)}" if r > 0.015 else f"policy{i}"
        dt = (f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if r > 0.02 else
              (f"{random.randint(1,12)}-{random.randint(1,28)}-2025" if r < 0.015 else "date?"))
        amt = round(random.uniform(500, 250000), 2) if r > 0.02 else round(random.uniform(10**6, 10**7), 2)
        st = random.choice(stats) if r > 0.03 else random.choice(["open", "APPROVED ", "X", ""])
        adj = f"{random.choice(FIRST)} {random.choice(LAST)}" if r > 0.05 else ""
        yield [cid, pol, name, dt, amt, st, adj]


if __name__ == "__main__":
    wcsv("demo_ecommerce_orders.csv", *(["order_id","customer_email","order_date","amount_usd","status","category","qty"], gen_ecommerce()))
    wcsv("demo_telco_events.csv", *(["event_id","msisdn","event_ts","duration_sec","bytes","cell_id","event_type"], gen_telco()))
    wcsv("demo_insurance_claims.csv", *(["claim_id","policy_no","claimant_name","claim_date","amount_claimed","status","adjuster"], gen_insurance()))
    print("Done — onboard each via UI: Onboard -> upload -> profile -> review -> run")
