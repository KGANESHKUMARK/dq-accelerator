"""Generate 3 more demo datasets (fresh domains) with seeded defects."""
import csv, random
from pathlib import Path
random.seed(99)
OUT = Path(__file__).resolve().parent.parent / "data" / "uploads"

def wcsv(name, header, rows):
    p = OUT / name
    with open(p, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f); w.writerow(header)
        for r in rows: w.writerow(r)
    print(f"{name}: {len(rows):,} rows -> {p.stat().st_size/1e6:.1f} MB")

# ── 1. Airline flights (~80K) ─────────────────────────────────────────
CITIES = ["DEL","BOM","BLR","MAA","HYD","CCU","AMD","GOI","JAI","PNQ"]
AIR = ["6E-201","AI-412","UK-618","SG-055","QP-734","Vistara-221"]
STS = ["SCHEDULED","DEPARTED","ARRIVED","DELAYED","CANCELLED","DIVERTED"]
BAD_STS = ["scheduled","CANCEL","","unknown"]
def flights():
    hdr = ["flight_no","route","depart_utc","arrive_utc","delay_min","passengers","fare_inr","status","aircraft"]
    for i in range(1, 80001):
        r = random.random()
        fn = f"{random.choice(AIR)}" if r > 0.006 else f"FL-{i}"
        orig, dest = random.sample(CITIES, 2)
        dep = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}"
        arr = dep if r < 0.02 else f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}T{random.randint(0,23):02d}:{random.randint(0,59):02d}"
        dly = random.randint(-30, 240) if r > 0.015 else random.randint(-500, -100)
        pax = random.randint(0, 240) if r > 0.01 else random.randint(-50, -1)
        fare = round(random.uniform(2500, 45000), 2) if r > 0.02 else round(random.uniform(10**6, 10**7), 2)
        st = random.choice(STS) if r > 0.03 else random.choice(BAD_STS)
        ac = random.choice(["A320","B737","A321","ATR-72","B787"]) if r > 0.04 else ""
        yield [fn, f"{orig}-{dest}", dep, arr, dly, pax, fare, st, ac]
wcsv("demo_airline_flights.csv", None, None) if False else None
# wcsv needs header — regenerate properly
p = OUT / "demo_airline_flights.csv"
with open(p, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["flight_no","route","depart_utc","arrive_utc","delay_min","passengers","fare_inr","status","aircraft"])
    n = 0
    for row in flights(): w.writerow(row); n += 1
print(f"demo_airline_flights.csv: {n:,} rows -> {p.stat().st_size/1e6:.1f} MB")

# ── 2. Retail inventory (~120K) ───────────────────────────────────────
CAT = ["GROCERY","ELECTRONICS","APPAREL","HOME","BEAUTY","SPORTS"]
WH = ["WH-North","WH-South","WH-East","WH-West","WH-Central"]
SUP = ["Supp-A","Supp-B","Supp-C","Supp-D","Supp-E"]
def inv():
    for i in range(1, 120001):
        r = random.random()
        sku = f"SKU-{random.randint(10000,99999)}" if r > 0.004 else f"SKU-{i}"
        cat = random.choice(CAT) if r > 0.02 else "UNKNOWN"
        qty = random.randint(0, 5000) if r > 0.015 else random.randint(-500, -1)
        rl = random.randint(10, 200) if r > 0.03 else random.randint(-100, 0)
        cost = round(random.uniform(10, 5000), 2) if r > 0.02 else round(random.uniform(10**6, 10**8), 2)
        wh = random.choice(WH) if r > 0.02 else ""
        sup = random.choice(SUP) if r > 0.04 else "N/A"
        yield [sku, f"Prod-{random.randint(1,500)}", cat, qty, rl, cost, wh, sup]
p = OUT / "demo_retail_inventory.csv"
with open(p, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["sku","product","category","qty_on_hand","reorder_level","unit_cost","warehouse","supplier"])
    n = 0
    for row in inv(): w.writerow(row); n += 1
print(f"demo_retail_inventory.csv: {n:,} rows -> {p.stat().st_size/1e6:.1f} MB")

# ── 3. Bank loans (~60K) ──────────────────────────────────────────────
LST = ["APPROVED","PENDING","REJECTED","DISBURSED","DEFAULTED","CLOSED"]
BAD_L = ["approved","APPROVE","","unknown","sanctioned"]
def loans():
    for i in range(1, 60001):
        r = random.random()
        lid = f"LN-{i:07d}" if r > 0.005 else f"LN-{i-1:07d}"
        name = f"Applicant {random.randint(1000,9999)}" if r > 0.03 else ("" if r < 0.015 else f"App{i}XYZ")
        inc = round(random.uniform(20000, 200000), 2) if r > 0.02 else round(random.uniform(-50000, 0), 2)
        amt = round(random.uniform(50000, 5000000), 2) if r > 0.02 else round(random.uniform(10**8, 10**9), 2)
        rate = round(random.uniform(7.5, 18.5), 2) if r > 0.02 else round(random.uniform(-5, -0.1), 2)
        term = random.randint(12, 360) if r > 0.02 else random.randint(-60, 0)
        cs = random.randint(300, 900) if r > 0.02 else random.randint(-100, 100)
        st = random.choice(LST) if r > 0.03 else random.choice(BAD_L)
        dt = f"2024-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if r > 0.02 else "not-a-date"
        yield [lid, name, inc, amt, rate, term, cs, st, dt]
p = OUT / "demo_bank_loans.csv"
with open(p, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(["loan_id","applicant","annual_income","loan_amount","interest_rate","term_months","credit_score","status","disbursed_date"])
    n = 0
    for row in loans(): w.writerow(row); n += 1
print(f"demo_bank_loans.csv: {n:,} rows -> {p.stat().st_size/1e6:.1f} MB")
