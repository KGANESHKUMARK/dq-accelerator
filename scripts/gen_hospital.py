"""Generate demo_hospital_encounters.csv (~50K rows) with seeded defects."""
import csv, random
from pathlib import Path
random.seed(7)
OUT = Path(__file__).resolve().parent.parent / "data" / "uploads"
FIRST = ["Aarav","Diya","Arjun","Ananya","Vivaan","Myra","Aditya","Sara","Reyansh","Ira"]
LAST = ["Sharma","Patel","Verma","Iyer","Reddy","Nair","Singh","Kumar","Das","Mehta"]
DOCS = ["Dr. Rao","Dr. Iyer","Dr. Khan","Dr. Bose","Dr. Mehta","Dr. Nair"]
DX = ["J18.9","I10","E11.9","J06.9","M54.5","K21.9","N18.3","H10.9"]
STS = ["ADMITTED","DISCHARGED","OBSERVATION","TRANSFERRED","ICU"]
BAD = ["admitted","Admitted ","UNKNOWN","","released"]
hdr = ["encounter_id","patient_name","admission_date","diagnosis_code","los_days","bill_amount","status","doctor"]
p = OUT / "demo_hospital_encounters.csv"
n = 0
with open(p, "w", newline="", encoding="utf-8") as f:
    w = csv.writer(f); w.writerow(hdr)
    for i in range(1, 50001):
        r = random.random()
        eid = f"ENC-{i:06d}" if r > 0.008 else f"ENC-{i-1:06d}"
        name = f"{random.choice(FIRST)} {random.choice(LAST)}" if r > 0.025 else ("" if r < 0.015 else f"{random.choice(FIRST)}{i}{random.choice(LAST)}")
        dt = f"2025-{random.randint(1,12):02d}-{random.randint(1,28):02d}" if r > 0.02 else (f"{random.randint(1,12)}/{random.randint(1,28)}/2025" if r < 0.015 else "date-x")
        dx = random.choice(DX) if r > 0.02 else "XX.99"
        los = random.randint(0, 30) if r > 0.015 else random.randint(-10, -1)
        amt = round(random.uniform(500, 150000), 2) if r > 0.02 else round(random.uniform(10**6, 10**7), 2)
        st = random.choice(STS) if r > 0.035 else random.choice(BAD)
        doc = random.choice(DOCS) if r > 0.05 else ""
        w.writerow([eid, name, dt, dx, los, amt, st, doc]); n += 1
print(f"{p.name}: {n:,} rows -> {p} ({p.stat().st_size/1e6:.1f} MB)")
