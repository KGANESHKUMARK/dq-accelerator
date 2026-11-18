"""Generate ~200MB college_students.csv with realistic + seeded defects.

Usage: .venv/Scripts/python.exe scripts/gen_college_students.py [rows_millions]
Default ~2.1M rows (~200MB). Deterministic-ish via seed.
"""
import csv
import random
import sys
from pathlib import Path

random.seed(42)
OUT = Path(__file__).resolve().parents[1] / "sample_data" / "bronze" / "college_students.csv"

FIRST = ["Aarav","Diya","Ishaan","Ananya","Rohan","Priya","Arjun","Kavya","Vihaan","Sneha","Aditya","Ira","Kabir","Meera","Reyansh","Kiara","Vivaan","Aadhya","Dhruv","Sara","Om","Riya","Yash","Anika","Krishna","Navya","Shivansh","Pari","Aarush","Myra"]
LAST = ["Sharma","Patel","Verma","Iyer","Reddy","Nair","Singh","Khan","Das","Mehta","Joshi","Rao","Pillai","Chopra","Malhotra","Bose","Kulkarni","Deshpande","Chatterjee","Menon"]
CITIES = ["Mumbai","Delhi","Bangalore","Chennai","Hyderabad","Pune","Kolkata","Ahmedabad","Jaipur","Lucknow","Chandigarh","Kochi","Indore","Bhopal","Nagpur","Surat","Vadodara","Visakhapatnam","Coimbatore","Madurai"]
COURSES = ["B.Tech CSE","B.Tech ECE","B.Tech ME","B.Sc Physics","B.Sc Math","B.Com","BBA","BCA","MBA","M.Tech CSE","B.Arch","B.Pharm"]
DEPTS = ["CSE","ECE","ME","PHY","MAT","COM","BBA","BCA","MGT","ARC","PHR"]

def email(fn, ln, i):
    if random.random() < 0.06: return f"{fn}.{ln}"          # missing @domain
    if random.random() < 0.03: return f"{fn}@{ln}.edu"     # wrong tld-ish
    return f"{fn.lower()}.{ln.lower()}{i%97}@university.edu"

def phone(i):
    r = random.random()
    if r < 0.05: return str(random.randint(1000,99999))          # too short
    if r < 0.08: return f"+91-{random.randint(7000000000,9999999999)}"  # ok format
    return f"{random.randint(6000000000,9999999999)}"

def main():
    target_mb = float(sys.argv[1]) if len(sys.argv) > 1 else 200.0
    target_bytes = int(target_mb * 1024 * 1024)
    cols = ["student_id","full_name","email","phone","date_of_birth","gender",
            "course","semester","gpa","enrollment_year","city","attendance_pct","is_scholarship"]
    OUT.parent.mkdir(parents=True, exist_ok=True)
    written = 0; rows = 0; buf = []
    with open(OUT, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(cols)
        while written < target_bytes:
            for _ in range(50000):
                i = rows
                fn, ln = random.choice(FIRST), random.choice(LAST)
                sid = "" if random.random() < 0.01 else f"STU{100000+i}"
                dob_y = random.randint(1998, 2007)
                dob = f"{dob_y}-{random.randint(1,12):02d}-{random.randint(1,28):02d}"
                if random.random() < 0.04: dob = f"{random.randint(2025,2030)}-01-15"  # future date
                sem = random.randint(1, 8)
                if random.random() < 0.03: sem = random.randint(9, 14)               # out of range
                gpa = round(random.uniform(5.5, 10.0), 2)
                if random.random() < 0.05: gpa = round(random.uniform(0, 4.5), 2)    # scale mismatch
                att = round(random.uniform(40, 100), 1)
                if random.random() < 0.04: att = round(random.uniform(101, 150), 1)  # >100%
                row = [sid, f"{fn} {ln}", email(fn,ln,i), phone(i), dob,
                       random.choice(["M","F","O"]),
                       random.choice(COURSES), sem, gpa,
                       random.randint(2020, 2025),
                       random.choice(CITIES), att,
                       "Y" if random.random() < 0.2 else "N"]
                w.writerow(row)
                written += len(",".join(str(c) for c in row)) + 1
                rows += 1
                if rows % 500000 == 0:
                    print(f"  {rows/1e6:.1f}M rows, {written/1048576:.0f}MB...", flush=True)
    print(f"DONE: {rows:,} rows, {written/1048576:.0f}MB -> {OUT}")

if __name__ == "__main__":
    main()
