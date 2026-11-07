"""Generate a ~100 MB synthetic telecom CDR dataset for onboarding/load tests.

SYNTHETIC demo data only — no real subscribers. Deterministic (fixed seed) so
regenerated files are comparable. ~2-3% seeded defects across realistic
categories: mixed timestamp formats, case-inconsistent categoricals, rare
typos, negative charges, null keys, duplicates, invalid country codes.
"""

from __future__ import annotations

import csv
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "sample_data" / "bronze" / "cdr_records.csv"
TARGET_MB = float(sys.argv[1]) if len(sys.argv) > 1 else 100.0
SEED = 20261107

random.seed(SEED)

PLANS = ["PREPAID", "POSTPAID", "CORPORATE", "IOT"]
EVENTS = ["VOICE", "SMS", "DATA", "ROAM_VOICE", "ROAM_DATA"]
REGIONS = ["NORTH", "SOUTH", "EAST", "WEST", "CENTRAL"]
COUNTRIES = ["IN", "US", "GB", "SG", "AE", "DE", "AU"]
NETWORKS = ["4G", "5G", "3G", "VOLTE"]
DEVICES = ["Galaxy S23", "iPhone 15", "Pixel 8", "OnePlus 12", "Redmi Note 13", "Moto G84", "Nokia G42"]
BILLING = ["BILLED", "UNBILLED", "SETTLED", "DISPUTED"]
DROPS = ["NONE", "NONE", "NONE", "NONE", "WEAK_SIGNAL", "CONGESTION", "HANDOVER_FAIL"]

HEADER = ["record_id", "msisdn", "imsi", "customer_id", "plan_type", "event_type",
          "event_ts", "duration_sec", "bytes_mb", "cell_tower_id", "region",
          "country_code", "roaming_flag", "charge_amount", "billing_status",
          "network_type", "device_model", "sim_activation_date", "drop_reason", "dealer_code"]


def ts(day_offset: int, fmt: str) -> str:
    import datetime as dt
    base = dt.datetime(2024, 5, 1) + dt.timedelta(minutes=day_offset)
    if fmt == "slash":
        return base.strftime("%Y/%m/%d %H:%M:%S")
    if fmt == "dmy":
        return base.strftime("%d/%m/%Y %H:%M:%S")
    return base.strftime("%Y-%m-%d %H:%M:%S")


def act_date(fmt: str) -> str:
    import datetime as dt
    d = dt.date(2021, 1, 1) + dt.timedelta(days=random.randint(0, 1300))
    if fmt == "dmy":
        return d.strftime("%d/%m/%Y")
    return d.strftime("%Y-%m-%d")


def row(i: int) -> list[str]:
    plan = random.choice(PLANS)
    ev = random.choice(EVENTS)
    region = random.choice(REGIONS)
    net = random.choice(NETWORKS)
    country = random.choice(COUNTRIES)
    roaming = "Y" if ev.startswith("ROAM") else "N"
    charge = round(random.uniform(0.01, 45.0), 2)
    duration = random.randint(1, 3600) if "VOICE" in ev else 0
    bts = round(random.uniform(0.5, 2048.0), 1) if "DATA" in ev else 0.0
    r = [
        f"R{i:09d}", f"91{random.randint(7000000000, 9999999999)}",
        f"404{random.randint(10, 99)}{random.randint(100000000, 999999999)}",
        f"C{random.randint(100000, 999999)}", plan, ev,
        ts(i % 60000, "std"), duration, bts,
        f"TW-{random.choice(REGIONS)[:2]}{random.randint(100, 999)}",
        region, country, roaming, charge, random.choice(BILLING), net,
        random.choice(DEVICES), act_date("std"), random.choice(DROPS),
        f"DLR{random.randint(1000, 9999)}",
    ]
    # ---- seeded defects (~2.5% total) ----------------------------------------
    p = random.random()
    if p < 0.003:
        r[0] = ""                                        # null record_id
    elif p < 0.006:
        r[4] = plan.lower()                              # case-inconsistent plan
    elif p < 0.009:
        r[10] = region.lower()                           # case-inconsistent region
    elif p < 0.011:
        r[5] = {"VOICE": "VOIC", "DATA": "DAT", "SMS": "SMSs"}.get(ev, ev)  # event typo
    elif p < 0.013:
        r[6] = ts(i % 60000, "slash")                    # mixed ts format yyyy/MM/dd
    elif p < 0.014:
        r[6] = ts(i % 60000, "dmy")                      # mixed ts format dd/MM/yyyy
    elif p < 0.016:
        r[13] = -abs(charge)                             # negative charge
    elif p < 0.018:
        r[1] = f" {r[1]} "                               # whitespace on msisdn
    elif p < 0.020:
        r[11] = "XX"                                     # invalid country code
    elif p < 0.022:
        r[15] = net.lower()                              # case-inconsistent network
    elif p < 0.024:
        r[3] = ""                                        # null customer_id
    elif p < 0.025:
        r[17] = act_date("dmy")                          # mixed activation date format
    if i and i % 500000 == 0:
        return PREV[i % len(PREV)]                        # exact duplicate
    PREV.append(r)
    return r


def main() -> None:
    OUT.parent.mkdir(parents=True, exist_ok=True)
    n, size = 0, 0
    target = TARGET_MB * 1024 * 1024
    with open(OUT, "w", newline="", encoding="utf-8") as fh:
        w = csv.writer(fh)
        w.writerow(HEADER)
        size += len(",".join(HEADER)) + 2
        while size < target:
            r = row(n)
            w.writerow(r)
            size += sum(len(str(v)) + 1 for v in r) + 1
            n += 1
            if n % 200000 == 0:
                print(f"  {n:,} rows  {size/1024/1024:.1f} MB", flush=True)
    print(f"DONE  {n:,} rows  {OUT.stat().st_size/1024/1024:.1f} MB -> {OUT}")


PREV: list[list[str]] = []
if __name__ == "__main__":
    main()
