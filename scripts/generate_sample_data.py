"""Generate the SYNTHETIC banking sample datasets and their expected DQ outcomes.

All records are synthetic (no real customers). Identifiers are deterministic and every injected
defect is listed in ``sample_data/expected_results.json`` together with the outcome the POC must
produce (used by the automated tests and the demo script).

Usage:  .venv\\Scripts\\python.exe scripts\\generate_sample_data.py
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "sample_data"

CUSTOMER_COLUMNS = ["customer_id", "customer_name", "email", "age", "city", "country", "customer_status", "created_date"]
TRANSACTION_COLUMNS = ["transaction_id", "customer_id", "transaction_amount", "currency", "transaction_date",
                       "transaction_type", "transaction_status"]

# (row, defect description, expected SILVER outcome in REMEDIATE mode with customers v1.0)
CUSTOMERS = [
    (["C0001", "Aarav Sharma", "aarav.sharma@example.com", "34", "Mumbai", "IN", "ACTIVE", "2023-01-15"], "clean", "ACCEPTED"),
    (["C0002", "Emma Johnson", "emma.johnson@example.co.uk", "41", "London", "GB", "ACTIVE", "2023-02-20"], "clean", "ACCEPTED"),
    (["C0003", "Liam Smith", "liam.smith@example.com", "29", "New York", "US", "INACTIVE", "2023-03-05"], "clean", "ACCEPTED"),
    (["C0004", "Priya Nair", "priya.nair@example.in", "52", "Kochi", "IN", "ACTIVE", "2023-03-18"], "clean", "ACCEPTED"),
    (["C0005", "Wei Chen", "wei.chen@example.sg", "38", "Singapore", "SG", "ACTIVE", "2023-04-02"], "exact duplicate (survivor)", "ACCEPTED"),
    (["C0006", "Fatima Al Mansoori", "fatima.almansoori@example.ae", "45", "Dubai", "AE", "ACTIVE", "2023-04-22"], "clean", "ACCEPTED"),
    (["C0007", "Lukas Weber", "lukas.weber@example.de", "31", "Berlin", "DE", "DORMANT", "2023-05-10"], "clean", "ACCEPTED"),
    (["C0008", "Olivia Brown", "olivia.brown@example.com.au", "27", "Sydney", "AU", "ACTIVE", "2023-05-28"], "clean", "ACCEPTED"),
    (["C0009", "Rohan Mehta", "rohan.mehta@example.com", "60", "Pune", "IN", "CLOSED", "2023-06-14"], "clean", "ACCEPTED"),
    (["C0010", "Sophie Martin", "sophie.martin@example.com", "36", "Manchester", "GB", "ACTIVE", "2023-07-01"], "clean", "ACCEPTED"),
    (["C0011", "Noah Williams", "noah.williams@example.com", "48", "Chicago", "US", "ACTIVE", "2023-07-19"], "clean", "ACCEPTED"),
    (["C0012", "Ananya Iyer", "ananya.iyer@example.in", "25", "Chennai", "IN", "ACTIVE", "2023-08-03"], "conflicting duplicate customer_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["C0013", "James Taylor", "james.taylor@example.com", "55", "Boston", "US", "INACTIVE", "2023-08-21"], "clean", "ACCEPTED"),
    (["C0014", "Mei Lin Tan", "meilin.tan@example.sg", "33", "Singapore", "SG", "ACTIVE", "2023-09-09"], "clean", "ACCEPTED"),
    (["C0015", "Omar Haddad", "omar.haddad@example.ae", "39", "Abu Dhabi", "AE", "ACTIVE", "2023-09-30"], "clean", "ACCEPTED"),
    (["C0016", "Hannah Fischer", "hannah.fischer@example.de", "44", "Munich", "DE", "ACTIVE", "2022-11-30"], "INFO: created before programme start", "ACCEPTED"),
    (["C0005", "Wei Chen", "wei.chen@example.sg", "38", "Singapore", "SG", "ACTIVE", "2023-04-02"], "exact duplicate (removed)", "REMOVED_DUPLICATE"),
    (["C0017", "  Isabella Rossi  ", "isabella.rossi@example.com", "30", "London", "GB", "ACTIVE", "2023-10-05"], "name whitespace -> TRIM", "ACCEPTED_REMEDIATED"),
    (["C0018", "Arjun Reddy", " ARJUN.REDDY@EXAMPLE.COM ", "42", "Hyderabad", "IN", "ACTIVE", "2023-11-02"], "email whitespace/case -> TRIM+LOWERCASE", "ACCEPTED_REMEDIATED"),
    ([" C0019", "Chloe Dubois", "chloe.dubois@example.com", "37", "London", "GB", "ACTIVE", "2023-11-15"], "customer_id whitespace -> TRIM", "ACCEPTED_REMEDIATED"),
    (["C0020", "Daniel Kim", "daniel.kim@example.com", "46", "Seattle", "usa", "active", "2023-12-01"], "country lookup + status uppercase", "ACCEPTED_REMEDIATED"),
    (["C0021", "Kavya Menon", "kavya.menon@example.in", "28", "Bengaluru", "India", "ACTVE", "15/01/2024"], "country lookup + AI status 0.97 + date standardisation", "ACCEPTED_REMEDIATED"),
    (["C0022", "Ethan Clarke", "ethan.clarke@example.com", "51", "Leeds", "Untied Kingdom", "ACTIVE", "2024-01-20"], "AI country 0.95 (high confidence)", "ACCEPTED_REMEDIATED"),
    (["C0023", "Grace Lee", "", "33", "Perth", "AU", "ACTIVE", "2024-02-05"], "missing email (cannot be invented)", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["C0024", "Vikram Singh", "vikram.singh@example.com", "thirty-five", "Delhi", "IN", "ACTIVE", "2024-02-12"], "age not numeric", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["C0025", "Sara Ali", "sara.ali@example.ae", "150", "Dubai", "AE", "ACTIVE", "2024-02-28"], "age out of range", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["C0026", "Robert Miller", "robert.miller@example.com", "115", "Denver", "US", "ACTIVE", "2024-03-10"], "valid in v1.0, fails v1.1 age max 110", "ACCEPTED"),
    (["C0027", "Ayesha Khan", "ayesha.khan@example.com", "29", "", "IN", "ACTIVE", "2024-03-22"], "missing city -> approved NULL_REPLACEMENT", "ACCEPTED_REMEDIATED"),
    (["C0028", "Mohammed Rafi", "mohammed.rafi@example.com", "63", "Chennai", "Bharat", "ACTIVE", "2024-04-01"], "AI proposes invalid ISO-3 'IND' (0.91) -> fails revalidation", "QUARANTINED:CORRECTION_FAILED_REVALIDATION"),
    (["C0029", "Lucas Silva", "lucas.silva@example.com", "35", "Atlantis City", "Atlantis", "ACTIVE", "2024-04-12"], "AI low confidence 0.34", "QUARANTINED:BELOW_CONFIDENCE_THRESHOLD"),
    (["C0030", "Meera Pillai", "meera.pillai@example.in", "31", "Kochi", "IN", "ACT", "2024-04-25"], "AI low confidence 0.62", "QUARANTINED:BELOW_CONFIDENCE_THRESHOLD"),
    (["C0031", "Neha Gupta", "neha.gupta@yahoo", "26", "Delhi", "IN", "ACTIVE", "2024-05-03"], "AI email 0.93 but rule requires review", "QUARANTINED:PENDING_HUMAN_APPROVAL"),
    (["C0032", "J0hn Doe", "john.doe@example.com", "40", "Austin", "US", "ACTIVE", "2024-05-15"], "AI name 0.98 on sensitive identity column", "QUARANTINED:PENDING_HUMAN_APPROVAL"),
    (["C0033", "Arthur Pendleton", "arthur.pendleton@example.com", "58", "Bristol", "GB", "ACTIVE", "2024-02-30"], "invalid calendar date", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["C0034", "Zara Ahmed", "zara.ahmed@example.com", "38", "Dubai", "AE", "CLSD", "2024-06-01"], "AI status exactly 0.90 (threshold)", "ACCEPTED_REMEDIATED"),
    (["C0035", "Ivan Petrov", "ivan.petrov@example.com", "50", "Berlin", "DE", "INACTV", "2024-06-10"], "AI status 0.8999 (just below threshold)", "QUARANTINED:BELOW_CONFIDENCE_THRESHOLD"),
    (["C0012", "Ananya Iyer", "ananya.i@example.in", "25", "Chennai", "IN", "ACTIVE", "2023-08-03"], "conflicting duplicate customer_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
    (["", "Unknown Person", "unknown.person@example.com", "45", "Pune", "IN", "ACTIVE", "2024-06-20"], "missing customer_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION"),
]

# (row, defect, expected SILVER outcome (REMEDIATE v1.0), expected GOLD outcome)
TRANSACTIONS = [
    (["T0001", "C0001", "1500.00", "INR", "2024-01-05", "PURCHASE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0002", "C0002", "250.75", "GBP", "2024-01-06", "PURCHASE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0003", "C0003", "1200.00", "USD", "2024-01-07", "TRANSFER", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0004", "C0004", "5000.00", "INR", "2024-01-08", "DEPOSIT", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0005", "C0005", "320.40", "SGD", "2024-01-09", "PURCHASE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0006", "C0006", "780.00", "AED", "2024-01-10", "WITHDRAWAL", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0007", "C0007", "99.99", "EUR", "2024-01-11", "PURCHASE", "PENDING"], "exact duplicate (survivor)", "ACCEPTED", "GOLD"),
    (["T0008", "C0008", "450.00", "AUD", "2024-01-12", "PURCHASE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0009", "C0009", "-120.00", "INR", "2024-01-13", "REFUND", "COMPLETED"], "negative REFUND (valid by business rule)", "ACCEPTED", "GOLD"),
    (["T0010", "C0010", "60.00", "GBP", "2024-01-14", "FEE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0011", "C0011", "2200.00", "USD", "2024-01-15", "TRANSFER", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0012", "C0013", "875.25", "USD", "2024-01-16", "PURCHASE", "FAILED"], "clean", "ACCEPTED", "GOLD"),
    (["T0013", "C0014", "150.00", "SGD", "2024-01-17", "PURCHASE", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0014", "C0015", "3000.00", "AED", "2024-01-18", "DEPOSIT", "COMPLETED"], "clean", "ACCEPTED", "GOLD"),
    (["T0015", "C0016", "410.00", "EUR", "2024-01-19", "PURCHASE", "COMPLETED"], "conflicting duplicate transaction_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0016", "C0001", "2500000.00", "INR", "2024-01-20", "TRANSFER", "COMPLETED"], "large amount (Gold WARNING)", "ACCEPTED", "GOLD_WITH_WARNING"),
    (["T0017", "C0002", "-45.50", "GBP", "2024-01-21", "REFUND", "COMPLETED"], "negative REFUND (valid)", "ACCEPTED", "GOLD"),
    (["T0018", "C0003", "300.00", "USD", "2024-01-22", "WITHDRAWAL", "REVERSED"], "clean", "ACCEPTED", "GOLD"),
    (["T0019", "C0017", "120.00", "GBP", "2024-01-23", "PURCHASE", "COMPLETED"], "customer remediated in Silver", "ACCEPTED", "GOLD"),
    (["T0020", "C0021", "640.00", "INR", "2024-01-24", "PURCHASE", "COMPLETED"], "customer remediated in Silver", "ACCEPTED", "GOLD"),
    (["T0007", "C0007", "99.99", "EUR", "2024-01-11", "PURCHASE", "PENDING"], "exact duplicate (removed)", "REMOVED_DUPLICATE", "-"),
    (["T0015", "C0016", "999.00", "EUR", "2024-01-19", "PURCHASE", "COMPLETED"], "conflicting duplicate transaction_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0022", "", "75.00", "USD", "2024-01-25", "PURCHASE", "COMPLETED"], "missing customer_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0023", "C9999", "500.00", "USD", "2024-01-26", "PURCHASE", "COMPLETED"], "unknown customer reference", "ACCEPTED", "GOLD_QUARANTINE:REFERENTIAL_INTEGRITY"),
    (["T0024", "C0023", "210.00", "AUD", "2024-01-27", "PURCHASE", "COMPLETED"], "customer quarantined in Silver", "ACCEPTED", "GOLD_QUARANTINE:REFERENTIAL_INTEGRITY"),
    (["T0025", "C0004", "1O50.00", "INR", "2024-01-28", "PURCHASE", "COMPLETED"], "AI amount 0.97 on sensitive amount column", "QUARANTINED:PENDING_HUMAN_APPROVAL", "-"),
    (["T0026", "C0006", "abc", "AED", "2024-01-29", "PURCHASE", "COMPLETED"], "non-numeric amount, no proposal", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0027", "C0008", "-75.00", "AUD", "2024-01-30", "PURCHASE", "COMPLETED"], "negative PURCHASE (business rule)", "ACCEPTED", "GOLD_QUARANTINE:BUSINESS_RULE"),
    (["T0028", "C0009", "820.00", " usd", "2024-01-31", "PURCHASE", "COMPLETED"], "currency whitespace/case", "ACCEPTED_REMEDIATED", "GOLD"),
    (["T0029", "C0010", "95.00", "GBP", "05/02/2024", "PURCHASE", "COMPLETED"], "date dd/MM/yyyy -> standardised", "ACCEPTED_REMEDIATED", "GOLD"),
    (["T0030", "C0011", "130.00", "USD", "2024-02-31", "PURCHASE", "COMPLETED"], "invalid calendar date", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0031", "C0013", "55.00", "USD", "2024-02-02", "purchase ", "COMPLETED"], "type whitespace/case", "ACCEPTED_REMEDIATED", "GOLD"),
    (["T0032", "C0014", "700.00", "SGD", "2024-02-03", "TRANSFER", "COMPLETD"], "AI status 0.96", "ACCEPTED_REMEDIATED", "GOLD"),
    (["T0033", "C0015", "66.00", "AED", "2024-02-04", "PURCHASE", "DONE"], "AI status 0.70 (low confidence)", "QUARANTINED:BELOW_CONFIDENCE_THRESHOLD", "-"),
    (["T0034", "C0016", "88.00", "XYZ", "2024-02-05", "PURCHASE", "COMPLETED"], "invalid currency", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0035", "C0012", "140.00", "INR", "2024-02-06", "PURCHASE", "COMPLETED"], "customer quarantined in Silver", "ACCEPTED", "GOLD_QUARANTINE:REFERENTIAL_INTEGRITY"),
    (["", "C0001", "10.00", "INR", "2024-02-07", "FEE", "COMPLETED"], "missing transaction_id", "QUARANTINED:UNRESOLVED_NO_SAFE_REMEDIATION", "-"),
    (["T0037", "C0022", "410.00", "GBP", "2024-02-08", "PURCHASE", "COMPLETED"], "customer remediated by AI", "ACCEPTED", "GOLD"),
    (["T0038", "C0034", "77.00", "AED", "2024-02-09", "PURCHASE", "COMPLETED"], "customer remediated by AI", "ACCEPTED", "GOLD"),
    (["T0039", "C0005", "210.00", "SGD", "2024-02-10", "PURCHASE", "COMPLETED"], "deduplicated customer", "ACCEPTED", "GOLD"),
]


def _field(value: str) -> str:
    """Empty string = NULL (unquoted); values with edge whitespace / commas / quotes are quoted."""
    if value == "":
        return ""
    if value != value.strip() or any(ch in value for ch in ',"\n'):
        return '"' + value.replace('"', '""') + '"'
    return value


def _write_csv(path: Path, header: list[str], rows: list[list[str]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    lines = [",".join(header)] + [",".join(_field(v) for v in row) for row in rows]
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    # Sanity check: the file must round-trip through the standard csv reader.
    with open(path, newline="", encoding="utf-8") as handle:
        parsed = list(csv.reader(handle))
    assert parsed[1:] == rows, f"CSV round-trip mismatch for {path}"


def _count(outcomes: list[str], prefix: str) -> int:
    return sum(1 for o in outcomes if o.startswith(prefix))


def build_expected() -> dict:
    cust = [o for _, _, o in CUSTOMERS]
    txn_silver = [s for _, _, s, _ in TRANSACTIONS]
    txn_gold = [g for _, _, _, g in TRANSACTIONS]
    gold_amount = sum(float(r[2]) for r, _, _, g in TRANSACTIONS if g.startswith("GOLD") and not g.startswith("GOLD_QUARANTINE"))
    gold_customers = sorted({r[1] for r, _, _, g in TRANSACTIONS if g.startswith("GOLD") and not g.startswith("GOLD_QUARANTINE")})
    return {
        "note": "SYNTHETIC data. Expected outcomes for customers_v1.0 / transactions_v1.0 with the default policy (threshold 0.90).",
        "customers": {
            "input_records": len(CUSTOMERS),
            "remediate_v1_0": {
                "accepted": _count(cust, "ACCEPTED"),
                "accepted_remediated": _count(cust, "ACCEPTED_REMEDIATED"),
                "quarantined": _count(cust, "QUARANTINED"),
                "removed_duplicates": _count(cust, "REMOVED_DUPLICATE"),
            },
            "enforce_v1_0": {"accepted": 16, "quarantined": 22},
            "observe_only_v1_0": {"accepted": len(CUSTOMERS), "flagged_records": 22},
            "remediate_v1_1": {"accepted": _count(cust, "ACCEPTED") - 1, "quarantined": _count(cust, "QUARANTINED") + 1},
            "records": [{"row": i + 1, "customer_id": r[0], "defect": d, "expected": o} for i, (r, d, o) in enumerate(CUSTOMERS)],
        },
        "transactions": {
            "input_records": len(TRANSACTIONS),
            "remediate_v1_0": {
                "accepted": _count(txn_silver, "ACCEPTED"),
                "accepted_remediated": _count(txn_silver, "ACCEPTED_REMEDIATED"),
                "quarantined": _count(txn_silver, "QUARANTINED"),
                "removed_duplicates": _count(txn_silver, "REMOVED_DUPLICATE"),
            },
            "enforce_v1_0": {"accepted": 25, "quarantined": 15},
            "gold": {
                "accepted": sum(1 for g in txn_gold if g in ("GOLD", "GOLD_WITH_WARNING")),
                "quarantined": _count(txn_gold, "GOLD_QUARANTINE"),
                "warnings": _count(txn_gold, "GOLD_WITH_WARNING"),
                "summary_customers": len(gold_customers),
                "summary_total_amount": round(gold_amount, 2),
            },
            "records": [{"row": i + 1, "transaction_id": r[0], "defect": d, "expected_silver": s, "expected_gold": g}
                        for i, (r, d, s, g) in enumerate(TRANSACTIONS)],
        },
    }


def main() -> None:
    _write_csv(OUT / "bronze" / "customers.csv", CUSTOMER_COLUMNS, [r for r, _, _ in CUSTOMERS])
    _write_csv(OUT / "bronze" / "transactions.csv", TRANSACTION_COLUMNS, [r for r, _, _, _ in TRANSACTIONS])
    expected = build_expected()
    (OUT / "expected_results.json").write_text(json.dumps(expected, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: {kk: vv for kk, vv in v.items() if kk != "records"} for k, v in expected.items() if k != "note"}, indent=2))


if __name__ == "__main__":
    main()
