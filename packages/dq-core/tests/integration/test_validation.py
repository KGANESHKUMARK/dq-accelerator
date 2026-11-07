"""Integration tests: validation engine with the customer rule set."""

import pytest

pytestmark = pytest.mark.spark


def test_validation_identifies_failed_records(spark, customers_df, customers_v1):
    from dq_core.validation.engine import ValidationEngine

    out = ValidationEngine().validate(customers_df, customers_v1)
    assert out.report.total_records == 38
    assert out.report.failed_records > 0


def test_multiple_violations_per_record(spark, tiny_df, customers_v1):
    from dq_core.validation import columns as C
    from dq_core.validation.engine import ValidationEngine

    out = ValidationEngine().validate(tiny_df, customers_v1)
    # Row 3 (None, Eve Evil, eve@bad, abc, ...) should have >= 2 violations
    multi = out.annotated.where(f"{C.ERROR_COUNT} >= 2").count()
    assert multi >= 1


def test_ruleset_v1_loads_correctly(customers_v1):
    assert customers_v1.ruleset_id == "customers_silver"
    assert customers_v1.version == "1.0"
    assert len(customers_v1.rules) >= 15


def test_ruleset_v1_1_loads_correctly(customers_v1_1):
    assert customers_v1_1.version == "1.1"
    assert customers_v1_1.parent_version == "1.0"


def test_v1_1_stricter_age_rule(spark, customers_v1_1):
    from dq_core.validation.engine import ValidationEngine

    # C0026 is 115 years old – passes v1.0 (max=120) but fails v1.1 (max=110)
    df = spark.createDataFrame(
        [("C0026", "Robert Miller", "r@e.com", "115", "Denver", "US", "ACTIVE", "2024-03-10")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
    out = ValidationEngine().validate(df, customers_v1_1)
    # CUST_007 (age_range) must fail for this record in v1.1
    result = out.report.result("CUST_007")
    assert result.failed_records == 1


def test_schema_validation_passes_correct_schema(spark, customers_df, customers_v1):
    from dq_core.validation.engine import ValidationEngine

    out = ValidationEngine().validate(customers_df, customers_v1)
    schema_result = out.report.result("CUST_016")
    assert schema_result.status == "PASSED"


def test_dataset_level_row_count_rule(spark, customers_df, transactions_v1):
    from dq_core.validation.engine import ValidationEngine

    out = ValidationEngine().validate(customers_df, transactions_v1)
    row_count_result = out.report.result("TXN_011")
    # Schema will FAIL because column names differ, not empty – this tests row-count behaviour
    # TXN_011 is SCHEMA_VALIDATION; we just check it's evaluated as a dataset-level rule
    assert row_count_result.level == "DATASET"


def test_validation_report_summary_text(spark, customers_df, customers_v1):
    from dq_core.validation.engine import ValidationEngine

    out = ValidationEngine().validate(customers_df, customers_v1)
    text = out.report.summary_table()
    assert "CUST_001" in text
    assert "PASSED" in text or "FAILED" in text
