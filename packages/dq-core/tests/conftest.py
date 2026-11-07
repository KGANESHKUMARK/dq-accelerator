"""Pytest configuration: shared fixtures for unit and Spark integration tests.

A single SparkSession is created per test session (expensive to create). Tests that
modify DataFrames or write data use temporary directories on D: so C: is never touched.
"""

from __future__ import annotations

import os
import shutil
import tempfile
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[3]   # repo root
SAMPLE_DATA = ROOT / "sample_data" / "bronze"
CONFIG_DIR = ROOT / "config"
SCRATCH = ROOT / ".tmp"


def _ensure_scratch() -> None:
    (SCRATCH / "temp").mkdir(parents=True, exist_ok=True)
    os.environ["TEMP"] = os.environ["TMP"] = os.environ["TMPDIR"] = str(SCRATCH / "temp")
    os.environ.setdefault("DQ_SCRATCH_DIR", str(SCRATCH))


_ensure_scratch()


# ---------------------------------------------------------------------------  SparkSession
@pytest.fixture(scope="session")
def spark():
    """Single reusable local SparkSession for the whole test session."""
    from dq_core.runtime.local_spark import LocalSparkSettings, create_local_spark_session

    session = create_local_spark_session(
        LocalSparkSettings(app_name="dq-tests", master="local[2]",
                           driver_memory="1g", shuffle_partitions=2,
                           scratch_dir=str(SCRATCH)))
    session.sparkContext.setLogLevel("ERROR")
    yield session


# ---------------------------------------------------------------------------  Banking DataFrames
@pytest.fixture(scope="session")
def customers_df(spark):
    from dq_core.io.sources import CsvSource
    return CsvSource(str(SAMPLE_DATA / "customers.csv")).read(spark)


@pytest.fixture(scope="session")
def transactions_df(spark):
    from dq_core.io.sources import CsvSource
    return CsvSource(str(SAMPLE_DATA / "transactions.csv")).read(spark)


# ---------------------------------------------------------------------------  Rule sets
@pytest.fixture(scope="session")
def customers_v1():
    from dq_core.metadata.serialization import load_ruleset
    return load_ruleset(CONFIG_DIR / "rulesets" / "customers_v1.yaml")


@pytest.fixture(scope="session")
def customers_v1_1():
    from dq_core.metadata.serialization import load_ruleset
    return load_ruleset(CONFIG_DIR / "rulesets" / "customers_v1_1.yaml")


@pytest.fixture(scope="session")
def transactions_v1():
    from dq_core.metadata.serialization import load_ruleset
    return load_ruleset(CONFIG_DIR / "rulesets" / "transactions_v1.yaml")


@pytest.fixture(scope="session")
def transactions_gold_v1():
    from dq_core.metadata.serialization import load_ruleset
    return load_ruleset(CONFIG_DIR / "rulesets" / "transactions_gold_v1.yaml")


# ---------------------------------------------------------------------------  Policy
@pytest.fixture(scope="session")
def default_policy():
    from dq_core.policies.models import RemediationPolicyConfig
    return RemediationPolicyConfig.load(CONFIG_DIR / "policies" / "default.yaml")


# ---------------------------------------------------------------------------  Engine
@pytest.fixture(scope="session")
def engine(tmp_registry):
    from dq_core.engine import DataQualityEngine
    from dq_core.runtime.config import EngineConfig

    cfg = EngineConfig.from_yaml(CONFIG_DIR / "engine.yaml")
    cfg.registry.rulesets_path = str(tmp_registry / "rulesets")
    cfg.registry.pipelines_path = str(tmp_registry / "pipelines")
    return DataQualityEngine(cfg)


@pytest.fixture(scope="session")
def tmp_registry(tmp_path_factory):
    p = tmp_path_factory.mktemp("registry")
    return p


@pytest.fixture(scope="session")
def published_customers_v1(engine, customers_v1):
    """Publish customers v1.0 once for the whole session."""
    engine.ruleset_registry.publish(customers_v1, "test_session", activate=True)
    return customers_v1


@pytest.fixture(scope="session")
def published_customers_v1_1(engine, customers_v1_1, published_customers_v1):
    engine.ruleset_registry.publish(customers_v1_1, "test_session", activate=False)
    return customers_v1_1


# ---------------------------------------------------------------------------  helpers
@pytest.fixture
def tiny_df(spark):
    """Minimal clean+dirty DataFrame for fast unit tests."""
    return spark.createDataFrame(
        [("C0001", "Alice Smith", "alice@x.com", "30", "London", "GB", "ACTIVE", "2024-01-01"),
         ("C0002", "Bob Jones",   "bob@x.com",   "25", "Paris",  "FR", "ACTIVE", "2024-01-02"),
         (None,    "Eve Evil",    "eve@bad",      "abc","",       "ZZ", "ACTVE",  "15/01/2024")],
        "customer_id string, customer_name string, email string, age string, "
        "city string, country string, customer_status string, created_date string")
