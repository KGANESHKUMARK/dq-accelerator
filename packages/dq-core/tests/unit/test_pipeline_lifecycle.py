"""Unit tests for the pipeline lifecycle registry."""

import pytest

from dq_core.pipelines.registry import PipelineRegistry, PipelineState
from dq_core.runtime.errors import LifecycleError


def _manifest(pipeline_id: str = "DEMO_PIPELINE") -> "PipelineManifest":
    from dq_core.pipelines.manifest import PipelineManifest
    return PipelineManifest.from_dict({
        "pipeline_id": pipeline_id, "pipeline_name": "Demo", "platform": "DATABRICKS",
        "runtime": "PYSPARK", "integration_mode": "INLINE", "dq_mode": "OBSERVE_ONLY",
        "datasets": [{"dataset_id": "ds", "format": "PARQUET", "stages": ["SILVER"]}]})


def test_full_lifecycle(tmp_path):
    reg = PipelineRegistry(tmp_path / "pipelines")
    mf = _manifest("DEMO_LC")
    reg.onboard(mf)
    assert reg.state("DEMO_LC") == PipelineState.REGISTERED
    reg.record_test("DEMO_LC", {}, passed=True)
    assert reg.state("DEMO_LC") == PipelineState.TESTED
    reg.activate("DEMO_LC")
    assert reg.state("DEMO_LC") == PipelineState.ACTIVE
    reg.pause("DEMO_LC")
    assert reg.state("DEMO_LC") == PipelineState.PAUSED
    reg.activate("DEMO_LC")
    reg.offboard("DEMO_LC")
    assert reg.state("DEMO_LC") == PipelineState.OFFBOARDED


def test_invalid_transition(tmp_path):
    reg = PipelineRegistry(tmp_path / "pipelines")
    reg.onboard(_manifest("BAD_TRANS"))
    with pytest.raises(LifecycleError, match="not allowed"):
        reg.activate("BAD_TRANS")   # REGISTERED -> ACTIVE not in TRANSITIONS


def test_offboard_retains_history(tmp_path):
    reg = PipelineRegistry(tmp_path / "pipelines")
    reg.onboard(_manifest("HIST_PIPE"))
    reg.record_test("HIST_PIPE", {}, passed=True)
    reg.activate("HIST_PIPE")
    reg.offboard("HIST_PIPE")
    entry = reg.get("HIST_PIPE")
    assert len(entry["history"]) >= 5
    assert entry["offboarding"]["data_deleted"] is False
    assert entry["offboarding"]["history_retained"] is True
