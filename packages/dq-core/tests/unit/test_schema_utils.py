"""Unit tests for schema utilities (pure Python)."""

from dq_core.runtime.schema import (
    compare_schema, missing_columns, normalize_type_name, validate_cast_type,
)


def test_validate_cast_type_allows_known_types():
    for t in ("string", "int", "integer", "bigint", "double", "float", "boolean",
              "date", "timestamp", "decimal(18,2)"):
        result = validate_cast_type(t)
        assert result, f"expected non-empty result for {t!r}"


def test_validate_cast_type_rejects_unknown():
    from dq_core.runtime.errors import ConfigurationError
    import pytest
    with pytest.raises(ConfigurationError, match="Unsupported"):
        validate_cast_type("varchar(100)")


def test_normalize_type_aliases():
    assert normalize_type_name("integer") == "int"
    assert normalize_type_name("long") == "bigint"
    assert normalize_type_name("bool") == "boolean"


def test_compare_schema_matching():
    expected = [{"name": "id", "type": "string"}, {"name": "val", "type": "int"}]
    actual = [{"name": "id", "type": "string"}, {"name": "val", "type": "int"}]
    result = compare_schema(expected, actual)
    assert result.passed


def test_compare_schema_missing_column():
    expected = [{"name": "id"}, {"name": "missing_col"}]
    actual = [{"name": "id", "type": "string"}]
    result = compare_schema(expected, actual)
    assert not result.passed
    assert "missing_col" in result.missing_columns


def test_compare_schema_extra_column_blocked():
    expected = [{"name": "id"}]
    actual = [{"name": "id", "type": "string"}, {"name": "extra"}]
    result = compare_schema(expected, actual, allow_extra_columns=False)
    assert not result.passed
    assert "extra" in result.unexpected_columns


def test_compare_schema_extra_column_allowed():
    expected = [{"name": "id"}]
    actual = [{"name": "id", "type": "string"}, {"name": "extra"}]
    result = compare_schema(expected, actual, allow_extra_columns=True)
    assert result.passed


def test_missing_columns_case_insensitive():
    assert missing_columns(["ID", "Name"], ["id", "name", "extra"]) == []
    assert missing_columns(["ID", "missing"], ["id"]) == ["missing"]
