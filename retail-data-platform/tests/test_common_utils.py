"""Unit tests for the reusable framework in common_utils."""
import json

from common_utils.config import load_config
from common_utils.data_quality import split_valid_invalid
from common_utils.generic_functions import (
    apply_standard_steps,
    deduplicate,
    rename_columns,
    safe_cast,
)


def test_load_config_fills_catalog_placeholder(tmp_path):
    path = tmp_path / "c.json"
    path.write_text(json.dumps({"t": "{catalog}.silver.x", "l": ["{catalog}.a"], "n": 1}))
    cfg = load_config(str(path), catalog="dev_cat")
    assert cfg == {"t": "dev_cat.silver.x", "l": ["dev_cat.a"], "n": 1}


def test_rename_ignores_missing_columns(spark):
    df = spark.createDataFrame([(1, "a")], "cust_id int, name string")
    out = rename_columns(df, {"cust_id": "customer_id", "missing": "x"})
    assert out.columns == ["customer_id", "name"]


def test_safe_cast_turns_bad_values_into_null_in_ansi_mode(spark):
    df = spark.createDataFrame([("34.0", "abc"), ("7", "2026-01-02")], "a string, d string")
    out = safe_cast(df, {"a": "long", "d": "date", "not_there": "int"}).collect()
    assert out[0]["a"] == 34 and out[0]["d"] is None
    assert out[1]["a"] == 7 and str(out[1]["d"]) == "2026-01-02"


def test_deduplicate_keeps_latest(spark):
    df = spark.createDataFrame([(1, 10), (1, 20), (2, 5)], "k int, ts int")
    out = deduplicate(df, ["k"], [{"column": "ts", "direction": "desc"}])
    assert sorted((r.k, r.ts) for r in out.collect()) == [(1, 20), (2, 5)]


def test_apply_standard_steps_order(spark):
    df = spark.createDataFrame([("1.0", "x", "drop me")], "id string, name string, junk string")
    cfg = {"rename_columns": {"name": "full_name"}, "drop_columns": ["junk"],
           "cast_columns": {"id": "long"}, "select_columns": ["id", "full_name"]}
    out = apply_standard_steps(df, cfg)
    assert out.columns == ["id", "full_name"]
    assert out.first()["id"] == 1


def test_quarantine_split(spark):
    df = spark.createDataFrame([(1, 5), (None, 3), (2, -1)], "id long, qty long")
    rules = [{"name": "id_not_null", "condition": "id IS NOT NULL"},
             {"name": "qty_positive", "condition": "qty > 0"}]
    valid, invalid = split_valid_invalid(df, rules)
    assert [r.id for r in valid.collect()] == [1]
    reasons = {r.qty: r.dq_failed_rules for r in invalid.collect()}
    assert reasons == {3: "id_not_null", -1: "qty_positive"}
