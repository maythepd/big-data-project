"""Spark fixtures for every Silver rule (src/silver/rules.py, R01-R14)."""

from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession, functions as F

from src.eda.common import RAW_COLUMNS
from src.silver.rules import apply_rules, dedup, finalize, load_rules

spark = (SparkSession.builder.appName("Check-Silver-Rules")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "2").getOrCreate())
spark.sparkContext.setLogLevel("ERROR")
T = "2024-01-01T00:00:00Z"
rows = [
    # id, sensor, location, datetime, lat, lon, parameter, units, value
    ("1", "11", "ok", T, "10", "20", "pm25", "µg/m³", "12"),                     # valid
    ("2", "12", "nf", "2024-03-10T00:00:01-03:-30", "47.5", "-52.7", "pm25", "µg/m³", "3"),  # R02
    ("3", "13", "group D", None, None, None, None, None, None),                 # R01 head
    ("-244365\"", "2021-08-13T19:01:07+08:00", "113.9", "113.9", "pm25", "µg/m³", "5.0", None, None),  # R01 tail
    ("5", "15", "x", T, "10", "20", "o3", "ppb", "40"),                         # R06 ppb -> ppm
    ("6", "16", "x", T, "10", "20", "no2", "µg/m³", "46.01"),                   # R06 µg/m³ -> ppm
    ("7", "17", "x", T, "10", "20", "temperature", "f", "50"),                  # R06 °F -> °C
    ("8", "18", "x", T, "10", "20", "pm25", "μg/m³", "7"),                      # R05 Greek mu
    ("9", "19", "x", T, "10", "20", "pm25", "µg/m³", "-9999"),                  # R08 sentinel
    ("10", "20", "x", T, "10", "20", "pm25", "µg/m³", "3.396151e37"),           # R08 extreme
    ("11", "21", "x", T, "10", "20", "pm25", "µg/m³", "2500"),                  # R09 above max
    ("12", "22", "x", T, "10", "20", "pm25", "µg/m³", "-3"),                    # R10 negative small
    ("13", "23", "x", T, "10", "20", "pm25", "ppm", "5"),                       # R07 invalid pair
    ("14", "24", "x", T, "200", "200", "pm25", "µg/m³", "1"),                   # R11 sentinel coord, fixed ref
    ("15", "25", "x", T, "127.5", "127.5", "pm25", "µg/m³", "1"),               # R11 lat == lon, no ref
    ("16", "26", "x", "1990-01-01T00:00:00Z", "10", "20", "pm25", "µg/m³", "1"),  # R04 too old
    ("17", "27", "x", T, "10", "20", "nox", "µg/m³", "30"),                     # unit_unconverted
    ("18", "28", "x", T, "10", "20", "pm25", "µg/m³", "4"),                     # R13 dup (block 2)
    ("18", "28", "x", T, "10", "20", "pm25", "µg/m³", "4"),                     # R13 dup (block 1)
    ("19", "29", "x", T, "10", "20", "co", "ppb", "0.5"),                       # R06b relabel_ppm
    ("20", "30", "x", T, "10", "20", "temperature", "c", "77"),                 # R06b relabel_f
    ("21", "31", "x", T, "10", "20", "pm10", "µg/m³", "900"),                   # R06b suspect
    ("22", "32", "x", T, "10", "20", "co", "ppb", "500"),                       # ppb, not in table
]
try:
    df = spark.createDataFrame(rows, ", ".join(f"{c} string" for c in RAW_COLUMNS))
    for raw in ["value", "lat", "lon"]:
        df = df.withColumn(f"{raw}_num", F.col(raw).cast("double"))
    df = (df.withColumn("datetime_ts", F.to_timestamp("datetime"))
            .withColumn("source_block", F.when(F.monotonically_increasing_id() % 2 == 0, "blk_2").otherwise("blk_1")))
    dim = spark.createDataFrame([("14", "fixed", 1.0, 2.0)], "location_id string, location_kind string, lat_ref double, lon_ref double")
    rules = load_rules()
    units = spark.createDataFrame([("29", "co", "ppb", "relabel_ppm"), ("30", "temperature", "c", "relabel_f"),
                                   ("31", "pm10", "µg/m³", "suspect")],
                                  "sensors_id string, parameter string, units string, action string")
    measured, quarantine = apply_rules(df, rules, dim, "2026-10-24T00:00:00Z", units)
    deduped, keep = dedup(measured.repartition("sensors_id"))
    # collect() converts timestamps to the local timezone: compare UTC strings formatted by Spark.
    final = finalize(deduped.filter(keep)).withColumn("utc", F.date_format("datetime_utc", "yyyy-MM-dd HH:mm:ss"))
    out = {r.location_id: r.asDict() for r in final.collect()}
    q = {r.location_id: r.quarantine_reason for r in quarantine.collect()}

    assert q == {"3": "multiline_split_record", '-244365"': "multiline_split_record"}, q
    assert out["1"]["dq_status"] == "valid" and out["1"]["value"] == 12 and out["1"]["coord_source"] == "row"
    assert out["2"]["datetime_offset_repaired"] and out["2"]["utc"] == "2024-03-10 03:30:01"
    assert out["2"]["dq_status"] == "valid"
    assert abs(out["5"]["value"] - 0.04) < 1e-12 and out["5"]["unit_std"] == "ppm"
    assert abs(out["6"]["value"] - 0.02445) < 1e-9
    assert abs(out["7"]["value"] - 10.0) < 1e-9 and out["7"]["unit_std"] == "c"
    assert out["8"]["unit_std"] == "µg/m³" and out["8"]["dq_status"] == "valid"
    for i, flag in [("9", "sentinel"), ("10", "extreme"), ("11", "above_max"), ("13", "invalid_pair"), ("16", "ts_out_of_range")]:
        assert out[i][flag] and out[i]["dq_status"] == "invalid" and out[i]["value"] is None, (i, out[i])
    assert out["12"]["negative_small"] and out["12"]["dq_status"] == "suspect" and out["12"]["value"] == -3
    assert out["14"]["coord_source"] == "location_ref" and out["14"]["lat"] == 1.0 and out["14"]["coord_filled"]
    assert out["15"]["coord_source"] == "none" and out["15"]["lat"] is None and out["15"]["dq_status"] == "valid"
    assert out["17"]["unit_unconverted"] and out["17"]["dq_status"] == "suspect"
    assert out["18"]["dup_count"] == 2 and out["18"]["source_block"] == "blk_1" and out["18"]["dup_resolved"]
    assert abs(out["19"]["value"] - 0.5) < 1e-12 and out["19"]["unit_relabeled"] and out["19"]["dq_status"] == "suspect"
    assert abs(out["20"]["value"] - 25.0) < 1e-9 and out["20"]["unit_relabeled"] and out["20"]["unit_std"] == "c"
    assert out["21"]["unit_suspect"] and out["21"]["value"] == 900 and out["21"]["dq_status"] == "suspect"
    assert abs(out["22"]["value"] - 0.5) < 1e-12 and out["22"]["dq_status"] == "valid" and not out["22"]["unit_relabeled"]
    assert len(out) == 20 and deduped.filter(~keep).count() == 1
    assert "sentinel" in out["9"]["dq_flags"] and out["1"]["dq_flags"] == []
    print("PASS: Silver rules R01-R14 fixtures (quarantine, offset, units, R06b sensor units, sentinels, bounds, coords, dedup, status)")
finally:
    spark.stop()
