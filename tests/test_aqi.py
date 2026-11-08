"""AQI sub-indices, unit handling, NowCast and overall AQI (src/gold/aqi.py).

Expected values are worked by hand from the published tables (EPA-403/B-26-003 Table 6,
QĐ 1459/QĐ-TCMT Bảng 2), not taken from the code under test.
"""

import numpy as np
import pytest
from pyspark.sql import functions as F

from src.gold import aqi

# (standard, breakpoint key, concentration in the table's unit, expected index)
INDEX_CASES = [
    ("us", "pm25", 0.0, 0), ("us", "pm25", -3.0, 0), ("us", "pm25", 9.0, 50), ("us", "pm25", 9.1, 51),
    ("us", "pm25", 12.0, 56), ("us", "pm25", 35.4, 100), ("us", "pm25", 35.5, 101),
    ("us", "pm25", 55.5, 151), ("us", "pm25", 225.5, 301), ("us", "pm25", 325.4, 500),
    ("us", "pm25", 500.0, 848),                       # above the table: slope of 301-500
    ("us", "pm10", 54.0, 50), ("us", "pm10", 55.0, 51), ("us", "pm10", 604.0, 500),
    ("us", "o3_8h", 0.054, 50), ("us", "o3_8h", 0.055, 51), ("us", "o3_8h", 0.078, 126),
    ("us", "o3_8h", 0.200, 300), ("us", "o3_8h", 0.201, None),          # > 0.200: 1-hour only
    ("us", "o3_1h", 0.124, None), ("us", "o3_1h", 0.125, 101), ("us", "o3_1h", 0.604, 500),
    ("us", "o3_1h", 0.700, 596),
    ("us", "so2_1h", 30.0, 43), ("us", "so2_1h", 304.0, 200), ("us", "so2_1h", 305.0, None),
    ("us", "so2_24h", 305.0, 201), ("us", "so2_24h", 1004.0, 500),
    ("us", "co", 4.4, 50), ("us", "co", 9.5, 101),
    ("us", "no2", 100.0, 100), ("us", "no2", 101.0, 101), ("us", "no2", 2049.0, 500),
    ("vn", "pm25", 25.0, 50), ("vn", "pm25", 37.5, 75), ("vn", "pm25", 500.0, 500),
    ("vn", "pm25", 600.0, 500), ("vn", "pm25", -1.0, 0),               # capped at 500; negatives as 0
    ("vn", "pm10", 50.0, 50), ("vn", "pm10", 150.0, 100),
    ("vn", "o3_1h", 160.0, 50), ("vn", "o3_1h", 1500.0, 500),
    ("vn", "o3_8h", 100.0, 50), ("vn", "o3_8h", 400.0, 300), ("vn", "o3_8h", 401.0, None),
    ("vn", "co", 10000.0, 50), ("vn", "no2", 100.0, 50), ("vn", "no2", 3850.0, 500), ("vn", "so2", 125.0, 50),
]
KEYS = sorted({(s, k) for s, k, _, _ in INDEX_CASES})


def column(spark, values, expr, dtype="double"):
    """Evaluate expr(col("c")) on each value; one partition keeps the input order."""
    df = spark.createDataFrame([(v,) for v in values], f"c {dtype}")
    return [r[0] for r in df.select(expr(F.col("c"))).collect()]


def test_breakpoint_tables_are_increasing(rules):
    for standard in aqi.STANDARDS:
        cfg = rules["aqi"]["us_epa" if standard == "us" else "vn"]
        for key in cfg["breakpoints" if standard == "us" else "bp"]:
            segs = aqi.segments(rules, standard, key)
            for lo, hi, ilo, ihi in segs:
                assert lo < hi and ilo < ihi, (standard, key)
            for prev, nxt in zip(segs, segs[1:]):
                assert prev[1] <= nxt[0] and prev[3] <= nxt[2], (standard, key)


@pytest.mark.parametrize("impl", ["builtin", "pandas"])
def test_index_known_values(spark, rules, impl):
    for standard, key in KEYS:
        cases = [(c, e) for s, k, c, e in INDEX_CASES if (s, k) == (standard, key)]
        got = column(spark, [c for c, _ in cases] + [None],
                     lambda c: aqi.index_col(c, rules, standard, key, impl))
        assert got == [e for _, e in cases] + [None], (standard, key)


@pytest.mark.parametrize("standard,key", KEYS)
def test_index_implementations_agree(spark, rules, standard, key):
    """builtin (CASE WHEN), pandas UDF and numpy give identical indices on a dense grid."""
    segs, top = aqi.segments(rules, standard, key), aqi.above_top(standard, key)
    edges = [x for s in segs for x in s[:2]]
    grid = sorted(set(np.round(np.linspace(-1.0, edges[-1] * 1.3, 997), 6).tolist() + edges))
    builtin = column(spark, grid, lambda c: aqi.index_col(c, rules, standard, key, "builtin"))
    pandas = column(spark, grid, lambda c: aqi.index_col(c, rules, standard, key, "pandas"))
    expected = [None if np.isnan(x) else x for x in aqi._index_numpy(grid, segs, top)]
    assert builtin == expected
    assert pandas == expected


def test_us_conc_converts_and_truncates(spark, rules):
    cases = [("pm25", 35.45, 35.4),      # truncated, not rounded: AQI 100, not 101
             ("pm10", 54.9, 54.0),
             ("o3", 0.078, 0.078),       # stored 0.0779999...; must stay 0.078
             ("o3", 0.0789, 0.078),
             ("co", 9.49, 9.4),
             ("so2", 0.0304, 30.0),      # Silver ppm -> ppb
             ("no2", 0.1, 100.0)]
    for parameter, silver, expected in cases:
        got, = column(spark, [silver], lambda c: aqi.us_conc(c, parameter, rules))
        assert got == pytest.approx(expected, abs=1e-9), parameter


def test_vn_conc_ppm_to_ugm3(spark, silver_rules):
    co, = column(spark, [1.0], lambda c: aqi.vn_conc(c, "co", silver_rules))
    assert co == pytest.approx(28.01 * 1000 / 24.45)
    pm, = column(spark, [42.0], lambda c: aqi.vn_conc(c, "pm25", silver_rules))
    assert pm == 42.0


def test_us_so2_switches_to_24h_table(spark, rules):
    rows = [(0.030, None, 43), (0.304, None, 200), (0.305, 0.100, 200), (0.305, None, 200),
            (0.305, 0.305, 201), (0.400, 0.605, 301), (None, None, None)]
    df = spark.createDataFrame([r[:2] for r in rows], "s1 double, s24 double")
    got = [r[0] for r in df.select(aqi._so2_us(F.col("s1"), F.col("s24"), rules, "builtin")).collect()]
    assert got == [r[2] for r in rows]


@pytest.mark.parametrize("values,expected", [
    ([(h, 10.0) for h in range(12)], 10.0),
    ([(0, 20.0), (1, 10.0)], 25 / 1.5),           # cmin/cmax = 0.5 -> w = 0.5
    ([(0, 10.0), (1, 8.0)], 16.4 / 1.8),          # w = cmin/cmax = 0.8
    ([(0, 20.0), (1, 2.0)], 21 / 1.5),            # cmin/cmax = 0.1 -> w floored at 0.5
    ([(0, 0.0), (1, 0.0)], 0.0),                  # cmax <= 0 -> w = 1
    ([(0, 10.0), (2, 10.0)], 10.0),               # 2 of the 3 recent hours
    ([(0, 10.0), (5, 10.0)], None),               # only 1 recent hour
    ([(3, 10.0), (4, 10.0)], None),               # no recent hour
])
def test_nowcast(spark, rules, values, expected):
    df = spark.createDataFrame([(values,)], "v array<struct<ago:int, c:double>>")
    got = df.select(aqi.nowcast(F.col("v"), rules)).first()[0]
    assert got == (None if expected is None else pytest.approx(expected))


HOURLY_INPUTS = ["pm25_nowcast", "pm10_nowcast", "o3_1h", "o3_8h", "co_1h", "co_8h", "no2_1h", "so2_1h", "so2_24h"]


@pytest.mark.parametrize("impl", ["builtin", "pandas"])
def test_hourly_overall_and_vn_pm_requirement(spark, rules, silver_rules, impl):
    def row(loc, **values):
        return (loc, *[values.get(c) for c in HOURLY_INPUTS])
    df = spark.createDataFrame(
        [row("pm_and_no2", pm25_nowcast=35.5, no2_1h=0.2), row("no2_only", no2_1h=0.2)],
        "location_id string, " + ", ".join(f"{c} double" for c in HOURLY_INPUTS))
    out = {r.location_id: r for r in aqi.hourly_aqi(df, rules, silver_rules, impl).collect()}
    # NO2 0.2 ppm = 200 ppb (US 120) = 376.4 µg/m³ (VN 118); PM2.5 35.5 µg/m³: US 101, VN 71
    a = out["pm_and_no2"]
    assert (a.aqi_us_pm25, a.aqi_us_no2, a.aqi_us, a.aqi_us_main) == (101, 120, 120, "no2")
    assert (a.aqi_vn_pm25, a.aqi_vn_no2, a.aqi_vn, a.aqi_vn_main) == (71, 118, 118, "no2")
    b = out["no2_only"]
    assert (b.aqi_us, b.aqi_us_main) == (120, "no2")
    assert (b.aqi_vn_no2, b.aqi_vn, b.aqi_vn_main) == (118, None, None)   # QĐ 1459: PM required


def test_daily_ignores_invalid_days(spark, rules, silver_rules):
    params = rules["aqi_parameters"]
    fields = [f"{p}_{k}" for p in params for k in ("mean", "max_1h", "max_8h", "valid")]
    values = {"pm25_mean": 37.5, "pm25_valid": True, "no2_max_1h": 0.2, "no2_valid": False}
    schema = "location_id string, " + ", ".join(
        f"{f} {'boolean' if f.endswith('_valid') else 'double'}" for f in fields)
    df = spark.createDataFrame([("x", *[values.get(f) for f in fields])], schema)
    r = aqi.daily_aqi(df, rules, silver_rules, "builtin").first()
    assert (r.aqi_us_pm25, r.aqi_vn_pm25) == (106, 75)
    assert (r.aqi_us_no2, r.aqi_vn_no2) == (None, None)
    assert (r.aqi_us, r.aqi_us_main, r.aqi_vn, r.aqi_vn_main) == (106, "pm25", 75, "pm25")
