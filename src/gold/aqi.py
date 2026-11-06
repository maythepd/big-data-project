"""US EPA and VN_AQI sub-indices and PM NowCast (breakpoints: config/gold_rules_v1.yaml).

Sources: EPA-403/B-26-003 (May 2026) and QĐ 1459/QĐ-TCMT (2019).
Two equal implementations: index_builtin (CASE WHEN) and index_pandas (pandas UDF).
Inputs in Silver units (PM µg/m³, gases ppm); negatives count as 0.
"""

import numpy as np
import pandas as pd
from pyspark.sql import functions as F

STANDARDS = ["us", "vn"]


# ---------- breakpoint segments ----------

def segments(rules, standard, key):
    """[(conc_lo, conc_hi, aqi_lo, aqi_hi)] for one standard and breakpoint key."""
    cfg = rules["aqi"]["us_epa" if standard == "us" else "vn"]
    if standard == "us":
        return [tuple(float(x) for x in s) for s in cfg["breakpoints"][key]]
    bp, levels = cfg["bp"][key], cfg["levels"]
    return [(float(bp[i]), float(bp[i + 1]), float(levels[i]), float(levels[i + 1])) for i in range(len(bp) - 1)]


# above the last segment: "extrapolate" (EPA), "cap" at 500 (VN) or None (no index)
def above_top(standard, key):
    if standard == "us":
        return None if key in ("o3_8h", "so2_1h") else "extrapolate"
    return None if key == "o3_8h" else "cap"


def _index_numpy(c, segs, top):
    c = np.asarray(c, dtype="float64")
    lo = np.array([s[0] for s in segs]); hi = np.array([s[1] for s in segs])
    ilo = np.array([s[2] for s in segs]); ihi = np.array([s[3] for s in segs])
    first = lo[0]
    x = np.where(c < 0, 0.0, c) if first == 0 else c
    # segment = last one whose lower bound <= x
    k = np.clip(np.searchsorted(lo, x, side="right") - 1, 0, len(segs) - 1)
    out = (ihi[k] - ilo[k]) / (hi[k] - lo[k]) * (x - lo[k]) + ilo[k]
    out = np.where(x < first, np.nan, out)
    above = x > hi[-1]
    if top == "cap":
        out = np.where(above, ihi[-1], out)
    elif top is None:
        out = np.where(above, np.nan, out)
    # round half up like Spark's F.round; floor(x + 0.5) fails at 0.49999999999999994
    f = np.floor(out)
    out = f + ((out - f) >= 0.5)                    # out - floor(out) is exact
    return np.where(np.isnan(c), np.nan, out)


def index_pandas(segs, top):
    @F.pandas_udf("double")
    def _index(c: pd.Series) -> pd.Series:
        return pd.Series(_index_numpy(c.to_numpy(dtype="float64", na_value=np.nan), segs, top))
    return _index


def index_builtin(col, segs, top):
    first = segs[0][0]
    x = F.when(col < 0, F.lit(0.0)).otherwise(col) if first == 0 else col
    # first match wins: test the top segment first, like searchsorted
    expr = None
    for lo, hi, ilo, ihi in reversed(segs):
        value = F.lit((ihi - ilo) / (hi - lo)) * (x - F.lit(lo)) + F.lit(ilo)
        expr = F.when(x >= F.lit(lo), value) if expr is None else expr.when(x >= F.lit(lo), value)
    last_hi, last_top = segs[-1][1], segs[-1][3]
    if top == "cap":
        expr = F.when(x > F.lit(last_hi), F.lit(last_top)).otherwise(expr)
    elif top is None:
        expr = F.when(x > F.lit(last_hi), F.lit(None).cast("double")).otherwise(expr)
    return F.when(col.isNotNull(), F.round(expr, 0))


def index_col(col, rules, standard, key, impl):
    segs, top = segments(rules, standard, key), above_top(standard, key)
    if impl == "pandas":
        return index_pandas(segs, top)(col)
    return index_builtin(col, segs, top)


# ---------- unit handling ----------

def truncate(col, decimals):
    """EPA truncation; the epsilon keeps 0.078 (stored 0.07799999...) at 0.078."""
    scale = 10.0 ** decimals
    return F.floor(col * F.lit(scale) + F.lit(1e-9)) / F.lit(scale)


def us_conc(col, parameter, rules):
    unit = rules["aqi"]["us_epa"]["units"][parameter]
    c = col * F.lit(1000.0) if unit == "ppb" else col           # Silver gases are ppm
    return truncate(c, rules["aqi"]["us_epa"]["truncate_decimals"][parameter])


def vn_conc(col, parameter, silver_rules):
    if parameter in ("pm25", "pm10"):
        return col
    mw = silver_rules["molecular_weight"][parameter]
    return col * F.lit(mw * 1000.0 / silver_rules["molar_volume_l"])


# ---------- PM NowCast ----------

def nowcast(values, rules):
    """PM NowCast over the last `hours` hours (array<struct<ago, c>>); null if too few recent hours."""
    nc = rules["nowcast"]
    cs = F.transform(values, lambda v: v["c"])
    cmin, cmax = F.array_min(cs), F.array_max(cs)
    w = F.when(cmax <= 0, F.lit(1.0)).otherwise(F.greatest(cmin / cmax, F.lit(nc["min_weight"])))
    num = F.aggregate(values, F.lit(0.0), lambda acc, v: acc + F.pow(w, v["ago"]) * v["c"])
    den = F.aggregate(values, F.lit(0.0), lambda acc, v: acc + F.pow(w, v["ago"]))
    recent = F.size(F.filter(values, lambda v: v["ago"] < nc["recent_hours"]))
    return F.when(recent >= nc["recent_min"], num / den)


# ---------- hourly and daily AQI ----------

def _overall(df, prefix, pollutants, require_pm=False):
    entries = F.filter(F.array(*[F.struct(F.col(f"{prefix}_{p}").alias("i"), F.lit(p).alias("p"))
                                 for p in pollutants]), lambda e: e["i"].isNotNull())
    best = F.array_max(entries)
    out = df.withColumn(prefix, best["i"].cast("int")).withColumn(f"{prefix}_main", best["p"])
    if require_pm:   # QĐ 1459 2.1.b: at least one of PM10, PM2.5 is required
        has_pm = F.col(f"{prefix}_pm25").isNotNull() | F.col(f"{prefix}_pm10").isNotNull()
        out = (out.withColumn(prefix, F.when(has_pm, F.col(prefix)))
                  .withColumn(f"{prefix}_main", F.when(has_pm, F.col(f"{prefix}_main"))))
    return out


def _so2_us(c1h, c24h, rules, impl):
    """EPA SO2: 1-hour table below 305 ppb, else the 24-hour rule."""
    s1, s24 = us_conc(c1h, "so2", rules), us_conc(c24h, "so2", rules)
    return (F.when(s1 < 305, index_col(s1, rules, "us", "so2_1h", impl))
             .when(s24 >= 305, index_col(s24, rules, "us", "so2_24h", impl))
             .when(s1.isNotNull(), F.lit(200.0)))


def hourly_aqi(df, rules, silver_rules, impl):
    """Hourly US and VN sub-indices and overall AQI for one row per location-hour."""
    c = F.col
    us = {
        "pm25": index_col(us_conc(c("pm25_nowcast"), "pm25", rules), rules, "us", "pm25", impl),
        "pm10": index_col(us_conc(c("pm10_nowcast"), "pm10", rules), rules, "us", "pm10", impl),
        "o3": F.greatest(index_col(us_conc(c("o3_8h"), "o3", rules), rules, "us", "o3_8h", impl),
                         index_col(us_conc(c("o3_1h"), "o3", rules), rules, "us", "o3_1h", impl)),
        "co": index_col(us_conc(c("co_8h"), "co", rules), rules, "us", "co", impl),
        "no2": index_col(us_conc(c("no2_1h"), "no2", rules), rules, "us", "no2", impl),
        "so2": _so2_us(c("so2_1h"), c("so2_24h"), rules, impl),
    }
    vn = {
        "pm25": index_col(c("pm25_nowcast"), rules, "vn", "pm25", impl),
        "pm10": index_col(c("pm10_nowcast"), rules, "vn", "pm10", impl),
        "o3": index_col(vn_conc(c("o3_1h"), "o3", silver_rules), rules, "vn", "o3_1h", impl),
        "co": index_col(vn_conc(c("co_1h"), "co", silver_rules), rules, "vn", "co", impl),
        "no2": index_col(vn_conc(c("no2_1h"), "no2", silver_rules), rules, "vn", "no2", impl),
        "so2": index_col(vn_conc(c("so2_1h"), "so2", silver_rules), rules, "vn", "so2", impl),
    }
    for p, e in us.items():
        df = df.withColumn(f"aqi_us_{p}", e.cast("int"))
    for p, e in vn.items():
        df = df.withColumn(f"aqi_vn_{p}", e.cast("int"))
    df = _overall(df, "aqi_us", list(us))
    return _overall(df, "aqi_vn", list(vn), require_pm=True)


def daily_aqi(df, rules, silver_rules, impl):
    """Daily US and VN AQI for one row per location-day (valid days only)."""
    c = F.col
    v = lambda p, e: F.when(c(f"{p}_valid"), e)
    o3_8h_vn = vn_conc(c("o3_max_8h"), "o3", silver_rules)
    us = {
        "pm25": index_col(us_conc(c("pm25_mean"), "pm25", rules), rules, "us", "pm25", impl),
        "pm10": index_col(us_conc(c("pm10_mean"), "pm10", rules), rules, "us", "pm10", impl),
        "o3": F.greatest(index_col(us_conc(c("o3_max_8h"), "o3", rules), rules, "us", "o3_8h", impl),
                         index_col(us_conc(c("o3_max_1h"), "o3", rules), rules, "us", "o3_1h", impl)),
        "co": index_col(us_conc(c("co_max_8h"), "co", rules), rules, "us", "co", impl),
        "no2": index_col(us_conc(c("no2_max_1h"), "no2", rules), rules, "us", "no2", impl),
        "so2": _so2_us(c("so2_max_1h"), c("so2_mean"), rules, impl),
    }
    vn = {
        "pm25": index_col(c("pm25_mean"), rules, "vn", "pm25", impl),
        "pm10": index_col(c("pm10_mean"), rules, "vn", "pm10", impl),
        # VN O3: max of the 1-hour and 8-hour indices
        "o3": F.greatest(index_col(vn_conc(c("o3_max_1h"), "o3", silver_rules), rules, "vn", "o3_1h", impl),
                         index_col(o3_8h_vn, rules, "vn", "o3_8h", impl)),
        "co": index_col(vn_conc(c("co_max_1h"), "co", silver_rules), rules, "vn", "co", impl),
        "no2": index_col(vn_conc(c("no2_max_1h"), "no2", silver_rules), rules, "vn", "no2", impl),
        "so2": index_col(vn_conc(c("so2_max_1h"), "so2", silver_rules), rules, "vn", "so2", impl),
    }
    for p, e in us.items():
        df = df.withColumn(f"aqi_us_{p}", v(p, e).cast("int"))
    for p, e in vn.items():
        df = df.withColumn(f"aqi_vn_{p}", v(p, e).cast("int"))
    df = _overall(df, "aqi_us", list(us))
    return _overall(df, "aqi_vn", list(vn), require_pm=True)
