"""AQI forecasting and early warning with Spark MLlib, trained on Gold station_hourly_aqi.

Two models per AQI standard (VN_AQI and US EPA), trained on every station in the lake (Vietnam and
the rest of the world: Vietnam alone has too few station-hours) and scored on Vietnam separately:

  forecast  GBTRegressor: the AQI h = 1..24 hours ahead. One model covers every horizon (h is a
            feature); it learns the change from the current AQI, which is added back.
  risk      RandomForestClassifier: the AQI band of the worst hour in the next 24 h (6 bands).
            Early warning = P(that band is above --warn-above, 150 = Xấu/Unhealthy) >= a threshold
            picked on a validation window by F2 (missing an episode costs more than a false alarm).

Features, per station-hour, from the past only: AQI now; 1, 2, 3, 6, 12 and 24 h ago; mean of the
last 6 h and 24 h; min and max of the last 24 h; change over 3 h and 24 h; the hourly
concentrations; local hour (target hour too, for the forecast), weekday, latitude, longitude. Lags
use range frames on epoch seconds, so a missing hour stays missing (then filled with the current
value) instead of borrowing an older row.

Split by time, never by row: train | validation (--val-days) | test (last --test-days). Training
anchors end 24 h before the validation window, so no label reaches into it. Baselines on the
same test rows: persistence (AQI stays as now), the mean of the last 24 h, and for the warning
"the last 24 h already went above the line".

Outputs, openaq_lake/gold/ml/<run>/ (std=vn|us partitions):
  forecast          VN stations, the next 24 h from each station's last hour, with a 10-90% band
                    from the test residuals at that horizon
  warning           VN stations, band probabilities for the next 24 h and the warning
  backtest          VN test hours: actual vs predicted 1, 6 and 24 h ahead
  warning_backtest  VN test hours: warning probability and the AQI that followed
  recent            VN stations, the 48 h before their forecast
  features          the feature table (every station-hour), for src/ml/compare_models.py
  metrics.json      data sizes, split, error by horizon, warning scores, lead times, importances
Models: openaq_lake/models/<run>/<std>_{forecast,risk}. MongoDB: python -m src.serving.load_ml.

    docker compose -f docker-compose.spark.yml run --rm spark-lake \\
        /opt/spark/bin/spark-submit --driver-memory 4g src/ml/aqi_forecast.py
"""
import argparse
from datetime import datetime, timezone
from functools import reduce
import json
from operator import add
from pathlib import Path
import sys
import time

from pyspark import StorageLevel
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.functions import vector_to_array
from pyspark.ml.regression import GBTRegressor
from pyspark.sql import SparkSession, Window, functions as F

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import LAKE  # noqa: E402
from src.ml.scoring import (BAND_TOPS, LAGS, N_BANDS, POLLUTANTS, THRESHOLDS, best_threshold,  # noqa: E402
                            class_scores, feature_cols, lead_summary, lead_times, scores, warn_band)

HOUR, DAY = 3600, 86400
STDS = ("vn", "us")
ALL_H = list(range(1, 25))
BACKTEST_H = (1, 6, 24)
MIN_LABEL_HOURS = 18          # the next-24-h label needs at least this many of those hours


def uri(path):
    return "file://" + str(path)


def band_col(c):
    return reduce(add, [(c > top).cast("int") for top in BAND_TOPS])


def with_features(df):
    """Station-hour rows with past-only features and next-24-h labels, both standards."""
    w = Window.partitionBy("location_id").orderBy("ts")
    out = (df.withColumn("ts", F.col("hour_utc").cast("long"))
             .fillna(0.0, subset=["lat", "lon"])
             # local solar hour from longitude: Gold keeps the UTC offset per hour, not per AQI row
             .withColumn("hour_local", F.pmod(F.hour("hour_utc") + F.round(F.col("lon") / 15).cast("int"), F.lit(24)))
             .withColumn("dow", F.dayofweek("hour_utc")))
    for s in STDS:
        a = F.col(f"aqi_{s}")
        cols = {f"{s}_lag{k}": F.coalesce(F.max(a).over(w.rangeBetween(-k * HOUR, -k * HOUR)), a) for k in LAGS}
        cols |= {f"{s}_mean6": F.avg(a).over(w.rangeBetween(-5 * HOUR, 0)),
                 f"{s}_mean24": F.avg(a).over(w.rangeBetween(-23 * HOUR, 0)),
                 f"{s}_min24": F.min(a).over(w.rangeBetween(-23 * HOUR, 0)),
                 f"{s}_max24": F.max(a).over(w.rangeBetween(-23 * HOUR, 0)),
                 f"{s}_next24_max": F.max(a).over(w.rangeBetween(HOUR, 24 * HOUR)),
                 f"{s}_next24_n": F.count(a).over(w.rangeBetween(HOUR, 24 * HOUR))}
        out = out.withColumns(cols)
        out = out.withColumns({f"{s}_d3": a - F.col(f"{s}_lag3"), f"{s}_d24": a - F.col(f"{s}_lag24")})
    return out.fillna(-1.0, subset=POLLUTANTS)


def split_bounds(t_end, test_days, val_days):
    """(validation start, test start), epoch seconds: the test is the last test_days * 24 hours."""
    test_start = t_end - test_days * DAY + HOUR
    return test_start - val_days * DAY, test_start


def windows(feats, val_start, test_start):
    """Train, validation and test anchors; a training label (up to 24 h ahead) ends before validation."""
    train = feats.filter(F.col("ts") + 24 * HOUR < val_start)
    val = feats.filter((F.col("ts") >= val_start) & (F.col("ts") + 24 * HOUR < test_start))
    test = feats.filter(F.col("ts") >= test_start)
    return train, val, test


def sample(df, frac, seed):
    """Every Vietnam row, a fraction of the others (the rest of the world is ~99.8% of the rows)."""
    return df.filter(F.col("is_vn") | (F.rand(seed) < frac))


def horizon_rows(anchors, feats, s, horizons):
    """One row per (anchor, h) with the AQI h hours later (`y_aqi`) and the change to learn (`y`).
    Joined on (station, exact hour), so an anchor whose target hour is missing drops out."""
    target = (feats.filter(F.col(f"aqi_{s}").isNotNull())
                   .select("location_id", F.col("ts").alias("t_ts"), F.col(f"aqi_{s}").alias("y_aqi")))
    return (anchors.withColumn("h", F.explode(horizons))
                   .withColumn("t_ts", F.col("ts") + F.col("h") * HOUR)
                   .join(target, ["location_id", "t_ts"])
                   .withColumn("target_hour_local", F.pmod(F.col("hour_local") + F.col("h"), F.lit(24)))
                   .withColumn("y", F.col("y_aqi") - F.col(f"aqi_{s}")))


def random_horizons(n, seed):
    return F.array(*[(F.floor(F.rand(seed + i) * 24) + 1).cast("int") for i in range(n)])


def every_horizon(hs=ALL_H):
    return F.array(*[F.lit(h) for h in hs])


def latest_per_station(feats, s):
    w = Window.partitionBy("location_id").orderBy(F.col("ts").desc())
    return (feats.filter(F.col("is_vn") & F.col(f"aqi_{s}").isNotNull())
                 .withColumn("rn", F.row_number().over(w)).filter("rn = 1").drop("rn"))


def counts(hit, y):
    return [F.sum((hit & y).cast("int")), F.sum((hit & ~y).cast("int")), F.sum((~hit & y).cast("int"))]


def scopes(df):
    return [("all", df), ("vn", df.filter("is_vn"))]


def warn_counts(df, p, y):
    """{scope: {threshold: (tp, fp, fn)}} for the warning `p >= threshold` against the truth `y`,
    over every station ("all") and Vietnam ("vn"), one pass per scope."""
    out = {}
    for scope, part in scopes(df):
        row = part.agg(*[e for t in THRESHOLDS for e in counts(p >= t, y)]).first()
        vals = [v or 0 for v in row]
        out[scope] = {t: tuple(vals[3 * i:3 * i + 3]) for i, t in enumerate(THRESHOLDS)}
    return out


def rule_counts(df, rule, y):
    """{scope: (tp, fp, fn)} of a fixed yes/no rule."""
    return {scope: tuple(v or 0 for v in part.agg(*counts(rule, y)).first()) for scope, part in scopes(df)}


def write(df, path):
    df.coalesce(1).write.mode("overwrite").parquet(uri(path))


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run-id", default=datetime.now(timezone.utc).strftime("ml_%Y%m%d_%H%M"))
    p.add_argument("--test-days", type=int, default=7)
    p.add_argument("--val-days", type=int, default=3)
    p.add_argument("--sample", type=float, default=0.1, help="share of non-VN training anchors")
    p.add_argument("--test-sample", type=float, default=0.05, help="share of non-VN test anchors")
    p.add_argument("--horizons-per-anchor", type=int, default=3, help="random horizons per training anchor")
    p.add_argument("--trees", type=int, default=60, help="GBT iterations (forecast)")
    p.add_argument("--forest", type=int, default=40, help="random forest trees (risk)")
    p.add_argument("--warn-above", type=int, default=150, choices=BAND_TOPS)
    args = p.parse_args()

    out_dir = LAKE / "gold" / "ml" / args.run_id
    model_dir = LAKE / "models" / args.run_id
    features = out_dir / "features"      # kept: src/ml/compare_models.py trains on the same table
    spark = (SparkSession.builder.appName("OpenAQ-ML-AqiForecast")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "32")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    t0 = time.time()

    # ---- features: one pass of window functions over every station, written once, read many times
    src = (spark.read.parquet(uri(LAKE / "gold" / "station_hourly_aqi"))
                .select("location_id", "hour_utc", "lat", "lon", "is_vn", "aqi_vn", "aqi_us", *POLLUTANTS)
                .filter(F.col("aqi_vn").isNotNull() | F.col("aqi_us").isNotNull()))
    write_t = time.time()
    with_features(src).repartition(32, "location_id").write.mode("overwrite").parquet(uri(features))
    feats = spark.read.parquet(uri(features))
    info = feats.agg(F.count("*").alias("rows"), F.countDistinct("location_id").alias("stations"),
                     F.countDistinct(F.when(F.col("is_vn"), F.col("location_id"))).alias("vn_stations"),
                     F.min("ts").alias("first"), F.max("ts").alias("last")).first().asDict()
    val_start, test_start = split_bounds(info["last"], args.test_days, args.val_days)
    train_a, val_a, test_a = windows(feats, val_start, test_start)
    iso = lambda t: datetime.fromtimestamp(t, timezone.utc).strftime("%Y-%m-%dT%H:%MZ")  # noqa: E731
    metrics = dict(run_id=args.run_id, created_utc=iso(time.time()), params=vars(args),
                   data=dict(rows=info["rows"], stations=info["stations"], vn_stations=info["vn_stations"],
                             first_hour=iso(info["first"]), last_hour=iso(info["last"]),
                             features_seconds=round(time.time() - write_t)),
                   split=dict(train_until=iso(val_start - 24 * HOUR), val_start=iso(val_start),
                              test_start=iso(test_start), test_end=iso(info["last"])),
                   std={})
    print(f"features: {info['rows']:,} station-hours, {info['stations']:,} stations "
          f"({info['vn_stations']} VN), {metrics['data']['first_hour']} .. {metrics['data']['last_hour']}", flush=True)
    wb = warn_band(args.warn_above)

    for s in STDS:
        aqi = f"aqi_{s}"
        m = metrics["std"][s] = {}
        has = F.col(aqi).isNotNull()

        # ---- forecast: GBT on (anchor, random horizon) rows, predicting the change
        fa = VectorAssembler(inputCols=feature_cols(s, True), outputCol="features", handleInvalid="skip")
        train = fa.transform(horizon_rows(sample(train_a.filter(has), args.sample, 11), feats, s,
                                          random_horizons(args.horizons_per_anchor, 21))).select("features", "y").persist()
        n_train = train.count()
        t = time.time()
        gbt = GBTRegressor(labelCol="y", maxIter=args.trees, maxDepth=6, stepSize=0.1, subsamplingRate=0.8,
                           maxBins=64, seed=7).fit(train)
        fit_s = time.time() - t
        train.unpersist()
        gbt.write().overwrite().save(uri(model_dir / f"{s}_forecast"))
        print(f"[{s}] forecast: {n_train:,} training rows, {fit_s:.0f} s", flush=True)

        pred = F.greatest(F.lit(0.0), F.col(aqi) + F.col("prediction"))
        test = (gbt.transform(fa.transform(horizon_rows(sample(test_a.filter(has), args.test_sample, 12), feats, s,
                                                         every_horizon())))
                   .withColumn("pred", pred)
                   # only what the scores need: vectors and features of ~7M rows overran a 4 GB driver
                   .select("location_id", "is_vn", "h", "t_ts", "y_aqi", "pred", aqi, f"{s}_mean24")
                   .persist(StorageLevel.MEMORY_AND_DISK))
        err = F.col("pred") - F.col("y_aqi")
        aggs = [F.count("*").alias("n"), F.avg(F.abs(err)).alias("mae"), F.sqrt(F.avg(err * err)).alias("rmse"),
                F.avg(F.abs(F.col(aqi) - F.col("y_aqi"))).alias("mae_persistence"),
                F.avg(F.abs(F.col(f"{s}_mean24") - F.col("y_aqi"))).alias("mae_mean24"),
                F.percentile_approx(F.col("y_aqi") - F.col("pred"), [0.1, 0.9], 1000).alias("q")]
        by_h = [dict(scope="all", **r.asDict()) for r in test.groupBy("h").agg(*aggs).collect()] + \
               [dict(scope="vn", **r.asDict()) for r in test.filter("is_vn").groupBy("h").agg(*aggs).collect()]
        for r in by_h:
            r["q10"], r["q90"] = r.pop("q") or (None, None)
        by_h.sort(key=lambda r: (r["scope"], r["h"]))
        m["forecast"] = dict(train_rows=n_train, fit_seconds=round(fit_s), by_h=by_h,
                             importances=sorted(zip(feature_cols(s, True), gbt.featureImportances.toArray().tolist()),
                                                key=lambda kv: -kv[1]))
        write(test.filter(F.col("is_vn") & F.col("h").isin(*BACKTEST_H))
                  .select("location_id", "h", F.col("t_ts").cast("timestamp").alias("target_utc"),
                          F.col("y_aqi").alias("actual"), "pred", F.col(aqi).alias("persistence")),
              out_dir / "backtest" / f"std={s}")
        test.unpersist()

        # next 24 h from each VN station's last hour, with the VN (else all-station) residual band
        band = {}
        for r in by_h:
            if r["q10"] is not None and (r["scope"] == "vn" and r["n"] >= 100 or r["h"] not in band):
                band[r["h"]] = (r["q10"], r["q90"])
        qdf = spark.createDataFrame([(h, *band.get(h, (0.0, 0.0))) for h in ALL_H], "h int, q10 double, q90 double")
        last = latest_per_station(feats, s)
        # the 48 h before each forecast, so the page can draw it whatever MongoDB's hourly table holds
        recent = (feats.filter(F.col("is_vn") & has)
                       .join(last.select("location_id", F.col("ts").alias("last_ts")), "location_id")
                       .filter(F.col("ts") > F.col("last_ts") - 48 * HOUR))
        write(recent.select("location_id", F.col("ts").cast("timestamp").alias("hour_utc"), F.col(aqi).alias("aqi")),
              out_dir / "recent" / f"std={s}")
        fc = (last.withColumn("h", F.explode(every_horizon()))
                  .withColumn("t_ts", F.col("ts") + F.col("h") * HOUR)
                  .withColumn("target_hour_local", F.pmod(F.col("hour_local") + F.col("h"), F.lit(24))))
        fc = gbt.transform(fa.transform(fc)).withColumn("pred", pred).join(qdf, "h")
        write(fc.select("location_id", F.col("ts").cast("timestamp").alias("issued_utc"), "h",
                        F.col("t_ts").cast("timestamp").alias("target_utc"), F.col(aqi).alias("aqi_now"), "pred",
                        F.greatest(F.lit(0.0), F.col("pred") + F.col("q10")).alias("lo"),
                        (F.col("pred") + F.col("q90")).alias("hi")),
              out_dir / "forecast" / f"std={s}")

        # ---- risk: random forest on the band of the worst hour in the next 24 h
        ra = VectorAssembler(inputCols=feature_cols(s, False), outputCol="features", handleInvalid="skip")
        labelled = (F.col(f"{s}_next24_n") >= MIN_LABEL_HOURS) & has
        label = band_col(F.col(f"{s}_next24_max")).cast("double")
        train = (ra.transform(sample(train_a.filter(labelled), args.sample, 13).withColumn("label", label))
                   .select("features", "label").persist())
        n_train = train.count()
        t = time.time()
        rf = RandomForestClassifier(maxDepth=10, numTrees=args.forest, maxBins=64, subsamplingRate=0.8,
                                    seed=7).fit(train)
        fit_s = time.time() - t
        train.unpersist()
        rf.write().overwrite().save(uri(model_dir / f"{s}_risk"))
        print(f"[{s}] risk: {n_train:,} training rows, {fit_s:.0f} s", flush=True)

        probs = vector_to_array("probability")
        p_warn = F.aggregate(F.slice(probs, wb + 1, N_BANDS), F.lit(0.0), lambda acc, x: acc + x)
        score = lambda df: rf.transform(ra.transform(df)).withColumn("p_warn", p_warn)  # noqa: E731
        y = F.col(f"{s}_next24_max") > args.warn_above
        val = score(sample(val_a.filter(labelled), args.test_sample, 14))
        vc = warn_counts(val, F.col("p_warn"), y)
        # Vietnam's own validation hours when they hold enough episodes, else every station's
        tp, _, fn = vc["vn"][THRESHOLDS[0]]
        thr_scope = "vn" if tp + fn >= 30 else "all"
        thr = best_threshold(vc[thr_scope])

        test = (score(sample(test_a.filter(has), args.test_sample, 15)).withColumn("label", label)
                .select("location_id", "ts", "is_vn", aqi, f"{s}_max24", f"{s}_next24_max", f"{s}_next24_n",
                        "label", "prediction", "p_warn")
                .persist(StorageLevel.MEMORY_AND_DISK))
        tl = test.filter(labelled)
        curve = warn_counts(tl, F.col("p_warn"), y)
        base = rule_counts(tl, F.col(f"{s}_max24") > args.warn_above, y)
        conf = {}
        for r in tl.groupBy("is_vn", "label", "prediction").count().collect():
            for scope in ("all", "vn") if r["is_vn"] else ("all",):
                mat = conf.setdefault(scope, [[0] * N_BANDS for _ in range(N_BANDS)])
                mat[int(r["label"])][int(r["prediction"])] += r["count"]
        m["risk"] = dict(train_rows=n_train, fit_seconds=round(fit_s), warn_above=args.warn_above, threshold=thr, threshold_scope=thr_scope,
                         confusion=conf, bands={k: class_scores(v) for k, v in conf.items()},
                         warning={k: scores(*curve[k][thr]) for k in curve},
                         baseline={k: scores(*base[k]) for k in base},
                         curve={k: [dict(threshold=t, **scores(*c)) for t, c in curve[k].items()] for k in curve},
                         importances=sorted(zip(feature_cols(s, False), rf.featureImportances.toArray().tolist()),
                                            key=lambda kv: -kv[1]))

        vn = (test.filter("is_vn")
                  .select("location_id", F.col("ts").cast("timestamp").alias("hour_utc"), F.col(aqi).alias("aqi"),
                          "p_warn", (F.col("p_warn") >= thr).alias("warn"),
                          (F.col(f"{s}_max24") > args.warn_above).alias("warn_baseline"),
                          F.when(labelled, F.col(f"{s}_next24_max")).alias("next24_max")))
        write(vn, out_dir / "warning_backtest" / f"std={s}")
        bt = vn.toPandas()
        start = datetime.fromtimestamp(test_start, timezone.utc).replace(tzinfo=None)
        m["risk"]["lead"] = {"model": lead_summary(lead_times(bt, args.warn_above, start)),
                             "baseline": lead_summary(lead_times(bt.assign(warn=bt["warn_baseline"]),
                                                                 args.warn_above, start))}
        test.unpersist()

        now = score(latest_per_station(feats, s))
        write(now.select("location_id", F.col("ts").cast("timestamp").alias("issued_utc"), F.col(aqi).alias("aqi_now"),
                         F.col(f"{s}_max24").alias("max24"), probs.alias("band_probs"), "p_warn",
                         (F.col("p_warn") >= thr).alias("warn")),
              out_dir / "warning" / f"std={s}")
        r = m["risk"]
        print(f"[{s}] warning threshold {thr}: VN {r['warning'].get('vn', {})}", flush=True)

    metrics["seconds"] = round(time.time() - t0)
    (out_dir / "metrics.json").write_text(json.dumps(metrics, indent=2, default=str), encoding="utf-8")
    spark.stop()
    print(f"done in {metrics['seconds']} s: {out_dir}", flush=True)


if __name__ == "__main__":
    main()
