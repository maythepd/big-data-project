"""Plain-Python parts of the AQI models: feature names, AQI bands, warning scores, threshold, lead times.

No Spark here: src/ml/aqi_forecast.py (Spark MLlib) and src/ml/compare_models.py (XGBoost, LSTM)
share it, and it is unit tested without a JVM (tests/test_ml_scoring.py).
"""
import pandas as pd

# upper edges of the AQI bands, the same numbers in VN_AQI (QĐ 1459) and US EPA (src/dashboard/common.BANDS)
BAND_TOPS = (50, 100, 150, 200, 300)
N_BANDS = len(BAND_TOPS) + 1
# candidate probability thresholds for the early warning
THRESHOLDS = tuple(round(0.05 * i, 2) for i in range(1, 20))
LAGS = (1, 2, 3, 6, 12, 24)
POLLUTANTS = ["pm25_1h", "pm10_1h", "o3_1h", "no2_1h", "co_1h", "so2_1h"]


def feature_cols(s, horizon):
    """Model inputs for AQI standard s ("vn" or "us"); the forecast adds the horizon and target hour."""
    cols = [f"aqi_{s}"] + [f"{s}_lag{k}" for k in LAGS] + \
           [f"{s}_{x}" for x in ("mean6", "mean24", "min24", "max24", "d3", "d24")] + \
           POLLUTANTS + ["hour_local", "dow", "lat", "lon"]
    return cols + (["h", "target_hour_local"] if horizon else [])


def band_of(aqi):
    """Band index 0..5 (Tốt/Good .. Nguy hại/Hazardous) of one AQI value."""
    return sum(aqi > top for top in BAND_TOPS)


def warn_band(warn_above):
    """First band above `warn_above`, which must be a band edge (150: Xấu/Unhealthy and worse)."""
    return BAND_TOPS.index(warn_above) + 1


def scores(tp, fp, fn):
    """Precision, recall, F1 and F2 of a yes/no warning (F2 weighs a missed episode 4x a false alarm)."""
    precision = tp / (tp + fp) if tp + fp else 0.0
    recall = tp / (tp + fn) if tp + fn else 0.0

    def f(beta):
        b2 = beta * beta
        d = b2 * precision + recall
        return (1 + b2) * precision * recall / d if d else 0.0
    return dict(tp=tp, fp=fp, fn=fn, precision=precision, recall=recall, f1=f(1), f2=f(2))


def best_threshold(counts):
    """counts {threshold: (tp, fp, fn)} on the validation window: the threshold with the best F2
    (an early warning that misses an episode costs more than one that cries wolf); ties go to the
    higher threshold, which raises fewer false alarms."""
    return max(counts, key=lambda t: (scores(*counts[t])["f2"], t))


def class_scores(matrix):
    """Accuracy and macro F1 of a band classifier from its confusion matrix [true][predicted];
    the macro average covers the bands that occur in the truth."""
    n = sum(map(sum, matrix))
    correct = sum(matrix[i][i] for i in range(len(matrix)))
    f1s = []
    for k in range(len(matrix)):
        true_k = sum(matrix[k])
        if not true_k:
            continue
        pred_k = sum(row[k] for row in matrix)
        p = matrix[k][k] / pred_k if pred_k else 0.0
        r = matrix[k][k] / true_k
        f1s.append(2 * p * r / (p + r) if p + r else 0.0)
    return dict(n=n, accuracy=correct / n if n else 0.0, macro_f1=sum(f1s) / len(f1s) if f1s else 0.0)


def lead_times(df, warn_above, start, horizon=24):
    """Pollution episodes in an hourly warning backtest, and how early each one was warned.

    df: one row per station-hour (gaps allowed) with location_id, hour_utc, aqi, warn (bool).
    An episode starts at an hour T above `warn_above` when the hour before (exactly T - 1 h) was not
    above it or is missing. Only onsets from start + horizon count, so the whole window in which a
    warning could have been raised lies inside the backtest. The episode was warned when some hour in
    [T - horizon, T - 1 h] raised a warning; the lead time is T minus the earliest of those, in hours.
    """
    rows = []
    hour = pd.Timedelta(hours=1)
    first = pd.Timestamp(start) + horizon * hour
    for loc, g in df.sort_values("hour_utc").groupby("location_id"):
        above = (g.set_index("hour_utc")["aqi"] > warn_above).to_dict()
        warned_at = g.loc[g["warn"].astype(bool), "hour_utc"]
        for t, is_above in above.items():
            if not is_above or t < first or above.get(t - hour, False):
                continue
            w = warned_at[(warned_at >= t - horizon * hour) & (warned_at < t)]
            rows.append(dict(location_id=loc, onset_utc=t, warned=not w.empty,
                             lead_h=(t - w.min()) / hour if not w.empty else None))
    return pd.DataFrame(rows, columns=["location_id", "onset_utc", "warned", "lead_h"])


def lead_summary(episodes):
    warned = episodes[episodes["warned"]]
    return dict(episodes=len(episodes), warned=len(warned),
                recall=len(warned) / len(episodes) if len(episodes) else None,
                median_lead_h=float(warned["lead_h"].median()) if len(warned) else None)
