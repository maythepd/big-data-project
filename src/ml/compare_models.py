"""Compare the Spark MLlib models with XGBoost and a light LSTM, on the same data, features and split.

Reads a finished src/ml/aqi_forecast.py run (<lake>/gold/ml/<run>/features and metrics.json) and
trains, on the GPU when XGBoost and PyTorch see one:
  XGBoost forecast  the Spark GBT's inputs: (hour, horizon) rows -> change in AQI h hours later
  XGBoost warning   binary: is the worst hour of the next 24 h above warn_above? The probability
                    threshold is picked by F2 on the validation window, as for the random forest
  LSTM              the last 24 hours of a station in, the next 24 hours out in one pass; its
                    warning is "the highest of the 24 predictions is above warn_above"
Same validation and test windows and the same share of non-VN stations as the Spark run; every
model is scored on Vietnam's test hours, next to the Spark numbers from metrics.json.
Output: <run>/comparison.json (into ml_metrics by src/serving/load_ml.py).

    LAKE_DIR=openaq_lake_hist python -m src.ml.compare_models            # the newest run
"""
import argparse
from datetime import datetime, timezone
import json
from pathlib import Path
import sys
import time
import zlib

import warnings

import numpy as np
import pandas as pd
import pyarrow.dataset as ds

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from src.common.config import LAKE  # noqa: E402
from src.ml.scoring import (POLLUTANTS, THRESHOLDS, best_threshold, feature_cols, lead_summary,  # noqa: E402
                            lead_times, scores)

HOUR = 3600
warnings.filterwarnings("ignore", message=".*mismatched devices.*")   # GPU model, CPU input: fine here
H = 24
MIN_LABEL_HOURS = 18
STDS = ("vn", "us")


def epoch(iso):
    return int(datetime.strptime(iso, "%Y-%m-%dT%H:%MZ").replace(tzinfo=timezone.utc).timestamp())


def load(run_dir, s):
    cols = ["location_id", "ts", "is_vn", f"{s}_next24_max", f"{s}_next24_n"] + feature_cols(s, False)
    df = ds.dataset(run_dir / "features", format="parquet").to_table(columns=cols).to_pandas()
    df = df[df[f"aqi_{s}"].notna()].reset_index(drop=True)
    for c in feature_cols(s, False):
        df[c] = df[c].astype("float32")
    return df


def horizon_rows(anchors, targets, s, hs):
    """(anchor, h) rows joined to the AQI exactly h hours later; hs: one array of horizons per anchor."""
    k = hs.shape[1]
    rows = anchors.loc[anchors.index.repeat(k)].copy()
    rows["h"] = hs.ravel().astype("float32")
    rows["t_ts"] = rows["ts"] + rows["h"].astype("int64") * HOUR
    rows = rows.merge(targets, on=["location_id", "t_ts"])
    rows["target_hour_local"] = ((rows["hour_local"] + rows["h"]) % 24).astype("float32")
    rows["y"] = rows["y_aqi"] - rows[f"aqi_{s}"]
    return rows


def warn_counts(p, y, vn):
    """{scope: {threshold: (tp, fp, fn)}} as in aqi_forecast.warn_counts."""
    out = {}
    for scope, m in (("all", np.ones_like(vn, bool)), ("vn", vn)):
        out[scope] = {t: (int(((p >= t) & y & m).sum()), int(((p >= t) & ~y & m).sum()),
                          int(((p < t) & y & m).sum())) for t in THRESHOLDS}
    return out


def pick_threshold(counts):
    tp, _, fn = counts["vn"][THRESHOLDS[0]]
    scope = "vn" if tp + fn >= 30 else "all"
    return best_threshold(counts[scope]), scope


def lead(test_vn, warn, warn_above, start):
    bt = pd.DataFrame({"location_id": test_vn["location_id"].to_numpy(),
                       "hour_utc": pd.to_datetime(test_vn["ts"].to_numpy(), unit="s"),
                       "aqi": test_vn["aqi"].to_numpy(), "warn": warn})
    return lead_summary(lead_times(bt, warn_above, start))


def by_h(h, pred, actual, now):
    d = pd.DataFrame({"h": h, "err": np.abs(pred - actual), "pers": np.abs(now - actual)})
    g = d.groupby("h").agg(n=("err", "size"), mae=("err", "mean"), mae_persistence=("pers", "mean"))
    return [dict(h=int(i), n=int(r.n), mae=float(r.mae), mae_persistence=float(r.mae_persistence))
            for i, r in g.iterrows()]


# ------------------------------------------------------------------ XGBoost
def xgboost_models(df, s, split, params, warn_above, device, rng):
    import xgboost as xgb
    aqi, out = f"aqi_{s}", {}
    keep = df["is_vn"].to_numpy() | (rng.random(len(df)) < params["sample"])
    train = df[keep & (df["ts"] + H * HOUR < split["val"]).to_numpy()]
    val = df[keep & (df["ts"] >= split["val"]).to_numpy() & (df["ts"] + H * HOUR < split["test"]).to_numpy()]
    test = df[(df["ts"] >= split["test"]) & df["is_vn"]]
    targets = df[["location_id", "ts", aqi]].rename(columns={"ts": "t_ts", aqi: "y_aqi"})
    k = params["horizons_per_anchor"]
    xf = feature_cols(s, True)
    common = dict(tree_method="hist", device=device, max_depth=8, learning_rate=0.05, subsample=0.8,
                  colsample_bytree=0.8, min_child_weight=5, n_estimators=2000, early_stopping_rounds=50)

    tr = horizon_rows(train, targets, s, rng.integers(1, H + 1, (len(train), k)))
    va = horizon_rows(val, targets, s, rng.integers(1, H + 1, (len(val), k)))
    te = horizon_rows(test, targets, s, np.tile(np.arange(1, H + 1), (len(test), 1)))
    t0 = time.time()
    reg = xgb.XGBRegressor(objective="reg:squarederror", eval_metric="mae", **common)
    reg.fit(tr[xf], tr["y"], eval_set=[(va[xf], va["y"])], verbose=False)
    pred = np.maximum(0, te[aqi].to_numpy() + reg.predict(te[xf]))
    out["forecast"] = dict(train_rows=len(tr), seconds=round(time.time() - t0), trees=int(reg.best_iteration) + 1,
                           by_h=by_h(te["h"].to_numpy(), pred, te["y_aqi"].to_numpy(), te[aqi].to_numpy()))
    print(f"[{s}] xgboost forecast: {len(tr):,} rows, {out['forecast']['seconds']} s, "
          f"{out['forecast']['trees']} trees", flush=True)

    xw = feature_cols(s, False)
    lab = lambda d: d[d[f"{s}_next24_n"] >= MIN_LABEL_HOURS]  # noqa: E731
    y = lambda d: (d[f"{s}_next24_max"] > warn_above).to_numpy()  # noqa: E731
    trl, val_l, tel = lab(train), lab(val), lab(test)
    t0 = time.time()
    clf = xgb.XGBClassifier(objective="binary:logistic", eval_metric="logloss", **common)
    clf.fit(trl[xw], y(trl), eval_set=[(val_l[xw], y(val_l))], verbose=False)
    thr, scope = pick_threshold(warn_counts(clf.predict_proba(val_l[xw])[:, 1], y(val_l), val_l["is_vn"].to_numpy()))
    p_test = clf.predict_proba(tel[xw])[:, 1]
    tp, fp, fn = warn_counts(p_test, y(tel), tel["is_vn"].to_numpy())["vn"][thr]
    warn_all = clf.predict_proba(test[xw])[:, 1] >= thr
    out["warning"] = dict(train_rows=len(trl), seconds=round(time.time() - t0), threshold=thr, threshold_scope=scope,
                          vn=scores(tp, fp, fn),
                          lead=lead(test.rename(columns={aqi: "aqi"}), warn_all, warn_above,
                                    datetime.fromtimestamp(split["test"], timezone.utc).replace(tzinfo=None)))
    print(f"[{s}] xgboost warning: threshold {thr}, VN F1 {out['warning']['vn']['f1']:.3f}", flush=True)
    return out


# ------------------------------------------------------------------ LSTM
FEATURES_LSTM = 6 + len(POLLUTANTS)       # aqi, filled flag, pollutants, hour sin/cos, lat, lon


def grid(df, s, stations, scale):
    """Every chosen station on a dense hourly grid, stations end to end with H empty hours between.
    Returns inputs X (filled forward, with a flag where filled), raw AQI Y (NaN where missing),
    and each df row's grid position (-1 for stations not chosen)."""
    d = df[df["location_id"].isin(stations)].sort_values(["location_id", "ts"])
    first = d.groupby("location_id")["ts"].min()
    span = (d.groupby("location_id")["ts"].max() - first) // HOUR + 1
    offset = (span + H).cumsum() - (span + H) + H          # H empty hours before every station
    total = int(offset.iloc[-1] + span.iloc[-1]) + H      # H empty hours after the last station too
    pos = (offset.reindex(d["location_id"]).to_numpy() + (d["ts"].to_numpy() - first.reindex(d["location_id"]).to_numpy()) // HOUR)
    X = np.full((total, FEATURES_LSTM), np.nan, np.float32)
    Y = np.full(total, np.nan, np.float32)
    a = d[f"aqi_{s}"].to_numpy(np.float32)
    Y[pos] = a
    X[pos, 0] = a / 100
    X[pos, 1] = 0
    for i, c in enumerate(POLLUTANTS):
        X[pos, 2 + i] = np.clip(d[c].to_numpy(np.float32), 0, None) / scale[c]
    hr = d["hour_local"].to_numpy(np.float32) * (2 * np.pi / 24)
    X[pos, 2 + len(POLLUTANTS)] = np.sin(hr)
    X[pos, 3 + len(POLLUTANTS)] = np.cos(hr)
    X[pos, 4 + len(POLLUTANTS)] = d["lat"].to_numpy(np.float32) / 90
    X[pos, 5 + len(POLLUTANTS)] = d["lon"].to_numpy(np.float32) / 180
    # forward fill along time; a station's first hour is always observed, so nothing crosses stations
    valid = ~np.isnan(X[:, 0])
    last = np.maximum.accumulate(np.where(valid, np.arange(total), 0))
    X = X[last]
    X[:, 1] = (~valid).astype(np.float32)
    rows = pd.Series(-1, index=df.index, dtype="int64")
    rows.loc[d.index] = pos
    return np.nan_to_num(X), Y, rows.to_numpy()


def lstm_model(df, s, split, params, warn_above, device, rng):
    import torch
    from torch import nn
    aqi = f"aqi_{s}"
    world = df.loc[~df["is_vn"], "location_id"].unique()
    # salted: the fetch (src/ingestion/hf_near_silver.py) kept stations by the unsalted hash, so the
    # same test again would keep nearly all of them
    chosen = [i for i in world if zlib.crc32(f"lstm:{i}".encode()) % 10_000 < params["sample"] * 10_000]
    stations = np.concatenate([df.loc[df["is_vn"], "location_id"].unique(), chosen])
    pos_ok = df["ts"] < split["val"] - H * HOUR
    scale = {c: max(float(df.loc[pos_ok & (df[c] > 0), c].quantile(0.95)), 1e-6) for c in POLLUTANTS}
    X, Y, gpos = grid(df, s, stations, scale)
    df = df.assign(gpos=gpos)
    usable = df["gpos"] >= H                       # a full 24-hour window inside the grid
    # the window must not reach back into the gap before the station's first hour
    first = df.groupby("location_id")["ts"].transform("min")
    usable &= df["ts"] - first >= (H - 1) * HOUR
    train = df[usable & (df["ts"] + H * HOUR < split["val"])]
    val = df[usable & (df["ts"] >= split["val"]) & (df["ts"] + H * HOUR < split["test"])]
    test = df[usable & (df["ts"] >= split["test"]) & df["is_vn"]]

    dev = torch.device(device)
    Xt, Yt = torch.from_numpy(X).to(dev), torch.from_numpy(Y).to(dev)
    back, ahead = torch.arange(-H + 1, 1, device=dev), torch.arange(1, H + 1, device=dev)

    def batch(idx):
        i = torch.as_tensor(idx, device=dev)
        return Xt[i[:, None] + back], Yt[i[:, None] + ahead], Yt[i]

    class Net(nn.Module):
        def __init__(self):
            super().__init__()
            self.lstm = nn.LSTM(FEATURES_LSTM, 64, batch_first=True)
            self.out = nn.Linear(64, H)

        def forward(self, x):
            o, _ = self.lstm(x)
            return self.out(o[:, -1])

    def loss_on(net, idx, bs=8192):
        tot, n = 0.0, 0
        with torch.no_grad():
            for j in range(0, len(idx), bs):
                x, y, now = batch(idx[j:j + bs])
                m = ~torch.isnan(y)
                d = (net(x) - (y - now[:, None]) / 100)[m]
                tot += d.abs().sum().item()
                n += m.sum().item()
        return tot / max(n, 1)

    torch.manual_seed(7)
    net = Net().to(dev)
    opt = torch.optim.Adam(net.parameters(), lr=2e-3)
    tr_idx, va_idx = train["gpos"].to_numpy(), val["gpos"].to_numpy()
    best, best_state, t0 = float("inf"), None, time.time()
    for ep in range(params.get("lstm_epochs", 8)):
        net.train()
        perm = rng.permutation(len(tr_idx))
        for j in range(0, len(perm), 4096):
            x, y, now = batch(tr_idx[perm[j:j + 4096]])
            m = ~torch.isnan(y)
            loss = (net(x) - (y - now[:, None]) / 100)[m].abs().mean()
            opt.zero_grad()
            loss.backward()
            opt.step()
        net.eval()
        v = loss_on(net, va_idx)
        print(f"[{s}] lstm epoch {ep + 1}: validation MAE {v * 100:.2f}", flush=True)
        if v < best:
            best, best_state = v, {k: t.detach().clone() for k, t in net.state_dict().items()}
    net.load_state_dict(best_state)
    net.eval()
    preds = []
    with torch.no_grad():
        te_idx = test["gpos"].to_numpy()
        for j in range(0, len(te_idx), 8192):
            x, _, now = batch(te_idx[j:j + 8192])
            preds.append((now[:, None] + net(x) * 100).clamp(min=0).cpu().numpy())
    pred = np.concatenate(preds) if preds else np.zeros((0, H), np.float32)
    actual = Y[te_idx[:, None] + np.arange(1, H + 1)]
    now = test[aqi].to_numpy()[:, None].repeat(H, 1)
    hs = np.tile(np.arange(1, H + 1), (len(test), 1))
    m = ~np.isnan(actual)
    out = dict(stations=len(stations), train_windows=len(tr_idx), seconds=round(time.time() - t0),
               by_h=by_h(hs[m], pred[m], actual[m], now[m]))
    warn = pred.max(axis=1) > warn_above
    labelled = (test[f"{s}_next24_n"] >= MIN_LABEL_HOURS).to_numpy()
    y = (test[f"{s}_next24_max"] > warn_above).to_numpy()
    w, yl = warn[labelled], y[labelled]
    out["warning"] = dict(rule=f"highest of the 24 predictions > {warn_above}",
                          vn=scores(int((w & yl).sum()), int((w & ~yl).sum()), int((~w & yl).sum())),
                          lead=lead(test.rename(columns={aqi: "aqi"}), warn, warn_above,
                                    datetime.fromtimestamp(split["test"], timezone.utc).replace(tzinfo=None)))
    print(f"[{s}] lstm: {len(tr_idx):,} windows from {len(stations)} stations, {out['seconds']} s", flush=True)
    return out


def main():
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--run", help="run id (default: the newest finished run)")
    p.add_argument("--lstm-epochs", type=int, default=8)
    p.add_argument("--skip", nargs="*", default=[], choices=["xgboost", "lstm"])
    args = p.parse_args()
    root = LAKE / "gold" / "ml"
    runs = sorted(d.name for d in root.glob("*") if (d / "metrics.json").exists() and (d / "features").is_dir())
    if not runs:
        raise SystemExit(f"no finished run with features in {root}")
    run_dir = root / (args.run or runs[-1])
    metrics = json.loads((run_dir / "metrics.json").read_text(encoding="utf-8"))
    split = {"val": epoch(metrics["split"]["val_start"]), "test": epoch(metrics["split"]["test_start"])}
    params = dict(metrics["params"], lstm_epochs=args.lstm_epochs)
    try:
        import torch
        device = "cuda" if torch.cuda.is_available() else "cpu"
    except ImportError:
        device = "cpu"
    result = dict(run_id=metrics["run_id"], device=device, created_utc=datetime.now(timezone.utc).isoformat(), std={})
    for s in STDS:
        rng = np.random.default_rng(7)
        df = load(run_dir, s)
        sp = metrics["std"][s]
        warn_above = sp["risk"]["warn_above"]
        r = result["std"][s] = {"spark": {
            "forecast": dict(by_h=[{k: v for k, v in b.items() if k in ("h", "n", "mae", "mae_persistence")}
                                   for b in sp["forecast"]["by_h"] if b["scope"] == "vn"],
                             seconds=sp["forecast"]["fit_seconds"], train_rows=sp["forecast"]["train_rows"]),
            "warning": dict(vn=sp["risk"]["warning"].get("vn"), threshold=sp["risk"]["threshold"],
                            lead=sp["risk"]["lead"]["model"], seconds=sp["risk"]["fit_seconds"]),
            "baseline_warning": dict(vn=sp["risk"]["baseline"].get("vn"), lead=sp["risk"]["lead"]["baseline"])}}
        print(f"[{s}] {len(df):,} station-hours with AQI", flush=True)
        if "xgboost" not in args.skip:
            r["xgboost"] = xgboost_models(df, s, split, params, warn_above, device, rng)
        if "lstm" not in args.skip:
            r["lstm"] = lstm_model(df, s, split, params, warn_above, device, rng)
        for name in ("spark", "xgboost", "lstm"):
            if name in r:
                f = r[name].get("forecast", r[name]) if name != "lstm" else r[name]
                bh = {b["h"]: b for b in f["by_h"]}
                print(f"[{s}] {name:8s} MAE h1 {bh[1]['mae']:.1f}  h6 {bh[6]['mae']:.1f}  h24 {bh[24]['mae']:.1f}  "
                      f"(persistence {bh[1]['mae_persistence']:.1f} / {bh[6]['mae_persistence']:.1f} / "
                      f"{bh[24]['mae_persistence']:.1f})", flush=True)
    (run_dir / "comparison.json").write_text(json.dumps(result, indent=2), encoding="utf-8")
    print(f"written {run_dir / 'comparison.json'}", flush=True)


if __name__ == "__main__":
    main()
