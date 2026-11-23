"""Warning scores, threshold choice and lead times (src/ml/scoring.py); no Spark needed."""
import pandas as pd

from src.ml.scoring import band_of, best_threshold, class_scores, lead_summary, lead_times, scores, warn_band


def test_bands_share_edges_in_both_standards():
    assert [band_of(v) for v in (0, 50, 51, 150, 151, 300, 301)] == [0, 0, 1, 2, 3, 4, 5]
    assert warn_band(150) == 3                      # > 150: Xấu / Unhealthy and worse


def test_scores_and_f2_threshold():
    s = scores(tp=8, fp=8, fn=2)
    assert s["precision"] == 0.5 and s["recall"] == 0.8
    # 0.3 catches everything with many false alarms, 0.7 misses half: F2 prefers recall
    counts = {0.3: (10, 10, 0), 0.5: (8, 4, 2), 0.7: (5, 0, 5)}
    assert best_threshold(counts) == 0.3
    assert best_threshold({0.4: (5, 5, 5), 0.6: (5, 5, 5)}) == 0.6   # tie: fewer alarms


def test_class_scores_skip_bands_absent_from_truth():
    m = [[8, 2, 0], [1, 9, 0], [0, 0, 0]]
    r = class_scores(m)
    assert r["n"] == 20 and r["accuracy"] == 17 / 20
    assert abs(r["macro_f1"] - (0.8421 + 0.8571) / 2) < 1e-3


def test_lead_times_count_onsets_and_earliest_warning():
    t0 = pd.Timestamp("2025-01-01")
    hours = pd.date_range(t0, periods=60, freq="h")
    aqi = [100] * 60
    aqi[40:45] = [180] * 5                           # episode from hour 40
    aqi[50] = 160                                     # second episode at hour 50
    warn = [False] * 60
    warn[30] = warn[31] = True                        # 10 h before the first onset
    df = pd.DataFrame(dict(location_id=1, hour_utc=hours, aqi=aqi, warn=warn))
    df = df.drop(index=[41])                          # hour 42 follows a missing hour: a new onset
    ep = lead_times(df, 150, t0)
    assert ep["onset_utc"].tolist() == [hours[40], hours[42], hours[50]]
    assert ep["lead_h"].tolist() == [10.0, 12.0, 20.0]   # hour 30 is within 24 h of all three
    assert ep["warned"].all()
    late = lead_times(df.assign(warn=df["hour_utc"] == hours[20]), 150, t0)
    assert late["warned"].tolist() == [True, True, False]  # hour 20 is 30 h before hour 50
    s = lead_summary(ep)
    assert s["episodes"] == 3 and s["warned"] == 3 and s["median_lead_h"] == 12.0


def test_lead_times_skip_onsets_without_a_full_window():
    t0 = pd.Timestamp("2025-01-01")
    df = pd.DataFrame(dict(location_id=1, hour_utc=pd.date_range(t0, periods=30, freq="h"),
                           aqi=[200] + [100] * 29, warn=False))
    assert lead_times(df, 150, t0).empty            # onset at hour 0: its window lies before the test
