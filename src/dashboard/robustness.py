"""Robustness lab: the problems we simulated, how the pipeline handles them, and the measured result.

Reads the reports in openaq_lake/ (no MongoDB needed); sections whose run has not finished show
as pending. docs/fault-injection.md and docs/bigdata-experiments.md describe the runs.

    streamlit run src/dashboard/robustness.py --server.port 8502      # http://localhost:8502
"""

import csv
import json
from pathlib import Path
import re
import sys

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from src.chaos.faults import BY_NAME  # noqa: E402
from src.common.config import LAKE  # noqa: E402

REPORTS = LAKE / "reports"
STATUS_COLOR = {"handled": "#1baf7a", "partly": "#eda100", "gap": "#e34948", "pending": "#9e9e9e"}

# data-quality faults: the real problem behind each, and what handles it (or should)
FAULT_TEXT = {
    "sentinel": ("Sensors report -999 / 9999 'no data' codes as values (-999 in 7.9M archive rows)",
                 "Sentinel list in the Silver rules: value nulled, row invalid"),
    "out_of_range": ("Physically impossible values (e.g. 42949472 hPa)", "Hard physical bounds per parameter"),
    "spike": ("A single reading jumps 10x and falls back", "R14 spike: reading vs its hour and neighbouring hours"),
    "bad_number": ("Locale-formatted numbers ('12,5') cannot be parsed",
                   "GAP: row stays valid with a null value. Proposed R15 unparseable_value (hard)"),
    "unknown_parameter": ("Parameter spelled differently ('pm2.5')", "Unknown-parameter flag (invalid); aliases table"),
    "unit_alias": ("Unit spelled differently ('ug/m3')", "unit_unconverted flag (suspect); units alias table"),
    "future_ts": ("Device clock wrong: timestamp years ahead", "R04 time range vs build time"),
    "coord_swap": ("Latitude and longitude swapped",
                   "R11 replaces impossible coordinates with the station reference; swaps that stay possible "
                   "(mid-latitudes) pass. Proposed R17 coord_far: > 50 km from the reference"),
    "null_island": ("Coordinates (0, 0) sent for 'unknown'", "R11: replaced by the station reference"),
    "missing_key": ("Sensor id missing", "R01/R03 quarantine: no_key"),
    "shifted_row": ("Columns shifted (a split CSV record)", "R01/R03 quarantine: multiline_split_record"),
    "duplicate": ("The same record delivered twice", "R13 dedup on (sensor, time)"),
    "dup_conflict": ("Two different values for the same sensor and time",
                     "GAP: dedup keeps one by block order, silently. Proposed R18 dup_conflict (suspect)"),
    "stuck": ("A dead sensor repeats one value for days", "R14 flatline: same value held >= 24 h"),
    "unit_x1000": ("Upstream unit change: values 1000x", "Hard bounds catch most; small gas values x1000 stay "
                                                        "inside them. Proposed R19 level_shift"),
    "unit_div1000": ("Upstream unit change: values /1000", "GAP: values look plausible. Proposed R19 level_shift: "
                                                          "sensor-day median 100x below its month"),
    "calibration_bias": ("Low-cost sensor drifts 60% high", "GAP: proposed R20 neighbour_disagree (needs a "
                                                            "false-alarm check first)"),
    "tz_shift": ("Local time sent as UTC", "GAP: proposed R16 offset_mismatch: offset differs from the "
                                          "station's usual offset"),
}

EXPERIMENTS = [
    {"key": "velocity", "title": "1. Velocity and backpressure",
     "problem": "Data arrives faster than the consumer processes it, or piles up during an outage; the first "
                "batch after the outage then tries to take everything at once.",
     "simulate": "Replay 300k real readings into Kafka at 2,000 / 5,000 / 10,000 msg/s and unthrottled; then "
                 "queue 1M messages with the consumer down and start it.",
     "handle": "Kafka buffers (3 brokers, retention 7 days); Spark Structured Streaming micro-batches; "
               "maxOffsetsPerTrigger caps each batch (backpressure); progress and lag logged per batch.",
     "tools": "Kafka, Spark Structured Streaming, replay producer, progress listener"},
    {"key": "exactly_once", "title": "2. Exactly-once under failure",
     "problem": "Crashes and retries lose messages or store them twice.",
     "simulate": "600k messages at 3,000 msg/s with 1% producer retries, while the consumer is killed (kill -9) "
                 "twice, a Kafka broker and an HDFS DataNode are stopped.",
     "handle": "Producer acks=all + idempotence; replication 3 / min.insync 2; checkpointed file sinks; Silver "
               "batches published atomically and skipped if already committed; dedup within a watermark. "
               "Checked by reconciling every acknowledged message.",
     "tools": "Kafka replication, HDFS replication, Spark checkpoints, reconciliation job"},
    {"key": "memory", "title": "3. Memory pressure",
     "problem": "Jobs run out of memory (OOM), spill to disk or spend their time in garbage collection.",
     "simulate": "Full-month Silver build (33M rows) at 1/2/4/8 GB; R14 with small vs 10x larger tasks; a 1 GB "
                 "consumer after a 1M-message backlog, with and without a batch cap.",
     "handle": "Repartitioning by size, --rows-per-task (R14's memory knob), maxOffsetsPerTrigger; measured "
               "from Spark event logs (spill, GC, peak memory, failed tasks).",
     "tools": "Spark event logs, event-log summarizer"},
    {"key": "reprocess", "title": "4. Reprocessing safety",
     "problem": "A rebuild crashes halfway: readers see a broken month, or a rerun stores rows twice; a bad "
                "rebuild overwrites good data.",
     "simulate": "Failpoints crash the Silver build mid-move and before its done marker; a faulty rebuild "
                 "(the chaos Silver) overwrites a Delta table; an overwrite is killed mid-write.",
     "handle": "Staging + done markers make reruns idempotent; Delta Lake commits a whole overwrite or nothing; "
               "a quality gate (invalid share, row count) compares versions; RESTORE rolls back.",
     "tools": "Failpoints, Delta Lake (time travel, RESTORE, replaceWhere), quality gate"},
    {"key": "schema", "title": "5. Schema evolution in the stream",
     "problem": "Producers change the message format: old formats, new fields, renamed fields, text instead of "
                "numbers, broken JSON, messages over Kafka's size limit.",
     "simulate": "100k messages, 2% each of 7 variants, first without a contract, then with one.",
     "handle": "Dead-letter store for records the archive rejects (with the reason); Silver quarantine; "
               "schema versions 2 and 3 accepted; JSON Schemas in a Schema Registry with BACKWARD "
               "compatibility, checked at the producer, rejects to a -dlq topic.",
     "tools": "Confluent Schema Registry, JSON Schema, dead-letter store and topic"},
]

FOUND = [
    ("Silver stream read every Kafka record 5 times", "each Spark action in the per-batch function re-read "
     "the micro-batch from Kafka", "persist the batch once: 101,075 reads -> 20,215 for 20,215 messages"),
    ("Streaming dedup compared Kafka offsets as text", "'...:1000' sorted before '...:999', so a retry copy "
     "could replace the original", "offsets zero-padded in source_block"),
    ("Archive dropped unusable messages silently", "parse() filtered them out with no record",
     "dead-letter store: raw message + reason on HDFS"),
    ("Silver rejected the additive schema v3 as 'legacy'", "it accepted only schema_version 2",
     "versions 2 and 3 accepted (v3 registered BACKWARD-compatible)"),
    ("Fault injection read a month with 2-4 tasks on 16 cores", "a month is 2 large Parquet files",
     "repartition to 64 tasks"),
    ("Spark generated code over the 64 KB method limit", "long CASE chains repeated the same hash",
     "compute each hash once as a column"),
    ("Same input recomputed three times", "a DataFrame reused by three actions is rebuilt each time",
     "persist it once"),
    ("Rebuild + move is not atomic", "old files are deleted before new ones are moved in",
     "measured by experiment 4; Delta Lake commits atomically"),
]


def read_json(path):
    try:
        return json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


def read_csv(path):
    try:
        return pd.read_csv(path)
    except (OSError, ValueError):
        return None


def latest(pattern):
    found = sorted(REPORTS.glob(pattern), key=lambda p: p.stat().st_mtime)
    return found[-1] if found else None


def badge(status):
    return f"<span style='background:{STATUS_COLOR[status]};color:white;padding:2px 8px;border-radius:8px'>" \
           f"{status}</span>"


def data_quality():
    summary = read_csv(REPORTS / "chaos_01_eval" / "summary.csv")
    st.markdown("Bad data injected into one month of Near-Silver (January 2025, global, 33M rows) with a hidden "
                "label per row, then built into Silver with the normal rules and compared with an untouched "
                "control run. Every fault copies a problem the real archive showed.")
    if summary is None:
        st.info("pending: run `scripts/chaos_batch.sh chaos_01 2025-01`")
        return
    s = summary[summary["fault"] != "clean"].copy()
    caught = s["expected_share"].fillna(s["caught_share"]).astype(float)
    s["status"] = ["handled" if c >= 0.95 else "partly" if c >= 0.05 else "gap" for c in caught]
    s["share"] = caught
    s["problem"] = s["fault"].map(lambda f: FAULT_TEXT.get(f, ("", ""))[0])
    s["how we handle it"] = s["fault"].map(lambda f: FAULT_TEXT.get(f, ("", ""))[1])
    s["simulated as"] = s["fault"].map(lambda f: BY_NAME[f].what if f in BY_NAME else "")
    counts = s["status"].value_counts()
    k = st.columns(4)
    k[0].metric("Faults simulated", len(s))
    k[1].metric("Handled", int(counts.get("handled", 0)))
    k[2].metric("Partly", int(counts.get("partly", 0)))
    k[3].metric("Gaps (rule proposed)", int(counts.get("gap", 0)))

    s = s.sort_values(["share"], ascending=False)
    fig = go.Figure(go.Bar(x=s["share"] * 100, y=s["fault"], orientation="h",
                           marker_color=[STATUS_COLOR[x] for x in s["status"]],
                           text=[f"{v:.0%}" for v in s["share"]], textposition="outside"))
    fig.update_layout(height=520, margin=dict(l=10, r=40, t=10, b=10), xaxis_title="% of injected rows caught",
                      xaxis_range=[0, 110], yaxis_autorange="reversed")
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(s[["fault", "status", "share", "injected", "problem", "simulated as", "how we handle it"]],
                 hide_index=True, use_container_width=True,
                 column_config={"share": st.column_config.ProgressColumn("caught", format="%.2f", min_value=0,
                                                                          max_value=1)})
    run = read_json(REPORTS / "chaos_01_eval" / "run.json") or {}
    if run.get("clean_rows"):
        st.caption(f"Collateral: {run['clean_rows_changed']:,} of {run['clean_rows']:,} clean rows changed status "
                   f"against the control run ({run['clean_rows_changed'] / run['clean_rows']:.4%}).")


def replay_summary(run):
    return read_json(REPORTS / f"replay_{run}" / "summary.json")


def outcomes(run):
    return read_csv(REPORTS / f"replay_{run}" / "outcomes.csv")


def lost(df):
    return int(df.loc[df["silver"].astype(str).str.startswith("LOST"), "messages"].sum()) if df is not None else None


def result_velocity():
    rows = []
    for p in sorted(REPORTS.glob("replay_vel_*")):
        s = read_json(p / "summary.json")
        if not s:
            continue
        for q, m in (s.get("progress") or {}).items():
            rows.append({"run": p.name.replace("replay_", ""), "query": q, "messages": s["acked"],
                         "rows/s mean": m["processed_rps_mean"], "batch p50 s": m["batch_s_p50"],
                         "batch p95 s": m["batch_s_p95"], "batch max s": m["batch_s_max"],
                         "max offsets behind": m["offsets_behind_max"]})
    if not rows:
        st.info("pending: scripts/experiments/velocity.sh")
        return
    df = pd.DataFrame(rows)
    st.dataframe(df, hide_index=True, use_container_width=True)
    mem = sorted(LAKE.glob("replay/vel_*_backlog_*/memory_spark-replay.csv"))
    if mem:
        fig = go.Figure()
        for f in mem:
            m = read_csv(f)
            if m is None or m.empty:
                continue
            gib = m["mem"].astype(str).str.split("/").str[0].str.strip()
            val = gib.str.extract(r"([\d.]+)")[0].astype(float) * gib.str.contains("GiB").map({True: 1, False: 1 / 1024})
            fig.add_trace(go.Scatter(x=m["time"] - m["time"].iloc[0], y=val, name=f.parent.name.split("_")[-1],
                                     mode="lines"))
        fig.update_layout(height=280, margin=dict(l=10, r=10, t=30, b=10), title="Consumer memory while catching "
                          "up (GiB): no cap vs maxOffsetsPerTrigger", xaxis_title="seconds")
        st.plotly_chart(fig, use_container_width=True)


def result_exactly_once():
    run = next((p.name.replace("replay_", "") for p in sorted(REPORTS.glob("replay_eo_*"))), None)
    s, o = (replay_summary(run), outcomes(run)) if run else (None, None)
    if not s:
        st.info("pending: scripts/experiments/exactly_once.sh")
        return
    k = st.columns(4)
    k[0].metric("Messages acknowledged", f"{s['acked']:,}")
    k[1].metric("Lost (key missing)", lost(o))
    k[2].metric("Keys stored twice: archive", s["archive_duplicate_keys"])
    k[3].metric("Keys stored twice: Silver", s["silver_duplicate_keys"])
    st.caption("Timeline: kill -9 consumer at +45 s, back at +60 s; broker kafka2 down at +90 s; DataNode "
               "datanode2 down at +120 s; kill -9 consumer at +150 s, back at +160 s; broker back at +180 s.")
    if o is not None:
        st.dataframe(o, hide_index=True, use_container_width=True)


def result_memory():
    f = latest("memory_*.csv")
    df = read_csv(f) if f else None
    if df is None or df.empty:
        st.info("pending: scripts/experiments/memory.sh")
    else:
        st.dataframe(df, hide_index=True, use_container_width=True)
    stream = [p for p in sorted(REPORTS.glob("replay_mem_*"))]
    for p in stream:
        s = read_json(p / "summary.json")
        if s:
            prog = s.get("progress") or {}
            st.write(f"**{p.name.replace('replay_', '')}**: "
                     + ", ".join(f"{q}: p95 batch {m['batch_s_p95']} s, max {m['batch_s_max']} s"
                                 for q, m in prog.items()))


def result_reprocess():
    log = latest("experiments_*.log")
    if not log:
        st.info("pending: scripts/experiments/reprocess.sh")
        return
    text = log.read_text(encoding="utf-8", errors="replace")
    block = text.split("===== reprocess", 1)
    if len(block) < 2:
        st.info("pending: scripts/experiments/reprocess.sh")
        return
    block = block[1].split("=====", 1)[0]
    steps = []
    for line in block.splitlines():
        line = line.strip()
        m = re.match(r"\[[\d:]+\] (.*)", line)
        if m:
            steps.append({"step": m.group(1), "result": ""})
        elif line.startswith('{"folder"') and steps:
            r = json.loads(line)
            steps[-1]["result"] = (f"{r['files']} files, {r['rows']:,} rows, {r['distinct_keys']:,} distinct keys"
                                   + ("" if r.get("ok", True) else "  ← BROKEN"))
        elif line.startswith("delta_reprocess:") and steps:
            r = json.loads(line.split(":", 1)[1])
            seen = r.get("reader_sees", {})
            extra = f", gate passed={r['passed']}" if "passed" in r else ""
            steps[-1]["result"] = (f"versions {', '.join(r.get('versions', []))}; readers see {seen.get('rows', 0):,} "
                                   f"rows, invalid {seen.get('invalid_share', 0):.2%}{extra}")
    st.dataframe(pd.DataFrame(steps), hide_index=True, use_container_width=True)


def result_schema():
    shown = False
    for mode in ["open", "contract"]:
        run = next((p.name.replace("replay_", "") for p in sorted(REPORTS.glob(f"replay_schema_*_{mode}"))), None)
        o = outcomes(run) if run else None
        if o is None:
            continue
        shown = True
        st.write(f"**{'Without a contract' if mode == 'open' else 'With the Schema Registry contract'}**")
        g = (o.groupby(["variant", "in_archive", "deadletter", "silver"], dropna=False)["messages"].sum()
             .reset_index())
        st.dataframe(g, hide_index=True, use_container_width=True)
        s = replay_summary(run) or {}
        if s.get("not_acked"):
            st.caption("Rejected before Kafka: " + ", ".join(f"{k}: {v}" for k, v in s["not_acked"].items()))
    if not shown:
        st.info("pending: scripts/experiments/schema.sh")


RESULTS = {"velocity": result_velocity, "exactly_once": result_exactly_once, "memory": result_memory,
           "reprocess": result_reprocess, "schema": result_schema}


def main():
    st.set_page_config(page_title="Robustness lab", layout="wide")
    st.title("Robustness lab")
    st.markdown("What we broke on purpose, how the pipeline defends itself, and what we measured. "
                "Everything runs on Docker on one machine (Kafka 3 brokers, HDFS NameNode + 4 DataNodes, Spark).")
    tab1, tab2, tab3 = st.tabs(["Data quality (18 faults)", "Big-data experiments (5)", "Bugs the experiments found"])
    with tab1:
        data_quality()
    with tab2:
        for e in EXPERIMENTS:
            with st.expander(e["title"], expanded=True):
                c = st.columns(3)
                c[0].markdown(f"**Problem**  \n{e['problem']}")
                c[1].markdown(f"**How we simulate it**  \n{e['simulate']}")
                c[2].markdown(f"**How we handle it**  \n{e['handle']}  \n*Tools: {e['tools']}*")
                st.markdown("**Result**")
                RESULTS[e["key"]]()
    with tab3:
        st.dataframe(pd.DataFrame(FOUND, columns=["problem", "cause", "fix"]), hide_index=True,
                     use_container_width=True)


main()
