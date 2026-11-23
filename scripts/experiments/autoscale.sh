#!/usr/bin/env bash
# Autoscaling experiment and demo (docs/fault-tolerance-report.md, "Autoscaling").
# The stream validator (a Kafka consumer group) starts at 1 replica; src/streaming/autoscaler.py
# scales it by consumer lag between MIN and MAX replicas (12 partitions = the ceiling).
#   A steady load: Locust sensor fleets + a broken feed for STEADY seconds (USERS users)
#   B burst: BURST real readings replayed as fast as the producer goes (a backlog, as after an outage)
#   C idle: the load stops; after the stabilization window the replicas scale back in
# Then every message is counted: in = valid + dead-letter (+ redelivered by rebalances).
# Usage: scripts/experiments/autoscale.sh   (USERS, STEADY, BURST, MIN, MAX, LAG_PER_REPLICA, RATE_PER_REPLICA, TAG)
# RATE_PER_REPLICA=0 is the lag-only rule (run3: it scaled in mid-burst and the lag came back).
# Memory: Kafka (3 x 256 MB heap) + Locust + up to MAX validators (~70 MB each); no HDFS or Spark.
. "$(dirname "$0")/common.sh"
TAG=${TAG:-$(date +%m%d%H%M)}
export AUTOSCALE_IN="as-in-$TAG" AUTOSCALE_OUT="as-valid-$TAG" AUTOSCALE_DLQ="as-dlq-$TAG"
AUTOSCALE="docker compose -f docker-compose.autoscale.yml"
LOADTEST="docker compose -f docker-compose.loadtest.yml"
OUT="openaq_lake/reports/autoscale_$TAG"
mkdir -p "$OUT"
topic() { docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092 --create --if-not-exists \
            --topic "$1" --partitions "$2" --replication-factor 3 --config min.insync.replicas=2 >/dev/null; }
total() { docker exec kafka1 /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server kafka1:29092 --topic "$1" \
            | awk -F: '{s += $3} END {print s + 0}'; }
lag_now() { tail -n1 "$OUT/autoscale.csv" | cut -d, -f2; }
replicas_now() { $AUTOSCALE ps -q validator | wc -l | tr -d ' '; }

export OPENAQ_KAFKA_HEAP="-Xmx256m -Xms256m"
$KAFKA up -d kafka1 kafka2 kafka3 >/dev/null
for i in $(seq 1 60); do docker exec kafka1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka1:29092 --list >/dev/null 2>&1 && break; sleep 3; done
topic "$AUTOSCALE_IN" 12; topic "$AUTOSCALE_OUT" 12; topic "$AUTOSCALE_DLQ" 3
say "topics $AUTOSCALE_IN (12 partitions) -> $AUTOSCALE_OUT / $AUTOSCALE_DLQ"

$AUTOSCALE up -d --build --scale validator="${MIN:-1}" >/dev/null 2>&1
python -u -m src.streaming.autoscaler --topic "$AUTOSCALE_IN" --group validator \
  --compose-file docker-compose.autoscale.yml --service validator --min "${MIN:-1}" --max "${MAX:-6}" \
  --lag-per-replica "${LAG_PER_REPLICA:-20000}" --rate-per-replica "${RATE_PER_REPLICA:-4000}" --interval 10 --scale-in-after 60 --csv "$OUT/autoscale.csv" \
  > "$OUT/autoscaler.log" 2>&1 &
scaler=$!
tail -n +1 -f "$OUT/autoscaler.log" &          # the autoscaler's decisions, live in this terminal
tailer=$!
trap 'kill $scaler $tailer 2>/dev/null; $AUTOSCALE down >/dev/null 2>&1' EXIT
sleep 15

say "A: steady load, ${USERS:-30} Locust users for ${STEADY:-180} s"
LOADTEST_TOPIC="$AUTOSCALE_IN" $LOADTEST run --rm --no-deps locust -f loadtest/locustfile.py --headless \
  -u "${USERS:-30}" -r 10 -t "${STEADY:-180}s" --only-summary SensorFleet BadDataSender 2>&1 \
  | grep -E "kafka |bad data |Aggregated" | tee "$OUT/locust.txt" || true

say "B: burst of ${BURST:-1000000} readings, as fast as the producer goes"
mkdir -p "openaq_lake/replay/autoscale_$TAG"
$PRODUCERS run --rm replay --run-id "autoscale_$TAG" --topic "$AUTOSCALE_IN" --hours 48 --limit "${BURST:-1000000}" \
  --variants v1=0.01,malformed=0.01 2>&1 | grep "done:" || true

say "C: load stopped; waiting for the lag to drain and the replicas to scale back in"
for i in $(seq 1 120); do
  [ "$(lag_now)" = "0" ] && [ "$(replicas_now)" = "${MIN:-1}" ] && break
  sleep 10
done
kill $scaler $tailer 2>/dev/null
$AUTOSCALE down >/dev/null 2>&1                 # validators stop: commit, leave the group

in=$(total "$AUTOSCALE_IN"); valid=$(total "$AUTOSCALE_OUT"); dlq=$(total "$AUTOSCALE_DLQ")
python - "$in" "$valid" "$dlq" "$OUT" <<'EOF'
import csv, json, sys
n_in, valid, dlq = map(int, sys.argv[1:4])
out = sys.argv[4]
rows = list(csv.DictReader(open(f"{out}/autoscale.csv")))
peak = max(rows, key=lambda r: int(r["lag"]))
summary = {"messages_in": n_in, "valid_out": valid, "dead_letter": dlq,
           "lost": max(0, n_in - valid - dlq), "redelivered": max(0, valid + dlq - n_in),
           "replicas_max": max(int(r["replicas"]) for r in rows),
           "scale_events": sum(1 for a, b in zip(rows, rows[1:]) if a["replicas"] != b["replicas"]),
           "peak_lag": int(peak["lag"]), "peak_lag_at": peak["time_utc"],
           "consumed_per_s_max": max(int(r["consumed_per_s"]) for r in rows)}
json.dump(summary, open(f"{out}/summary.json", "w"), indent=2)
print(json.dumps(summary, indent=2))
EOF
say "done: $OUT (autoscale.csv, autoscaler.log, locust.txt, summary.json)"
