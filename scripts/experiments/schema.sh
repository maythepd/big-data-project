#!/usr/bin/env bash
# Experiment 5, schema evolution in the stream (docs/bigdata-experiments.md).
# LIMIT messages, 2% each of seven variants (legacy v1, an additive v3, a renamed field, a value
# sent as text, missing keys, cut-off JSON, an oversized message), into the consumer under test:
#  A. no contract: what the archive, the dead-letter store and Silver do with each variant
#  B. contract: the replay producer checks each message against its JSON Schema in the Schema
#     Registry (BACKWARD compatibility); breaking messages go to the -dlq topic at the source
# Usage: scripts/experiments/schema.sh    (LIMIT, TAG override)
. "$(dirname "$0")/common.sh"
infra_up
$KAFKA --profile registry up -d schema-registry
for i in $(seq 1 40); do curl -sf http://localhost:8081/subjects >/dev/null && break; sleep 3; done
say "Schema Registry ready"
TAG=${TAG:-$(date +%m%d%H%M)}
VARIANTS="v1=0.02,v3_extra_field=0.02,renamed_value=0.02,string_value=0.02,missing_key=0.02,malformed=0.02,oversized=0.001"

for mode in open contract; do
  run="schema_${TAG}_${mode}"
  consumer_up "$run"
  if [ "$mode" = open ]; then
    replay "$run" --hours 12 --limit "${LIMIT:-100000}" --variants "$VARIANTS"
  else
    replay "$run" --hours 12 --limit "${LIMIT:-100000}" --variants "$VARIANTS" --registry http://schema-registry:8081
  fi
  wait_drained "$run" 900 || say "$run: not drained"
  consumer_down "$run"
  reconcile "$run"
done
say "registry versions: $(curl -s "http://localhost:8081/subjects/replay-schema_${TAG}_contract-value/versions")"
