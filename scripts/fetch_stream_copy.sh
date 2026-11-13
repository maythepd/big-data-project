#!/usr/bin/env bash
# Copies the stream archive (/openaq/stream/readings) from the Docker HDFS to
# ~/openaq_lake/stream_copy/readings for the Gold source adapters. Needs HDFS running.
#   scripts/fetch_stream_copy.sh
set -euo pipefail
cd "$(dirname "$0")/.."
DEST=$HOME/openaq_lake/stream_copy
mkdir -p "$DEST"
rm -rf "$DEST/readings.tmp"
docker compose -f docker-compose.hdfs.yml run --rm -v "$DEST:/stream_copy" hdfs-client \
  sh -c "hdfs dfs -get /openaq/stream/readings /stream_copy/readings.tmp && chown -R $(id -u):$(id -g) /stream_copy/readings.tmp"
rm -rf "$DEST/readings"
mv "$DEST/readings.tmp" "$DEST/readings"
echo "stream archive: $(find "$DEST/readings" -name '*.parquet' | wc -l) files, $(du -sh "$DEST/readings" | cut -f1)"
