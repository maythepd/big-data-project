-- Tables the dashboard reads. Postgres runs this once, when the data volume is first
-- created (docker-compose.serving.yml); to apply changes to an existing volume, run
-- the statement by hand or recreate the volume.

-- Newest reading per station and parameter, upserted every minute by the streaming
-- consumer (src/streaming/consumer.py). Times are UTC.
CREATE TABLE IF NOT EXISTS latest_readings (
    station_id  text             NOT NULL,
    parameter   text             NOT NULL,
    source      text             NOT NULL,
    lat         double precision,
    lon         double precision,
    value       double precision NOT NULL,
    unit        text             NOT NULL,
    event_time  timestamptz      NOT NULL,   -- when the reading was measured
    ingested_at timestamptz,                 -- when the producer fetched it
    updated_at  timestamptz      NOT NULL DEFAULT now(),
    PRIMARY KEY (station_id, parameter)
);
