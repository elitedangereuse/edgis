-- === Metrics Storage ===
CREATE TABLE IF NOT EXISTS eddn_systems_metrics (
bucket          TIMESTAMPTZ PRIMARY KEY,
systems_processed BIGINT NOT NULL DEFAULT 0
);

CREATE TABLE IF NOT EXISTS eddn_bodies_metrics (
bucket           TIMESTAMPTZ PRIMARY KEY,
bodies_processed BIGINT NOT NULL DEFAULT 0
);
