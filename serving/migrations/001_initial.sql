-- Migration 001: immutable replay publication and atomic active-release pointer.
-- Publisher is INSERT-only on immutable tables; API role gets SELECT only.
CREATE SCHEMA IF NOT EXISTS fleetpulse;

CREATE DOMAIN fleetpulse.sha256 AS text
    CHECK (VALUE ~ '^[0-9a-f]{64}$');

CREATE TABLE fleetpulse.dataset_snapshots (
    dataset_id fleetpulse.sha256 PRIMARY KEY, -- Gold run ID
    manifest_sha256 fleetpulse.sha256 NOT NULL,
    zone_reference_sha256 fleetpulse.sha256 NOT NULL,
    event_time_basis text NOT NULL CHECK (event_time_basis = 'local_wall_clock'),
    calendar_assumption text NOT NULL CHECK (calendar_assumption = 'America/New_York'),
    feature_policy text NOT NULL CHECK (feature_policy = 'local-replay-v1'),
    first_observed_local timestamp without time zone NOT NULL,
    last_observed_local timestamp without time zone NOT NULL,
    source_manifests jsonb NOT NULL CHECK (jsonb_typeof(source_manifests) = 'object'),
    registered_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (isfinite(first_observed_local) AND isfinite(last_observed_local)),
    CHECK (first_observed_local <= last_observed_local)
);

CREATE TABLE fleetpulse.zones (
    dataset_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.dataset_snapshots,
    zone_id integer NOT NULL CHECK (zone_id > 0),
    zone_name text NOT NULL,
    borough text NOT NULL,
    PRIMARY KEY (dataset_id, zone_id)
);

CREATE TABLE fleetpulse.model_versions (
    model_id fleetpulse.sha256 PRIMARY KEY, -- hash of canonical model identity
    experiment_id fleetpulse.sha256 NOT NULL,
    experiment_manifest_sha256 fleetpulse.sha256 NOT NULL,
    training_dataset_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.dataset_snapshots,
    model_name text NOT NULL,
    model_kind text NOT NULL CHECK (model_kind IN ('lightgbm_poisson', 'weekly_baseline')),
    artifact_sha256 fleetpulse.sha256,
    training_start_local timestamp without time zone NOT NULL,
    training_end_exclusive_local timestamp without time zone NOT NULL,
    feature_schema jsonb NOT NULL CHECK (jsonb_typeof(feature_schema) = 'array'),
    evaluation_summary jsonb NOT NULL CHECK (jsonb_typeof(evaluation_summary) = 'object'),
    registered_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    CHECK (isfinite(training_start_local) AND isfinite(training_end_exclusive_local)),
    CHECK (training_start_local < training_end_exclusive_local),
    CHECK ((model_kind = 'lightgbm_poisson' AND artifact_sha256 IS NOT NULL)
        OR (model_kind = 'weekly_baseline' AND artifact_sha256 IS NULL))
);

CREATE TABLE fleetpulse.forecast_runs (
    run_id fleetpulse.sha256 PRIMARY KEY, -- deterministic publication identity
    dataset_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.dataset_snapshots,
    model_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.model_versions,
    mode text NOT NULL CHECK (mode = 'historical_replay'),
    source_split text NOT NULL CHECK (source_split = 'test'),
    prediction_artifact_sha256 fleetpulse.sha256 NOT NULL,
    baseline_artifact_sha256 fleetpulse.sha256 NOT NULL,
    publication_contract_version integer NOT NULL CHECK (publication_contract_version = 1),
    publisher_code_sha256 fleetpulse.sha256 NOT NULL,
    origins_start_local timestamp without time zone NOT NULL,
    origins_end_exclusive_local timestamp without time zone NOT NULL,
    prediction_count bigint NOT NULL CHECK (prediction_count > 0),
    published_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (run_id, dataset_id),
    CHECK (isfinite(origins_start_local) AND isfinite(origins_end_exclusive_local)),
    CHECK (origins_start_local < origins_end_exclusive_local)
);

CREATE TABLE fleetpulse.predictions (
    run_id fleetpulse.sha256 NOT NULL,
    dataset_id fleetpulse.sha256 NOT NULL,
    zone_id integer NOT NULL,
    forecast_origin_local timestamp without time zone NOT NULL,
    target_start_local timestamp without time zone NOT NULL,
    target_end_local timestamp without time zone NOT NULL,
    predicted_pickups double precision NOT NULL,
    baseline_pickups double precision NOT NULL,
    actual_pickups bigint, -- nullable for general response semantics; June rows have actuals
    PRIMARY KEY (run_id, forecast_origin_local, zone_id),
    FOREIGN KEY (run_id, dataset_id) REFERENCES fleetpulse.forecast_runs (run_id, dataset_id),
    FOREIGN KEY (dataset_id, zone_id) REFERENCES fleetpulse.zones (dataset_id, zone_id),
    CHECK (isfinite(forecast_origin_local)),
    CHECK (forecast_origin_local = date_trunc('hour', forecast_origin_local)),
    CHECK (target_start_local = forecast_origin_local),
    CHECK (target_end_local = target_start_local + interval '1 hour'),
    -- PostgreSQL NaN sorts above Infinity; this rejects both as well as negatives.
    CHECK (predicted_pickups >= 0 AND predicted_pickups < 'Infinity'::double precision),
    CHECK (baseline_pickups >= 0 AND baseline_pickups < 'Infinity'::double precision),
    CHECK (actual_pickups IS NULL OR actual_pickups >= 0)
);
CREATE INDEX predictions_by_zone_time ON fleetpulse.predictions
    (run_id, zone_id, forecast_origin_local);
CREATE INDEX forecast_runs_by_publication ON fleetpulse.forecast_runs
    (published_at_utc DESC, run_id DESC);

-- Move one pointer, so run and model activation cannot drift apart.
CREATE TABLE fleetpulse.active_releases (
    slot text PRIMARY KEY CHECK (slot = 'historical-demo'),
    run_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.forecast_runs,
    changed_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE fleetpulse.release_history (
    change_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    slot text NOT NULL CHECK (slot = 'historical-demo'),
    previous_run_id fleetpulse.sha256 REFERENCES fleetpulse.forecast_runs,
    next_run_id fleetpulse.sha256 NOT NULL REFERENCES fleetpulse.forecast_runs,
    reason text NOT NULL CHECK (length(trim(reason)) > 0),
    changed_at_utc timestamptz NOT NULL DEFAULT CURRENT_TIMESTAMP
);

-- Publisher responsibilities not expressible by these row-level constraints:
-- validate artifact/checksum/schema/key equality before BEGIN; COPY all predictions
-- within one transaction; check row count, actuals, bounds, and expected zone coverage;
-- activate only after checks pass; COMMIT publishes all rows at once.
-- On a repeated run_id, compare immutable identity AND stored content, not just row
-- count. Never use ON CONFLICT DO UPDATE to replace immutable forecast data.
-- Serialize publication/activation with a transaction-scoped advisory lock, including
-- initial creation of the active slot. An unchanged rerun must not reactivate an old run.
-- Rollback means atomically moving the active pointer to a prior COMPLETE run and
-- appending history; historical predictions remain immutable.
