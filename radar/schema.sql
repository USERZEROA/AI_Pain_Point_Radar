-- Dedicated event columns: no JSON-only event storage.
CREATE TABLE IF NOT EXISTS raw_events (
    event_id text PRIMARY KEY,
    schema_version smallint NOT NULL CHECK (schema_version = 1),
    source_item_id text NOT NULL,
    source text NOT NULL,
    item_type text NOT NULL CHECK (item_type IN ('story','comment')),
    thread_id text NOT NULL,
    source_created_at timestamptz NOT NULL,
    emitted_at timestamptz NOT NULL,
    title text NOT NULL,
    text text NOT NULL,
    text_length integer NOT NULL CHECK (text_length >= 0),
    source_url text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('live','replay')),
    run_id text NOT NULL,
    author_hash text NOT NULL DEFAULT '',
    received_at timestamptz NOT NULL DEFAULT now(),
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    UNIQUE (run_id, source, source_item_id)
);
CREATE INDEX IF NOT EXISTS raw_events_run_time ON raw_events(run_id, source_created_at);
CREATE INDEX IF NOT EXISTS raw_events_recent ON raw_events(received_at DESC);

-- Independent from raw_events so two consumer groups may safely race.
CREATE TABLE IF NOT EXISTS painpoint_results (
    event_id text PRIMARY KEY,
    run_id text NOT NULL,
    mode text NOT NULL CHECK (mode IN ('live','replay')),
    source_created_at timestamptz NOT NULL,
    emitted_at timestamptz NOT NULL,
    thread_id text NOT NULL,
    source_item_id text NOT NULL,
    source_url text NOT NULL,
    title text NOT NULL,
    text_length integer NOT NULL,
    author_hash text NOT NULL DEFAULT '',
    product text NOT NULL,
    category text NOT NULL,
    is_pain boolean NOT NULL,
    evidence text NOT NULL,
    method text NOT NULL,
    rule_version text NOT NULL,
    classified_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE (run_id, source_item_id)
);
CREATE INDEX IF NOT EXISTS painpoint_results_run_time ON painpoint_results(run_id, source_created_at);
CREATE INDEX IF NOT EXISTS painpoint_results_run_category ON painpoint_results(run_id, category);

CREATE TABLE IF NOT EXISTS painpoint_windows (
    run_id text NOT NULL,
    mode text NOT NULL,
    window_start timestamptz NOT NULL,
    category text NOT NULL,
    record_count integer NOT NULL CHECK (record_count >= 0),
    thread_count integer NOT NULL CHECK (thread_count >= 0),
    total_ai_records integer NOT NULL CHECK (total_ai_records >= 0),
    pain_share double precision NOT NULL CHECK (pain_share >= 0 AND pain_share <= 1),
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(run_id, window_start, category)
);

CREATE TABLE IF NOT EXISTS sequence_signals (
    run_id text NOT NULL,
    window_start timestamptz NOT NULL,
    category text NOT NULL,
    actual_count double precision NOT NULL,
    predicted_count double precision NOT NULL,
    residual double precision NOT NULL,
    anomaly_score double precision NOT NULL,
    is_anomaly boolean NOT NULL,
    history_windows integer NOT NULL,
    model_version text NOT NULL,
    forecast_ready boolean NOT NULL,
    updated_at timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY(run_id, window_start, category)
);

CREATE TABLE IF NOT EXISTS rejected_events (
    id bigserial PRIMARY KEY,
    consumer_role text NOT NULL,
    error_reason text NOT NULL,
    payload text NOT NULL,
    kafka_topic text NOT NULL DEFAULT 'ai_user_feedback',
    kafka_partition integer NOT NULL,
    kafka_offset bigint NOT NULL,
    received_at timestamptz NOT NULL DEFAULT now(),
    UNIQUE(consumer_role, kafka_topic, kafka_partition, kafka_offset)
);

CREATE TABLE IF NOT EXISTS consumer_status (
    consumer_role text PRIMARY KEY,
    group_id text NOT NULL,
    last_heartbeat timestamptz NOT NULL DEFAULT now(),
    last_event_at timestamptz,
    processed_events bigint NOT NULL DEFAULT 0,
    rejected_count bigint NOT NULL DEFAULT 0,
    status text NOT NULL DEFAULT 'starting'
);
