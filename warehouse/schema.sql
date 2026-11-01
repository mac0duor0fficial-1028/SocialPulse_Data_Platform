-- SocialPulse warehouse: star schema (PostgreSQL 13+). Fully idempotent: safe to run on every load.

-- ───────────────────────────── Dimensions ─────────────────────────────
CREATE TABLE IF NOT EXISTS dim_location (
    location_key  INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    city          TEXT NOT NULL UNIQUE,
    country       TEXT NOT NULL DEFAULT 'Kenya'
);

CREATE TABLE IF NOT EXISTS dim_platform (
    platform_key   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    platform_name  TEXT NOT NULL UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_key      INTEGER PRIMARY KEY,            -- yyyymmdd
    full_date     DATE NOT NULL UNIQUE,
    year          SMALLINT NOT NULL,
    quarter       SMALLINT NOT NULL,
    month         SMALLINT NOT NULL,
    month_name    TEXT NOT NULL,
    week_of_year  SMALLINT NOT NULL,
    day_of_month  SMALLINT NOT NULL,
    day_of_week   SMALLINT NOT NULL,              -- ISO: Mon=1 .. Sun=7
    day_name      TEXT NOT NULL,
    is_weekend    BOOLEAN NOT NULL
);

INSERT INTO dim_date
SELECT to_char(d, 'YYYYMMDD')::INT, d::DATE,
       EXTRACT(YEAR FROM d)::SMALLINT, EXTRACT(QUARTER FROM d)::SMALLINT, EXTRACT(MONTH FROM d)::SMALLINT,
       trim(to_char(d, 'Month')), EXTRACT(WEEK FROM d)::SMALLINT, EXTRACT(DAY FROM d)::SMALLINT,
       EXTRACT(ISODOW FROM d)::SMALLINT, trim(to_char(d, 'Day')), EXTRACT(ISODOW FROM d) >= 6
FROM generate_series(DATE '2023-01-01', DATE '2035-12-31', INTERVAL '1 day') AS g(d)
ON CONFLICT (date_key) DO NOTHING;

-- Creators are a Slowly Changing Dimension (Type 2): a change to username / niche / city closes
-- the current row (valid_to) and opens a new one, so history is preserved.
CREATE TABLE IF NOT EXISTS dim_creator (
    creator_key   INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,   -- surrogate key
    user_id       TEXT NOT NULL,                                      -- natural / business key
    username      TEXT NOT NULL,
    niche         TEXT NOT NULL,
    city          TEXT NOT NULL,
    location_key  INTEGER NOT NULL REFERENCES dim_location (location_key),
    joined_date   DATE NOT NULL,
    valid_from    DATE NOT NULL,
    valid_to      DATE NOT NULL DEFAULT DATE '9999-12-31',
    is_current    BOOLEAN NOT NULL DEFAULT TRUE,
    CONSTRAINT uq_creator_version UNIQUE (user_id, valid_from),
    CONSTRAINT ck_creator_validity CHECK (valid_from <= valid_to)
);
CREATE UNIQUE INDEX IF NOT EXISTS uq_creator_current ON dim_creator (user_id) WHERE is_current;
CREATE INDEX IF NOT EXISTS ix_creator_lookup ON dim_creator (user_id, valid_from, valid_to);

CREATE TABLE IF NOT EXISTS dim_post (
    post_key            INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    post_id             TEXT NOT NULL UNIQUE,
    content_type        TEXT NOT NULL,
    caption             TEXT NOT NULL,
    posted_at           TIMESTAMPTZ NOT NULL,
    posted_hour_eat     SMALLINT NOT NULL CHECK (posted_hour_eat BETWEEN 0 AND 23),
    posted_day_of_week  SMALLINT NOT NULL CHECK (posted_day_of_week BETWEEN 1 AND 7),
    posted_day_name     TEXT NOT NULL
);

-- ───────────────────────────── Facts ─────────────────────────────
-- Grain: one row per post per publication date.
CREATE TABLE IF NOT EXISTS fact_post_engagement (
    post_key          INTEGER NOT NULL REFERENCES dim_post (post_key),
    creator_key       INTEGER NOT NULL REFERENCES dim_creator (creator_key),
    platform_key      INTEGER NOT NULL REFERENCES dim_platform (platform_key),
    date_key          INTEGER NOT NULL REFERENCES dim_date (date_key),
    likes             INTEGER NOT NULL CHECK (likes >= 0),
    comments          INTEGER NOT NULL CHECK (comments >= 0),
    shares            INTEGER NOT NULL CHECK (shares >= 0),
    impressions       INTEGER NOT NULL CHECK (impressions >= 0),
    total_engagement  INTEGER NOT NULL CHECK (total_engagement = likes + comments + shares),
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (post_key, date_key)
);
CREATE INDEX IF NOT EXISTS ix_fpe_creator ON fact_post_engagement (creator_key);
CREATE INDEX IF NOT EXISTS ix_fpe_date ON fact_post_engagement (date_key);
CREATE INDEX IF NOT EXISTS ix_fpe_platform ON fact_post_engagement (platform_key);

-- Grain: one row per creator per snapshot date.
CREATE TABLE IF NOT EXISTS fact_follower_snapshots (
    creator_key              INTEGER NOT NULL REFERENCES dim_creator (creator_key),
    date_key                 INTEGER NOT NULL REFERENCES dim_date (date_key),
    follower_count           BIGINT NOT NULL CHECK (follower_count >= 0),
    previous_follower_count  BIGINT,
    growth_rate_pct          NUMERIC(12, 4),
    loaded_at                TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (creator_key, date_key)
);
CREATE INDEX IF NOT EXISTS ix_ffs_date ON fact_follower_snapshots (date_key);

-- Grain: one row per creator per day (daily roll-up used for creator comparison).
CREATE TABLE IF NOT EXISTS fact_creator_daily (
    creator_key          INTEGER NOT NULL REFERENCES dim_creator (creator_key),
    date_key             INTEGER NOT NULL REFERENCES dim_date (date_key),
    posts_count          INTEGER NOT NULL,
    likes                BIGINT NOT NULL,
    comments             BIGINT NOT NULL,
    shares               BIGINT NOT NULL,
    impressions          BIGINT NOT NULL,
    total_engagement     BIGINT NOT NULL,
    follower_count       BIGINT NOT NULL,
    engagement_rate_pct  NUMERIC(12, 4),
    loaded_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (creator_key, date_key)
);
CREATE INDEX IF NOT EXISTS ix_fcd_date ON fact_creator_daily (date_key);

-- Grain: one row per hashtag per platform per publication date (hashtag is a degenerate dimension).
CREATE TABLE IF NOT EXISTS fact_hashtag_performance (
    hashtag           TEXT NOT NULL,
    platform_key      INTEGER NOT NULL REFERENCES dim_platform (platform_key),
    date_key          INTEGER NOT NULL REFERENCES dim_date (date_key),
    posts_used        INTEGER NOT NULL,
    likes             BIGINT NOT NULL,
    comments          BIGINT NOT NULL,
    shares            BIGINT NOT NULL,
    impressions       BIGINT NOT NULL,
    total_engagement  BIGINT NOT NULL,
    loaded_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (hashtag, platform_key, date_key)
);
CREATE INDEX IF NOT EXISTS ix_fhp_date ON fact_hashtag_performance (date_key);

-- ───────────────────────────── Observability ─────────────────────────────
CREATE TABLE IF NOT EXISTS etl_load_audit (
    audit_id      BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    run_date      DATE NOT NULL,
    target_table  TEXT NOT NULL,
    rows_loaded   INTEGER NOT NULL,
    loaded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);
