-- =========================================================
-- DWH bootstrap (schemas, roles, meta, ETL utilities, model)
-- Target DB: pg_dwh_openaq (PostgreSQL 13+)
-- =========================================================
BEGIN;

-- ---------- 0) Extensions (безпечні)
CREATE EXTENSION IF NOT EXISTS pgcrypto;   -- gen_random_uuid()

-- ---------- 1) Schemas
CREATE SCHEMA IF NOT EXISTS stg;   -- “як прийшло”
CREATE SCHEMA IF NOT EXISTS proc;  -- нормалізація/пре-процес
CREATE SCHEMA IF NOT EXISTS dm;    -- dimensions + facts
CREATE SCHEMA IF NOT EXISTS svc;   -- мета, сервісні в’ю/агреги

-- ---------- 2) Roles & grants (групові)
DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'etl') THEN
    CREATE USER etl WITH PASSWORD 'etl';
  END IF;
END$$;

GRANT USAGE, CREATE ON SCHEMA stg TO etl;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES    IN SCHEMA stg TO etl;
GRANT USAGE,  SELECT, UPDATE        ON ALL SEQUENCES IN SCHEMA stg TO etl;

-- дефолтні привілеї на нові об’єкти
ALTER DEFAULT PRIVILEGES IN SCHEMA stg
  GRANT SELECT, INSERT, UPDATE, DELETE ON TABLES TO etl;
ALTER DEFAULT PRIVILEGES IN SCHEMA stg
  GRANT USAGE, SELECT, UPDATE ON SEQUENCES TO etl;


-- ---------- 3) svc.Мета та логування
CREATE TABLE IF NOT EXISTS svc.etl_run (
  run_id        UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  pipeline_name TEXT        NOT NULL,
  started_at    TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at   TIMESTAMPTZ,
  status        TEXT        NOT NULL DEFAULT 'running', -- running|ok|failed|partial
  rows_in       BIGINT      NOT NULL DEFAULT 0,
  rows_out      BIGINT      NOT NULL DEFAULT 0,
  msg           TEXT
);

CREATE TABLE IF NOT EXISTS svc.etl_task_log (
  task_id     UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  run_id      UUID NOT NULL REFERENCES svc.etl_run(run_id) ON DELETE CASCADE,
  task_name   TEXT NOT NULL,
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status      TEXT NOT NULL DEFAULT 'running',
  rows_in     BIGINT NOT NULL DEFAULT 0,
  rows_out    BIGINT NOT NULL DEFAULT 0,
  msg         TEXT
);

CREATE TABLE IF NOT EXISTS svc.error_log (
  error_id    BIGSERIAL PRIMARY KEY,
  run_id      UUID,
  task_name   TEXT,
  occurred_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  phase       TEXT,             -- extract/transform/load
  context     JSONB,            -- будь-які додаткові поля
  message     TEXT NOT NULL,
  detail      TEXT
);

-- ---------- 4) STAGING приклад (JSON/CSV сирі записи)
-- (для OpenAQ або будь-якого сенсорного фіда)
CREATE TABLE IF NOT EXISTS stg.measurements_raw (
  load_id     UUID      NOT NULL,         -- зовнішній ідентифікатор завантаження
  src         TEXT      NOT NULL,         -- назва джерела
  payload     JSONB     NOT NULL,         -- сирий JSON рядок (один вимір)
  loaded_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS ix_stg_meas_loaded_at ON stg.measurements_raw(loaded_at);
CREATE INDEX IF NOT EXISTS ix_stg_meas_src       ON stg.measurements_raw(src);

-- Додатково: трекінг завантажених файлів/викликів (для ідемпотентності)
CREATE TABLE IF NOT EXISTS stg.load_registry (
  load_id     UUID PRIMARY KEY,
  src         TEXT NOT NULL,
  src_ref     TEXT,                 -- ім'я файлу/URL/offset
  started_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status      TEXT NOT NULL DEFAULT 'running',
  rows_ingested BIGINT NOT NULL DEFAULT 0,
  checksum    TEXT
);

-- ---------- 5) PROC нормалізація (flat таблиця)
CREATE TABLE IF NOT EXISTS proc.measurements_clean (
  -- бізнес-ключі
  src               TEXT NOT NULL,
  external_id       TEXT,            -- якщо приходить ідентифікатор
  measured_at_utc   TIMESTAMPTZ NOT NULL,
  location_name     TEXT,
  latitude          DOUBLE PRECISION,
  longitude         DOUBLE PRECISION,
  parameter_code    TEXT NOT NULL,   -- pm25, pm10, no2, o3...
  unit              TEXT NOT NULL,   -- µg/m³, ppm...
  value_num         DOUBLE PRECISION,
  -- технічні
  load_id           UUID NOT NULL,
  inserted_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  CONSTRAINT uq_clean UNIQUE (src, external_id, measured_at_utc, parameter_code)
);
CREATE INDEX IF NOT EXISTS ix_clean_measured_at ON proc.measurements_clean(measured_at_utc);
CREATE INDEX IF NOT EXISTS ix_clean_param       ON proc.measurements_clean(parameter_code);
CREATE INDEX IF NOT EXISTS ix_clean_loc         ON proc.measurements_clean(location_name);

-- ---------- 6) DM: довідники (dimensions)
-- dim_date (мінімальний)
CREATE TABLE IF NOT EXISTS dm.dim_date (
  date_id        DATE PRIMARY KEY,     -- YYYY-MM-DD
  year           INT  NOT NULL,
  quarter        INT  NOT NULL,
  month          INT  NOT NULL,
  day            INT  NOT NULL,
  dow            INT  NOT NULL,        -- 1..7
  iso_week       INT  NOT NULL
);

-- dim_location
CREATE TABLE IF NOT EXISTS dm.dim_location (
  location_sk    BIGSERIAL PRIMARY KEY,
  location_name  TEXT NOT NULL,
  latitude       DOUBLE PRECISION,
  longitude      DOUBLE PRECISION,
  src            TEXT,
  CONSTRAINT uq_dim_location UNIQUE (src, location_name)
);

-- dim_parameter
CREATE TABLE IF NOT EXISTS dm.dim_parameter (
  parameter_sk   SMALLSERIAL PRIMARY KEY,
  parameter_code TEXT NOT NULL UNIQUE,     -- pm25/pm10/...
  unit_default   TEXT
);

-- ---------- 7) FACT із партиціюванням по місяцях
CREATE TABLE IF NOT EXISTS dm.fact_measurement (
  fact_id        BIGSERIAL,
  date_id        DATE NOT NULL REFERENCES dm.dim_date(date_id),
  location_sk    BIGINT NOT NULL REFERENCES dm.dim_location(location_sk),
  parameter_sk   SMALLINT NOT NULL REFERENCES dm.dim_parameter(parameter_sk),
  measured_at_utc TIMESTAMPTZ NOT NULL,
  value_num      DOUBLE PRECISION,
  load_id        UUID NOT NULL,
  PRIMARY KEY (fact_id, date_id)
) PARTITION BY RANGE (date_id);

-- функція створення партиції на місяць
CREATE OR REPLACE FUNCTION dm.ensure_month_partition(p_date DATE)
RETURNS VOID LANGUAGE plpgsql AS $$
DECLARE
  p_start DATE := date_trunc('month', p_date)::date;
  p_end   DATE := (p_start + INTERVAL '1 month')::date;
  part    TEXT := format('fact_measurement_%s', to_char(p_start, 'YYYYMM'));
  sql     TEXT;
BEGIN
  IF NOT EXISTS (
    SELECT 1 FROM pg_class c
    JOIN pg_namespace n ON n.oid=c.relnamespace
    WHERE c.relkind='r' AND n.nspname='dm' AND c.relname = part
  ) THEN
    sql := format(
      'CREATE TABLE dm.%I PARTITION OF dm.fact_measurement
       FOR VALUES FROM (%L) TO (%L);',
      part, p_start, p_end
    );
    EXECUTE sql;
  END IF;
END$$;

-- ---------- 8) Утиліти: наповнення dim_date
CREATE OR REPLACE FUNCTION dm.populate_dim_date(p_from DATE, p_to DATE)
RETURNS VOID LANGUAGE sql AS $$
INSERT INTO dm.dim_date(date_id, year, quarter, month, day, dow, iso_week)
SELECT d::date,
       EXTRACT(YEAR FROM d)::int,
       EXTRACT(QUARTER FROM d)::int,
       EXTRACT(MONTH FROM d)::int,
       EXTRACT(DAY FROM d)::int,
       EXTRACT(ISODOW FROM d)::int,
       EXTRACT(WEEK FROM d)::int
FROM generate_series(p_from, p_to, interval '1 day') AS g(d)
ON CONFLICT (date_id) DO NOTHING;
$$;

-- ---------- 9) Трансформація: із stg -> proc (JSON приклад)
CREATE OR REPLACE FUNCTION proc.normalize_from_raw(p_run_id UUID DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_cnt BIGINT := 0;
BEGIN
  INSERT INTO proc.measurements_clean(
    src, external_id, measured_at_utc, location_name, latitude, longitude,
    parameter_code, unit, value_num, load_id
  )
  SELECT
    r.src,
    COALESCE(r.payload->>'id', r.payload->>'uuid'),
    (r.payload->>'date_utc')::timestamptz,
    r.payload->>'location',
    NULLIF(r.payload->>'lat','')::float8,
    NULLIF(r.payload->>'lon','')::float8,
    r.payload->>'parameter',
    r.payload->>'unit',
    NULLIF(r.payload->>'value','')::float8,
    r.load_id
  FROM stg.measurements_raw r
  LEFT JOIN proc.measurements_clean c
    ON c.src = r.src
   AND c.external_id = COALESCE(r.payload->>'id', r.payload->>'uuid')
   AND c.measured_at_utc = (r.payload->>'date_utc')::timestamptz
   AND c.parameter_code   = (r.payload->>'parameter')
  WHERE c.src IS NULL                         -- ідемпотентність
    AND (r.payload->>'date_utc') IS NOT NULL  -- базова валідація
    AND (r.payload->>'parameter') IS NOT NULL;

  GET DIAGNOSTICS v_cnt = ROW_COUNT;

  IF p_run_id IS NOT NULL THEN
    UPDATE svc.etl_run SET rows_out = rows_out + v_cnt WHERE run_id = p_run_id;
  END IF;

  RETURN v_cnt;
EXCEPTION WHEN OTHERS THEN
  INSERT INTO svc.error_log(run_id, task_name, phase, context, message, detail)
  VALUES (p_run_id, 'normalize_from_raw', 'transform', NULL, SQLERRM, SQLSTATE);
  RAISE;
END$$;

-- ---------- 10) Завантаження в DM (upsert dims + fact)
CREATE OR REPLACE FUNCTION dm.load_star_from_clean(p_run_id UUID DEFAULT NULL)
RETURNS BIGINT LANGUAGE plpgsql AS $$
DECLARE
  v_cnt BIGINT := 0;
BEGIN
  -- 1) параметри
  INSERT INTO dm.dim_parameter(parameter_code, unit_default)
  SELECT DISTINCT parameter_code, unit
  FROM proc.measurements_clean
  ON CONFLICT (parameter_code) DO NOTHING;

  -- 2) локації
  INSERT INTO dm.dim_location(location_name, latitude, longitude, src)
  SELECT DISTINCT location_name, latitude, longitude, src
  FROM proc.measurements_clean
  WHERE location_name IS NOT NULL
  ON CONFLICT (src, location_name) DO NOTHING;

  -- 3) date dimension (за діапазоном дат із clean)
  PERFORM dm.populate_dim_date(
    (SELECT date_trunc('day', min(measured_at_utc))::date FROM proc.measurements_clean),
    (SELECT date_trunc('day', max(measured_at_utc))::date FROM proc.measurements_clean)
  );

  -- 4) FACT + створення партицій
  WITH ins AS (
    SELECT
      date_trunc('day', c.measured_at_utc)::date AS date_id,
      (SELECT location_sk FROM dm.dim_location dl
         WHERE dl.src=c.src AND dl.location_name=c.location_name
         ORDER BY location_sk LIMIT 1) AS location_sk,
      (SELECT parameter_sk FROM dm.dim_parameter dp
         WHERE dp.parameter_code=c.parameter_code) AS parameter_sk,
      c.measured_at_utc, c.value_num, c.load_id
    FROM proc.measurements_clean c
  )
  SELECT dm.ensure_month_partition(date_id) FROM (SELECT DISTINCT date_id FROM ins) d;

  INSERT INTO dm.fact_measurement(date_id, location_sk, parameter_sk, measured_at_utc, value_num, load_id)
  SELECT date_id, location_sk, parameter_sk, measured_at_utc, value_num, load_id
  FROM ins
  WHERE location_sk IS NOT NULL AND parameter_sk IS NOT NULL;

  GET DIAGNOSTICS v_cnt = ROW_COUNT;

  IF p_run_id IS NOT NULL THEN
    UPDATE svc.etl_run SET rows_out = rows_out + v_cnt WHERE run_id = p_run_id;
  END IF;

  RETURN v_cnt;
EXCEPTION WHEN OTHERS THEN
  INSERT INTO svc.error_log(run_id, task_name, phase, context, message, detail)
  VALUES (p_run_id, 'load_star_from_clean', 'load', NULL, SQLERRM, SQLSTATE);
  RAISE;
END$$;

-- ---------- 11) SVC в’ю та агрегати
-- Денний агрегат по локації/параметру
CREATE MATERIALIZED VIEW IF NOT EXISTS svc.mv_daily_agg AS
SELECT
  f.date_id,
  l.location_name,
  p.parameter_code,
  avg(f.value_num) AS value_avg,
  min(f.value_num) AS value_min,
  max(f.value_num) AS value_max,
  count(*)         AS samples
FROM dm.fact_measurement f
JOIN dm.dim_location  l ON l.location_sk = f.location_sk
JOIN dm.dim_parameter p ON p.parameter_sk = f.parameter_sk
GROUP BY f.date_id, l.location_name, p.parameter_code;
-- індекси для MV
CREATE INDEX IF NOT EXISTS ix_mv_daily ON svc.mv_daily_agg(date_id, location_name, parameter_code);

-- Актуальний сервісний в’ю для BI
CREATE OR REPLACE VIEW svc.vw_fact_measurement AS
SELECT
  f.measured_at_utc,
  f.value_num,
  d.date_id, d.year, d.month, d.day,
  l.location_sk, l.location_name, l.latitude, l.longitude,
  p.parameter_code, p.unit_default
FROM dm.fact_measurement f
JOIN dm.dim_date      d ON d.date_id = f.date_id
JOIN dm.dim_location  l ON l.location_sk = f.location_sk
JOIN dm.dim_parameter p ON p.parameter_sk = f.parameter_sk;

-- ---------- 12) Індексні підсилення
CREATE INDEX IF NOT EXISTS ix_fact_measured_at ON dm.fact_measurement USING BRIN(measured_at_utc);
CREATE INDEX IF NOT EXISTS ix_fact_loc_param   ON dm.fact_measurement(location_sk, parameter_sk);

-- ---------- 13) Мінімальні ETL-ранери (start/finish)
CREATE OR REPLACE FUNCTION svc.start_run(p_pipeline TEXT)
RETURNS UUID LANGUAGE sql AS $$
  INSERT INTO svc.etl_run(pipeline_name) VALUES (p_pipeline) RETURNING run_id;
$$;

CREATE OR REPLACE FUNCTION svc.finish_run(p_run_id UUID, p_status TEXT DEFAULT 'ok', p_msg TEXT DEFAULT NULL)
RETURNS VOID LANGUAGE sql AS $$
  UPDATE svc.etl_run
     SET finished_at = now(), status = p_status, msg = COALESCE(msg,'') || COALESCE(' | '||p_msg,'')
   WHERE run_id = p_run_id;
$$;

-- ---------- 14) Приклад end-to-end ETL (викликається з cron/Airflow)
-- SELECT * FROM svc.etl_end_to_end('openaq_hourly');
CREATE OR REPLACE FUNCTION svc.etl_end_to_end(p_pipeline TEXT)
RETURNS TABLE(step TEXT, affected BIGINT) LANGUAGE plpgsql AS $$
DECLARE
  v_run UUID;
  v1 BIGINT; v2 BIGINT;
BEGIN
  v_run := svc.start_run(p_pipeline);
  BEGIN
    -- 1) нормалізація
    v1 := proc.normalize_from_raw(v_run);
    RETURN QUERY SELECT 'normalize_from_raw', v1;

    -- 2) завантаження в DM
    v2 := dm.load_star_from_clean(v_run);
    RETURN QUERY SELECT 'load_star_from_clean', v2;

    PERFORM svc.finish_run(v_run, 'ok', 'done');
  EXCEPTION WHEN OTHERS THEN
    INSERT INTO svc.error_log(run_id, task_name, phase, message, detail)
    VALUES (v_run, 'etl_end_to_end', 'overall', SQLERRM, SQLSTATE);
    PERFORM svc.finish_run(v_run, 'failed', SQLERRM);
    RAISE;
  END;
END$$;

COMMIT;

-- ================== Корисні нотатки ==================
-- 1) Завантаження сирих даних:
-- INSERT INTO stg.load_registry(load_id, src, src_ref, status) VALUES ('00000000-0000-0000-0000-000000000001','openaq','page=1','running');
-- INSERT INTO stg.measurements_raw(load_id, src, payload) VALUES ('00000000-0000-0000-0000-000000000001','openaq','{...json...}');
-- UPDATE stg.load_registry SET status='ok', finished_at=now(), rows_ingested=1234 WHERE load_id='00000000-0000-0000-0000-000000000001';

-- 2) Запуск повного ETL:
-- SELECT * FROM svc.etl_end_to_end('openaq_hourly');

-- 3) Рефреш денного MV (по завершенню ETL):
-- REFRESH MATERIALIZED VIEW CONCURRENTLY svc.mv_daily_agg;