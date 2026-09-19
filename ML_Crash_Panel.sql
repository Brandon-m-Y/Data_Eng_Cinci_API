-- ============================================================
-- Cincinnati Crash Space-Time Panel — ML table, BigQuery
-- Spec: crash-panel-spec.md (v1 grain: neighborhood x day)
-- Built from the star schema (fact_crash_person + dim_location + dim_date),
-- so it inherits the cleaning view and stays current with every load.
-- Run order: Section 1 (idempotent DDL), Section 2 (build + assertions),
--            Section 3 (report). Run_Pipeline.py runs all three after every
--            --delta / --full load, or alone with --panel.
-- ============================================================


-- ============================================================
-- WHY A PANEL: the fact holds only crashes, so every row is a positive event
-- and it can't say how likely a crash is. A COMPLETE grid of (cell x day)
-- rows adds the missing negatives: a cell with no crash that day has
-- crashes = 0. The model trains on these counts, so predictions are rates.
--
-- GRAIN: one row per (cell_id, day). cell_id is the neighborhood under
-- p_cell_scheme; the column is generic so a later hex or road-segment grain
-- only needs to redefine it. Time grain is fixed at day for v1.
--
-- CRASH COUNT = distinct instanceid, not fact rows. The fact is one row per
-- person (~1.96 per crash); counting rows would inflate multi-occupant
-- crashes and bias the panel toward severe events. Measured: neighborhood
-- and crash date never differ between person rows of the same crash
-- (0 of 221,289), so the crash-level rollup is exact.
--
-- COVERAGE (measured 2026-09 on 221,289 crashes, Run_Pipeline --full):
--   start 2016-01-01  The feed begins Nov 2012. Mid-2013 to mid-2014 runs
--                     ~800 crashes/month against ~1,300+ either side — a
--                     reporting shift, not a trend — and 2015 is still
--                     ramping. From 2016 volume is stable at 1,100-1,750/month.
--                     The Mar-May 2020 COVID dip is real behavior and stays.
--   end MAX(crash_date) - 7 days
--                     The newest day in the feed is partial (2026-08-24 held
--                     1 crash against ~38/day). Reporting lag p99 is 3.7
--                     days, so a 7-day buffer leaves only complete days.
--
-- CELL SCHEMES (p_cell_scheme):
--   cpd_neighborhood               53 neighborhoods + 'N/A' (default)
--   sna_neighborhood               51 neighborhoods
--   community_council_neighborhood 71, incl. sparse boundary combinations
--                                  ('HYDE PARK - OAKLEY', 155 crashes)
-- 'N/A' and NULL cells are dropped (3,543 crashes since 2016 under CPD).
-- ============================================================


-- ============================================================
-- SECTION 1: EXTERNAL FEATURE TABLES (idempotent; empty until loaded)
-- Designed now so the joins in Section 2 already exist. Each is a
-- one-key join because the spine exists: weather broadcasts on day,
-- cell attributes on cell. Until they're loaded, their panel columns are
-- NULL and exposure falls back to hours_in_cell (a constant offset, which
-- the model's intercept absorbs).
-- ============================================================

-- NOAA daily observations, ONE station (CVG = GHCND:USW00093814, or Lunken
-- = GHCND:USW00093812). Exactly one row per day: a second station would
-- duplicate every panel row, and Section 2's row-count assertion would fail.
CREATE TABLE IF NOT EXISTS crashes.ml_weather_daily (
  day          DATE   NOT NULL,
  station_id   STRING,
  tmax_f       FLOAT64,
  tmin_f       FLOAT64,
  prcp_in      FLOAT64,
  snow_in      FLOAT64,
  snwd_in      FLOAT64,   -- snow depth
  awnd_mph     FLOAT64,   -- average wind speed
  PRIMARY KEY (day) NOT ENFORCED
);

-- Static attributes per cell. Keyed by scheme as well as name so each
-- neighborhood scheme can carry its own rows.
CREATE TABLE IF NOT EXISTS crashes.ml_cell_attributes (
  cell_scheme          STRING NOT NULL,   -- 'cpd_neighborhood', ...
  cell_id              STRING NOT NULL,   -- uppercase, as in ml_crash_panel
  road_miles           FLOAT64,           -- OSM; the stand-in exposure
  intersection_count   INT64,
  aadt                 FLOAT64,           -- ODOT average annual daily traffic
  population           INT64,             -- ACS
  PRIMARY KEY (cell_scheme, cell_id) NOT ENFORCED
);


-- ============================================================
-- SECTION 2: BUILD THE PANEL (every load)
-- Builds into a temp table, asserts, and only then replaces
-- crashes.ml_crash_panel, so the published table has always passed its
-- checks. A failed ASSERT fails the job and leaves the old table in place.
-- The panel is small (~200K rows), so a full rebuild each load is cheaper
-- and simpler than maintaining rolling features incrementally.
-- ============================================================

-- Grain and split configuration. Changing the cell scheme or bounds is a
-- config change here, not a rewrite.
DECLARE p_cell_scheme     STRING DEFAULT 'cpd_neighborhood';
DECLARE p_start           DATE   DEFAULT DATE '2016-01-01';
DECLARE p_end_buffer_days INT64  DEFAULT 7;
DECLARE p_valid_start     DATE   DEFAULT DATE '2024-01-01';   -- train < this
DECLARE p_test_start      DATE   DEFAULT DATE '2025-01-01';   -- valid < this <= test
DECLARE p_hours_in_cell   INT64  DEFAULT 24;
DECLARE p_end             DATE;

SET p_end = (SELECT DATE_SUB(MAX(crash_date), INTERVAL p_end_buffer_days DAY)
             FROM crashes.fact_crash_person
             WHERE crash_date <= CURRENT_DATE());

ASSERT p_cell_scheme IN ('cpd_neighborhood', 'sna_neighborhood', 'community_council_neighborhood')
  AS 'p_cell_scheme must name a dim_location neighborhood column';

-- Step 2 of the spec: one row per crash. MAX is exact given the
-- consistency measured above, and deterministic unlike ANY_VALUE.
CREATE TEMP TABLE crash_cells AS
SELECT
  f.instanceid,
  MAX(f.crash_date) AS day,
  MAX(NULLIF(UPPER(CASE p_cell_scheme
        WHEN 'cpd_neighborhood'               THEN l.cpd_neighborhood
        WHEN 'sna_neighborhood'               THEN l.sna_neighborhood
        WHEN 'community_council_neighborhood' THEN l.community_council_neighborhood
      END), 'N/A')) AS cell_id
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
WHERE f.crash_date BETWEEN p_start AND p_end
GROUP BY f.instanceid;

-- US federal holidays: the actual date and, for fixed-date holidays, the
-- weekday they're observed on (Saturday -> Friday, Sunday -> Monday). Both
-- are flagged: July 4 traffic happens on the 4th even when the day off is
-- the 3rd. Juneteenth from 2021.
CREATE TEMP FUNCTION nth_weekday(y INT64, m INT64, wd INT64, n INT64) AS (
  -- wd uses BigQuery DAYOFWEEK: 1 = Sunday ... 7 = Saturday
  DATE_ADD(DATE(y, m, 1),
           INTERVAL MOD(wd - EXTRACT(DAYOFWEEK FROM DATE(y, m, 1)) + 7, 7) + 7 * (n - 1) DAY)
);
CREATE TEMP FUNCTION last_weekday(y INT64, m INT64, wd INT64) AS (
  DATE_SUB(LAST_DAY(DATE(y, m, 1)),
           INTERVAL MOD(EXTRACT(DAYOFWEEK FROM LAST_DAY(DATE(y, m, 1))) - wd + 7, 7) DAY)
);
CREATE TEMP FUNCTION observed(d DATE) AS (
  CASE EXTRACT(DAYOFWEEK FROM d)
    WHEN 7 THEN DATE_SUB(d, INTERVAL 1 DAY)
    WHEN 1 THEN DATE_ADD(d, INTERVAL 1 DAY)
    ELSE d
  END
);

CREATE TEMP TABLE holidays AS
WITH years AS (
  SELECT y FROM UNNEST(GENERATE_ARRAY(EXTRACT(YEAR FROM p_start) - 1,
                                      EXTRACT(YEAR FROM p_end) + 1)) AS y
),
fixed AS (
  SELECT d FROM years, UNNEST([DATE(y, 1, 1), DATE(y, 7, 4), DATE(y, 11, 11), DATE(y, 12, 25)]) AS d
  UNION ALL
  SELECT DATE(y, 6, 19) FROM years WHERE y >= 2021
),
floating AS (
  SELECT d FROM years, UNNEST([
    nth_weekday(y, 1, 2, 3),    -- MLK Day: 3rd Monday of January
    nth_weekday(y, 2, 2, 3),    -- Presidents Day: 3rd Monday of February
    last_weekday(y, 5, 2),      -- Memorial Day: last Monday of May
    nth_weekday(y, 9, 2, 1),    -- Labor Day: 1st Monday of September
    nth_weekday(y, 10, 2, 2),   -- Columbus Day: 2nd Monday of October
    nth_weekday(y, 11, 5, 4)    -- Thanksgiving: 4th Thursday of November
  ]) AS d
)
SELECT DISTINCT d AS day
FROM (SELECT d FROM fixed
      UNION ALL SELECT observed(d) FROM fixed
      UNION ALL SELECT d FROM floating);

-- Step 4: the spine. CROSS JOIN every cell with every day, then LEFT JOIN
-- the sparse counts and zero-fill.
-- Step 5: lags are PARTITION BY cell_id (within cell) and strictly
-- backward. The rolling frames end at 1 PRECEDING — the SQL form of the
-- spec's shift(1) before rolling(). A frame ending at CURRENT ROW would
-- include the target in its own feature. The spine is complete, so N rows
-- back is exactly N days back.
-- The joined tables are backtick-quoted because the target column is named
-- `crashes`, like the dataset: after FROM grid, a bare crashes.dim_date
-- would resolve as a field of that column.
CREATE TEMP TABLE panel AS
WITH cells AS (
  SELECT DISTINCT cell_id FROM crash_cells WHERE cell_id IS NOT NULL
),
days AS (
  SELECT day FROM UNNEST(GENERATE_DATE_ARRAY(p_start, p_end)) AS day
),
counts AS (
  SELECT cell_id, day, COUNT(*) AS crashes
  FROM crash_cells
  WHERE cell_id IS NOT NULL
  GROUP BY cell_id, day
),
grid AS (
  SELECT c.cell_id, d.day, IFNULL(n.crashes, 0) AS crashes
  FROM cells c
  CROSS JOIN days d
  LEFT JOIN counts n ON n.cell_id = c.cell_id AND n.day = d.day
)
SELECT
  p_cell_scheme                                   AS cell_scheme,
  g.cell_id,
  g.day,
  g.crashes,                                      -- target

  -- Lags (NULL until a cell has enough history, like pandas NaN)
  LAG(g.crashes, 7)   OVER w                      AS lag_7,
  LAG(g.crashes, 364) OVER w                      AS lag_364,   -- same weekday, prior year
  IF(COUNT(*) OVER w28 >= 14, AVG(g.crashes) OVER w28, NULL) AS roll_28,   -- min_periods 14
  IF(COUNT(*) OVER w91 >= 30, AVG(g.crashes) OVER w91, NULL) AS roll_91,   -- min_periods 30

  -- Calendar
  dd.day_of_week                                  AS dow,       -- 1 = Sunday ... 7 = Saturday
  dd.month_number                                 AS month,
  dd.is_weekend,
  h.day IS NOT NULL                               AS is_holiday,
  SIN(2 * ACOS(-1) * EXTRACT(DAYOFYEAR FROM g.day) / 365.25) AS doy_sin,
  COS(2 * ACOS(-1) * EXTRACT(DAYOFYEAR FROM g.day) / 365.25) AS doy_cos,

  -- Weather (broadcast on day)
  wx.tmax_f, wx.tmin_f, wx.prcp_in, wx.snow_in, wx.snwd_in, wx.awnd_mph,

  -- Static cell attributes (join on cell)
  ca.road_miles, ca.intersection_count, ca.aadt, ca.population,

  -- Step 6: exposure. Pass LN(exposure) as the model offset
  -- (statsmodels offset=, LightGBM init_score, XGBoost base_margin).
  -- Ideal is AADT x road_miles x hours; each factor missing from
  -- ml_cell_attributes drops out to 1, and exposure_source says which
  -- form every row got. The build refuses to mix forms (see ASSERTs).
  IFNULL(ca.aadt, 1) * IFNULL(ca.road_miles, 1) * p_hours_in_cell AS exposure,
  LN(IFNULL(ca.aadt, 1) * IFNULL(ca.road_miles, 1) * p_hours_in_cell) AS log_exposure,
  CASE
    WHEN ca.aadt IS NOT NULL AND ca.road_miles IS NOT NULL THEN 'aadt_x_road_miles_x_hours'
    WHEN ca.road_miles IS NOT NULL                        THEN 'road_miles_x_hours'
    WHEN ca.aadt IS NOT NULL                              THEN 'aadt_x_hours'
    ELSE 'hours_only'
  END                                             AS exposure_source,

  -- Step 7: temporal split. Never shuffle — adjacent days of one cell are
  -- nearly identical, so a random split leaks cell identity.
  CASE
    WHEN g.day < p_valid_start THEN 'train'
    WHEN g.day < p_test_start  THEN 'valid'
    ELSE 'test'
  END                                             AS split
FROM grid g
JOIN `crashes.dim_date` dd              ON dd.full_date = g.day
LEFT JOIN holidays h                  ON h.day = g.day
LEFT JOIN `crashes.ml_weather_daily` wx ON wx.day = g.day
LEFT JOIN `crashes.ml_cell_attributes` ca
       ON ca.cell_scheme = p_cell_scheme AND ca.cell_id = g.cell_id
WINDOW w   AS (PARTITION BY g.cell_id ORDER BY g.day),
       w28 AS (w ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING),
       w91 AS (w ROWS BETWEEN 91 PRECEDING AND 1 PRECEDING);

-- Sum: every crash with a cell lands in exactly one panel row.
ASSERT (SELECT SUM(crashes) FROM panel)
     = (SELECT COUNT(*) FROM crash_cells WHERE cell_id IS NOT NULL)
  AS 'Sum check failed: panel crashes != crashes with a cell';

-- Row count: the grid is complete, cells x days, with no duplicate cell-day
-- (a second weather row per day would fail here).
ASSERT (SELECT COUNT(*) FROM panel)
     = (SELECT COUNT(DISTINCT cell_id) FROM crash_cells WHERE cell_id IS NOT NULL)
       * (DATE_DIFF(p_end, p_start, DAY) + 1)
  AS 'Row count check failed: panel is not cells x days';
ASSERT (SELECT COUNT(*) FROM panel)
     = (SELECT COUNT(DISTINCT FORMAT('%s|%t', cell_id, day)) FROM panel)
  AS 'Row count check failed: duplicate (cell_id, day) rows';

-- Leakage: recompute every lag feature from strictly prior days of the same
-- cell (q.day <= p.day - 1) by self-join and require an exact match. A frame
-- that included the current day, or crossed cells, would fail here.
CREATE TEMP FUNCTION matches_prior(a FLOAT64, b FLOAT64) AS (
  IFNULL((a IS NULL AND b IS NULL) OR ABS(a - b) < 1e-9, FALSE)
);
ASSERT NOT EXISTS (
  SELECT 1
  FROM panel p
  LEFT JOIN (
    SELECT p.cell_id, p.day,
           AVG(IF(q.day >= DATE_SUB(p.day, INTERVAL 28 DAY), q.crashes, NULL)) AS avg_28,
           COUNTIF(q.day >= DATE_SUB(p.day, INTERVAL 28 DAY))                  AS n_28,
           AVG(q.crashes)                                                     AS avg_91,
           COUNT(*)                                                           AS n_91,
           MAX(IF(q.day = DATE_SUB(p.day, INTERVAL 7 DAY), q.crashes, NULL))  AS lag_7
    FROM panel p
    JOIN panel q
      ON q.cell_id = p.cell_id
     AND q.day BETWEEN DATE_SUB(p.day, INTERVAL 91 DAY) AND DATE_SUB(p.day, INTERVAL 1 DAY)
    GROUP BY p.cell_id, p.day
  ) r ON r.cell_id = p.cell_id AND r.day = p.day
  LEFT JOIN panel y ON y.cell_id = p.cell_id AND y.day = DATE_SUB(p.day, INTERVAL 364 DAY)
  WHERE NOT matches_prior(p.lag_7,   r.lag_7)
     OR NOT matches_prior(p.lag_364, y.crashes)
     OR NOT matches_prior(p.roll_28, IF(IFNULL(r.n_28, 0) >= 14, r.avg_28, NULL))
     OR NOT matches_prior(p.roll_91, IF(IFNULL(r.n_91, 0) >= 30, r.avg_91, NULL))
) AS 'Leakage check failed: a lag feature does not match its strictly-prior recomputation';

-- Exposure: one form across the whole panel (a half-loaded attributes table
-- would otherwise mix AADT-scale and hours-scale offsets), and positive.
ASSERT (SELECT COUNT(DISTINCT exposure_source) FROM panel) = 1
  AS 'Exposure check failed: rows mix exposure forms; load ml_cell_attributes for every cell';
ASSERT NOT EXISTS (SELECT 1 FROM panel WHERE NOT exposure > 0)
  AS 'Exposure check failed: exposure must be positive';

-- Grain sanity: the spec's stop condition. Above 95% zeros the grain is too
-- fine for a count model.
ASSERT (SELECT COUNTIF(crashes = 0) / COUNT(*) FROM panel) < 0.95
  AS 'Zero fraction >= 95%: grain is too fine';

-- Publish. Clustered by split so train/valid/test reads prune.
CREATE OR REPLACE TABLE crashes.ml_crash_panel
CLUSTER BY split, cell_id
OPTIONS (description = 'Crash space-time panel (cell x day) for count models. Built by ML_Crash_Panel.sql; see crash-panel-spec.md.')
AS SELECT * FROM panel;


-- ============================================================
-- SECTION 3: PANEL REPORT (every build)
-- Expect a zero fraction around 50-70% and a mean near 0.8 at
-- neighborhood x day. dispersion = variance / mean: above 1 means
-- overdispersed, so Poisson is the floor and negative binomial the baseline.
-- ============================================================

SELECT cell_scheme,
       COUNT(DISTINCT cell_id)                            AS cells,
       MIN(day)                                           AS first_day,
       MAX(day)                                           AS last_day,
       COUNT(*)                                           AS panel_rows,
       SUM(crashes)                                       AS crashes,
       ROUND(AVG(crashes), 3)                             AS mean_crashes,
       ROUND(COUNTIF(crashes = 0) / COUNT(*), 3)          AS zero_fraction,
       ROUND(VARIANCE(crashes) / AVG(crashes), 2)         AS dispersion,
       MAX(crashes)                                       AS max_cell_day,
       ANY_VALUE(exposure_source)                         AS exposure_source
FROM crashes.ml_crash_panel
GROUP BY cell_scheme;

SELECT split,
       MIN(day)                                           AS first_day,
       MAX(day)                                           AS last_day,
       COUNT(*)                                           AS panel_rows,
       ROUND(AVG(crashes), 3)                             AS mean_crashes,
       ROUND(COUNTIF(crashes = 0) / COUNT(*), 3)          AS zero_fraction
FROM crashes.ml_crash_panel
GROUP BY split
ORDER BY first_day;
