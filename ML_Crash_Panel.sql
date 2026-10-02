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
-- (0 of 221,289 at that measurement). Section 3 reports current disagreements
-- across all fact rows before any date filtering. MAX provides deterministic
-- placement when the publisher supplies inconsistent person rows.
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
DECLARE p_min_cell_crashes INT64 DEFAULT 100;  -- a cell needs this much history to join
DECLARE p_end             DATE;

-- Downtown anchor for distance_to_cbd_km: Fountain Square, Fifth and Vine.
-- Star_Schema_ETL.sql Section 5 declares the same point for the row-level
-- fact column and documents how it was checked; change both together.
-- ST_GEOGPOINT takes longitude first.
DECLARE cbd_lon FLOAT64 DEFAULT -84.5125;
DECLARE cbd_lat FLOAT64 DEFAULT  39.1011;

SET p_end = (SELECT DATE_SUB(MAX(crash_date), INTERVAL p_end_buffer_days DAY)
             FROM crashes.fact_crash_person
             WHERE crash_date <= CURRENT_DATE());

ASSERT p_cell_scheme IN ('cpd_neighborhood', 'sna_neighborhood', 'community_council_neighborhood')
  AS 'p_cell_scheme must name a dim_location neighborhood column';

-- Declared primary keys are NOT ENFORCED: reject duplicate join keys before
-- joining or calculating window functions, rather than discovering fan-out later.
ASSERT NOT EXISTS (
  SELECT day FROM crashes.ml_weather_daily GROUP BY day HAVING COUNT(*) > 1
) AS 'Duplicate weather day: exactly one station observation per day is required';
ASSERT NOT EXISTS (
  SELECT cell_scheme, cell_id FROM crashes.ml_cell_attributes
  GROUP BY cell_scheme, cell_id HAVING COUNT(*) > 1
) AS 'Duplicate cell attribute key: (cell_scheme, cell_id) must be unique';

-- Step 2 of the spec: one row per crash. MAX is deterministic; disagreements
-- in source placement are reported separately over the unfiltered fact in Section 3.
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
-- These are retrospective event-time features. They do not reconstruct which
-- reports/amendments were available at a historical prediction issue time.
-- The end buffer matures targets, not feature availability. Target-day observed
-- weather must not be used as if it were a weather forecast issued beforehand.
-- Which cells the panel covers. A neighborhood joins only once it has
-- p_min_cell_crashes crashes across the whole window: one misspelling, or one
-- crash geocoded into a place the city doesn't really use, would otherwise add
-- a cell that is ~100% zeros and drag down every pooled model. The threshold
-- is self-maintaining -- a genuinely new neighborhood crosses it on its own --
-- and excluded cells are listed in the Section 3 report, so a cell sitting
-- just under it is visible rather than silently dropped.
CREATE TEMP TABLE panel_cells AS
SELECT cell_id, COUNT(*) AS crashes
FROM crash_cells
WHERE cell_id IS NOT NULL
GROUP BY cell_id
HAVING COUNT(*) >= p_min_cell_crashes;

ASSERT (SELECT COUNT(*) FROM panel_cells) > 0
  AS 'No neighborhood reaches p_min_cell_crashes; check the cell scheme and the window';

-- Static geography per cell: how far the neighborhood sits from downtown.
-- Derived from the fact's own coordinates rather than seeded into
-- ml_cell_attributes, so there is nothing to maintain by hand and it can
-- never drift from whichever cell scheme is in use.
--
-- The centre is the MARGINAL MEDIAN of the cell's crash coordinates, not the
-- mean. The published coordinates are fuzzed per row (~106 m) and about 3.7%
-- sit over 2 km from their own address block, so a mean would chase those
-- outliers; with hundreds of crashes per cell the median ignores both. This
-- is the distance from the cell's centre to downtown, which is what a model
-- wants from a static cell attribute -- not the median of the per-crash
-- distances, which is a different and noisier quantity.
CREATE TEMP TABLE cell_distance AS
SELECT
  c.cell_id,
  ROUND(ST_DISTANCE(
          ST_GEOGPOINT(APPROX_QUANTILES(f.longitude, 2)[OFFSET(1)],
                       APPROX_QUANTILES(f.latitude,  2)[OFFSET(1)]),
          ST_GEOGPOINT(cbd_lon, cbd_lat)) / 1000, 3) AS distance_to_cbd_km
FROM crash_cells c
JOIN crashes.fact_crash_person f USING (instanceid)
WHERE c.cell_id IS NOT NULL AND f.latitude IS NOT NULL AND f.longitude IS NOT NULL
GROUP BY c.cell_id;

-- A cell with no usable coordinate at all would join to NULL and quietly
-- become a missing feature for every one of its ~3,900 rows.
ASSERT NOT EXISTS (
  SELECT 1 FROM panel_cells pc
  LEFT JOIN cell_distance cd USING (cell_id)
  WHERE cd.distance_to_cbd_km IS NULL
) AS 'A panel cell has no usable coordinates, so its distance to downtown is unknown';

-- The joined tables are backtick-quoted because the target column is named
-- `crashes`, like the dataset: after FROM grid, a bare crashes.dim_date
-- would resolve as a field of that column.
CREATE TEMP TABLE panel AS
WITH cells AS (
  SELECT cell_id FROM panel_cells
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
  g.crashes,                                      -- same-day count: history, not the label

  -- FORECAST TARGET: crashes in this cell over the next 7 days (d+1 .. d+7).
  -- NULL for the last 7 days of the panel, where the window would run past
  -- p_end and silently return a short sum. The grid is dense (every cell x
  -- every day), so ROWS and RANGE agree here. The horizon is deliberately
  -- literal in both the frame and the column name: changing it means editing
  -- both, plus crash-panel-spec.md.
  IF(g.day <= DATE_SUB(p_end, INTERVAL 7 DAY),
     SUM(g.crashes) OVER wnext, NULL)             AS crashes_next_7,

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

  -- Distance from the cell's centre to downtown (Fountain Square), in km.
  -- Static per cell and constant over time, so it carries no leakage: it
  -- separates dense central cells from outlying ones without telling the
  -- model anything about the future.
  cd.distance_to_cbd_km,

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
LEFT JOIN cell_distance cd            ON cd.cell_id = g.cell_id
WINDOW w     AS (PARTITION BY g.cell_id ORDER BY g.day),
       w28   AS (w ROWS BETWEEN 28 PRECEDING AND 1 PRECEDING),
       w91   AS (w ROWS BETWEEN 91 PRECEDING AND 1 PRECEDING),
       wnext AS (w ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING);

-- Sum: every crash with a cell lands in exactly one panel row.
ASSERT (SELECT SUM(crashes) FROM panel)
     = (SELECT SUM(crashes) FROM panel_cells)
  AS 'Sum check failed: panel crashes != crashes in the cells the panel covers';

-- Row count: the grid is complete, cells x days, with no duplicate cell-day
-- (a second weather row per day would fail here).
ASSERT (SELECT COUNT(*) FROM panel)
     = (SELECT COUNT(*) FROM panel_cells) * (DATE_DIFF(p_end, p_start, DAY) + 1)
  AS 'Row count check failed: panel is not cells x days';
ASSERT (SELECT COUNT(*) FROM panel)
     = (SELECT COUNT(DISTINCT FORMAT('%s|%t', cell_id, day)) FROM panel)
  AS 'Row count check failed: duplicate (cell_id, day) rows';

-- Target: recompute it from the seven strictly-following days of the same
-- cell. The lag checks below prove the features never see the future. This
-- proves the label does, and that it covers exactly seven days.
ASSERT NOT EXISTS (
  SELECT 1
  FROM panel p
  LEFT JOIN (
    SELECT a.cell_id, a.day, SUM(b.crashes) AS next_7
    FROM panel a
    JOIN panel b
      ON b.cell_id = a.cell_id
     AND b.day BETWEEN DATE_ADD(a.day, INTERVAL 1 DAY) AND DATE_ADD(a.day, INTERVAL 7 DAY)
    GROUP BY a.cell_id, a.day
    HAVING COUNT(*) = 7
  ) r ON r.cell_id = p.cell_id AND r.day = p.day
  WHERE p.crashes_next_7 IS DISTINCT FROM r.next_7
) AS 'Target check failed: crashes_next_7 is not the next seven days of its own cell';

-- Exactly the last seven days of every cell are unlabelled, no more and no less.
ASSERT (SELECT COUNTIF(crashes_next_7 IS NULL) FROM panel)
     = (SELECT COUNT(*) FROM panel_cells) * 7
  AS 'Target check failed: rows without a target are not exactly the last seven days per cell';

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
ASSERT (SELECT SAFE_DIVIDE(COUNTIF(crashes = 0), COUNT(*)) FROM panel) < 0.95
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
       COUNTIF(crashes_next_7 IS NOT NULL)                AS labelled_rows,
       ROUND(AVG(crashes_next_7), 2)                      AS mean_next_7,
       ROUND(SAFE_DIVIDE(COUNTIF(crashes_next_7 = 0), COUNTIF(crashes_next_7 IS NOT NULL)), 3)
                                                          AS zero_fraction_next_7,
       ROUND(SAFE_DIVIDE(COUNTIF(crashes = 0), COUNT(*)), 3) AS zero_fraction,
       ROUND(SAFE_DIVIDE(VARIANCE(crashes), AVG(crashes)), 2) AS dispersion,
       MAX(crashes)                                       AS max_cell_day,
       ROUND(MIN(distance_to_cbd_km), 2)                  AS nearest_cell_km,
       ROUND(MAX(distance_to_cbd_km), 2)                  AS farthest_cell_km,
       ANY_VALUE(exposure_source)                         AS exposure_source
FROM crashes.ml_crash_panel
GROUP BY cell_scheme;

-- Distance sanity, printed every build: the nearest cell to the anchor should
-- be the central business district and the farthest should be on the city
-- edge. If the anchor is ever moved or mistyped, this ordering breaks here
-- before anything trains on it.
WITH ranked AS (
  SELECT cell_id, ANY_VALUE(distance_to_cbd_km) AS km
  FROM crashes.ml_crash_panel GROUP BY cell_id
),
ends AS (
  SELECT cell_id, km,
         ROW_NUMBER() OVER (ORDER BY km)      AS near_rank,
         ROW_NUMBER() OVER (ORDER BY km DESC) AS far_rank
  FROM ranked
)
SELECT IF(near_rank <= 5, 'nearest', 'farthest') AS edge, cell_id, km
FROM ends
WHERE near_rank <= 5 OR far_rank <= 5
ORDER BY km;

SELECT split,
       MIN(day)                                           AS first_day,
       MAX(day)                                           AS last_day,
       COUNT(*)                                           AS panel_rows,
       ROUND(AVG(crashes), 3)                             AS mean_crashes,
       ROUND(SAFE_DIVIDE(COUNTIF(crashes = 0), COUNT(*)), 3) AS zero_fraction
FROM crashes.ml_crash_panel
GROUP BY split
ORDER BY first_day;

-- Placement quality over ALL fact rows, before the panel date filter or MAX
-- rollup can hide disagreements. JSON preserves NULL as a distinct value.
WITH placements AS (
  SELECT f.instanceid, scheme,
         COUNT(DISTINCT TO_JSON_STRING(f.crash_date)) AS date_values,
         COUNT(DISTINCT TO_JSON_STRING(NULLIF(UPPER(CASE scheme
           WHEN 'cpd_neighborhood' THEN l.cpd_neighborhood
           WHEN 'sna_neighborhood' THEN l.sna_neighborhood
           WHEN 'community_council_neighborhood' THEN l.community_council_neighborhood
         END), 'N/A'))) AS cell_values
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  CROSS JOIN UNNEST(['cpd_neighborhood', 'sna_neighborhood',
                    'community_council_neighborhood']) AS scheme
  GROUP BY f.instanceid, scheme
)
SELECT scheme AS cell_scheme,
       COUNTIF(date_values > 1) AS crashes_disagree_on_day,
       COUNTIF(cell_values > 1) AS crashes_disagree_on_cell,
       COUNTIF(date_values > 1 OR cell_values > 1) AS crashes_with_placement_disagreement
FROM placements
GROUP BY scheme;

-- Neighborhoods the minimum-history threshold held out. A real neighborhood
-- sitting just under the threshold should be visible here, not silently
-- missing; a line with a handful of crashes and a name close to a real
-- neighborhood is the misspelling the threshold exists to catch.
WITH bounds AS (
  SELECT MAX(cell_scheme) AS scheme, MIN(day) AS first_day, MAX(day) AS last_day
  FROM crashes.ml_crash_panel
),
crash_cell AS (
  SELECT f.instanceid,
         MAX(NULLIF(UPPER(CASE (SELECT scheme FROM bounds)
           WHEN 'cpd_neighborhood'               THEN l.cpd_neighborhood
           WHEN 'sna_neighborhood'               THEN l.sna_neighborhood
           WHEN 'community_council_neighborhood' THEN l.community_council_neighborhood
         END), 'N/A')) AS cell_id
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  WHERE f.crash_date BETWEEN (SELECT first_day FROM bounds) AND (SELECT last_day FROM bounds)
  GROUP BY f.instanceid
)
SELECT (SELECT scheme FROM bounds) AS cell_scheme,
       cell_id                     AS excluded_cell,
       COUNT(*)                    AS crashes_in_window
FROM crash_cell
WHERE cell_id IS NOT NULL
  AND cell_id NOT IN (SELECT DISTINCT cell_id FROM crashes.ml_crash_panel)
GROUP BY cell_id
ORDER BY crashes_in_window DESC;
