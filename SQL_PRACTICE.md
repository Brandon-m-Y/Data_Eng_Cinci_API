# SQL practice: the Cincinnati crash warehouse

47 interview-style questions against the live `crashes` dataset, in three
tiers. Every question has a worked solution in the [answer key](#answer-key),
and **every solution in that key was run against this database** — the
expected result is printed with it, so you can tell "my query is different"
from "my query is wrong".

Run them in the BigQuery console (project `cincinnati-open-crash-data`,
dataset `crashes`, region `us-east1`), or from `bq query --use_legacy_sql=false`.
The whole dataset is small — a full scan of the fact table is about 40 MB, so
you will not get near the 1 TB free tier no matter how hard you practise.

Figures were verified 2026-10-02. The warehouse only changes when a load runs,
and the feed has published no new crash day since 2026-08-24, so counts should
match for a while. If one drifts, the query is still right.

---

## Before you start: three traps

These are not trivia. Each one has produced a wrong answer in this project,
and each shows up in the questions below.

**1. A row is a person, not a crash.** `fact_crash_person` is at
person-per-crash grain. `COUNT(*)` counts people. Crashes need
`COUNT(DISTINCT instanceid)`. There are 433,160 rows and 221,289 crashes —
get this wrong and you are off by 1.96x.

**2. The panel has a column called `crashes`, and so does the dataset.** In a
query that scans `ml_crash_panel`, an unqualified `crashes.dim_date` is read
as *the column* `crashes`, and the query fails with a confusing message about
aliases. Backtick the table reference, or alias the panel.

**3. `coding_era` is not a date range.** The two severity coding schemes
overlap heavily. Never filter on era as a proxy for time, or group by the
numeric prefix of a severity code.

---

## Schema cheat sheet

**Fact** — `fact_crash_person`, 433,160 rows, one per person per crash.

| Column | Notes |
|---|---|
| `instanceid` | The crash key. Stable across publishes; shared by every person in one crash |
| `localreportno` | Police report number |
| `crash_date`, `crash_datetime` | Event time |
| `reporting_lag_hours` | Crash to report, in hours |
| `is_injured`, `is_fatal` | 0/1 at the **person** level |
| `age`, `person_count` | Person attributes |
| `latitude`, `longitude` | Privacy-fuzzed, re-randomized every publish. Do not trust them |
| `distance_to_cbd_m` | Derived from those coordinates, so equally fuzzy |
| `crash_hash`, `loaded_at`, `socrata_updated_at` | ETL bookkeeping |
| `*_key` columns | Foreign keys to the dimensions below |

**Dimensions**

| Table | Join on | Useful columns |
|---|---|---|
| `dim_date` | `date_key` | `full_date`, `year`, `month_name`, `year_month`, `quarter`, `day_of_week_name`, `is_weekend` |
| `dim_crash_date` (view) | `crash_date_key` | `crash_year`, `crash_day_name`, `crash_is_weekend`, `crash_year_month` |
| `dim_reported_date` (view) | `reported_date_key` | `reported_year`, `reported_day_name` |
| `dim_time` | `crash_time_key` = `time_key` | `hour_24`, `time_of_day_band`, `is_rush_hour`, `is_overnight` |
| `dim_location` | `location_key` | `cpd_neighborhood`, `community_council_neighborhood`, `sna_neighborhood`, `zip`, `road_class_desc`, `is_intersection` |
| `dim_conditions` | `conditions_key` | `weather`, `light_conditions`, `is_dark`, `road_conditions`, `is_slick`, `is_adverse_weather` |
| `dim_crash_type` | `crash_type_key` | `manner_of_crash`, `crash_severity`, `crash_severity_rank`, `is_injury_crash`, `is_fatal_crash`, `coding_era` |
| `dim_person_profile` | `person_profile_key` | `type_of_person`, `is_motorist`, `unit_category`, `gender`, `age_band`, `injury_severity`, `injury_severity_rank` |

Watch the values: `type_of_person` is `Driver` / `Occupant` / `Pedestrian` /
`Unknown`, while `unit_category` spells it `Pedestrian/Skater`. Picking the
wrong one silently returns zero rows.

**Other tables**

| Table | What it is |
|---|---|
| `ml_crash_panel` | 205,746 rows: 53 neighborhoods × 3,882 days, complete grid including zero-crash days. Columns `crashes`, `crashes_next_7`, `lag_7`, `lag_364`, `roll_28`, `roll_91`, calendar flags |
| `etl_load_log` | One row per pipeline run: `load_mode`, `status`, timings, `crashes_new` / `crashes_changed` / `crashes_removed` |
| `etl_lease` | The single-writer lock. One row |
| `stg_crash_person` | Raw strings from the last load only — currently a 90-day delta window, **not** the full history |
| `vw_stg_crash_person_clean` | The cleaning view over staging: typed, labelled, with natural keys |

---

# Easy

Single tables, simple joins, basic aggregation.

**E1.** How many person-rows and how many distinct crashes does the fact table
hold? *(the grain trap, in one query)*

**E2.** What are the earliest and latest crash dates? Exclude the handful of
rows before 2010 — they are data-entry errors.

**E3.** How many crashes happened in 2025?

**E4.** Which 10 CPD neighborhoods have the most crashes?

**E5.** Across all person-rows, how many people were injured and how many
died?

**E6.** Count crashes by day of the week. Which day is worst?

**E7.** What are the five most common weather conditions by person-rows?

**E8.** List every injury severity level with how many people fall into it,
ordered from least to most severe. *(there is a column for the ordering —
find it rather than hand-writing a CASE)*

**E9.** What is the average age of people involved, and how many rows have no
age at all?

**E10.** How many crashes happened at an intersection, how many did not, and
how many are unclassified?

**E11.** Crashes per year for 2021 through 2025.

**E12.** Which five hours of the day see the most crashes?

**E13.** How many person-rows are missing a latitude?

**E14.** Show the five most recent pipeline runs with their mode, status and
row counts.

**E15.** How many neighborhoods, days and total rows are in the ML panel?

---

# Medium

Multiple joins, window functions, CTEs, conditional aggregation.

**M1.** For each year 2022–2025, show the number of crashes and the
percentage that were injury crashes. *(careful: `is_injury_crash` is a
crash-level flag sitting on person-level rows)*

**M2.** Produce one row per neighborhood with 2024 and 2025 crash counts as
two side-by-side columns. Order by 2025.

**M3.** Rank neighborhoods by the number of fatal crashes, showing the rank
alongside the count.

**M4.** Monthly crash counts since 2024, with a 3-month moving average.

**M5.** For each neighborhood with at least 2,000 person-rows, what
percentage of crashes happened in the dark? Show the five highest.

**M6.** For every neighborhood, the three most common manners of crash.
*(one window function and no subquery filter, if you use `QUALIFY`)*

**M7.** Average reporting lag in hours by crash severity. Which severity is
reported slowest, and does the direction surprise you?

**M8.** Which combination of weekday and time-of-day band produces the most
crashes?

**M9.** How many crashes involved at least one pedestrian?

**M10.** For each neighborhood since 2022, show crashes per year alongside the
previous year and the change.

**M11.** What are the worst five neighborhood-days in the panel, by crash
count?

**M12.** For each neighborhood, the mean crashes per day and the single worst
day. Top five by mean.

**M13.** What is the distribution of people per crash? Show the count and
percentage of crashes for each size.

**M14.** How many crashes involve more than one category of road user — say a
car and a bicycle?

**M15.** How many crashes were reported in a later calendar year than they
happened? *(both role-playing date views in one query)*

**M16.** For 2018–2020, count crashes by year and `coding_era`. What does the
result tell you about using era as a date filter?

**M17.** Median age of people by unit category, for the five most common
categories.

**M18.** From the ETL log, average and worst run duration by load mode.

---

# Hard

Window frames, gaps and islands, self-joins, data-quality detection, and the
traps above.

**H1.** Find the longest run of consecutive days on which a neighborhood
recorded zero crashes. Return the neighborhood, the start and end of the run,
and its length. *(classic gaps-and-islands; the panel includes zero rows, so
the days are all there)*

**H2.** For each neighborhood, find the 7-day window with the most crashes
ever, and return the day that window ends. *(you cannot nest an analytic
function inside another window's `ORDER BY` — BigQuery will reject it)*

**H3.** Show the first and last crash date for each `coding_era`. Then explain
why `WHERE coding_era = 'Pre-2019'` is not a way to select old crashes.

**H4.** For every neighborhood with at least 1,000 crashes, compute both
`COUNT(*)` and `COUNT(DISTINCT instanceid)` and the ratio between them. Which
neighborhoods have the most people per crash?

**H5.** Sum panel crashes by calendar month, joining the panel to `dim_date`.
First write it the natural way, with no table alias on the panel, and read the
error. Then fix it. *(this is trap 2; the error message does not say what is
actually wrong)*

**H6.** Compare January–August 2026 against the same months in 2025, per
neighborhood, with percent change. Only include neighborhoods with at least
100 crashes in 2025. *(why does the full-year comparison not work here?)*

**H7.** Every person-row of a crash should agree on crash-level attributes.
Find any crash whose rows disagree on `crash_type_key`.

**H8.** For each year since 2023, compute the 50th, 90th and 99th percentile
of reporting lag in hours.

**H9.** The panel's `crashes_next_7` label should equal the sum of crashes
over the following seven days. Verify it: count mismatches, and count rows
where the label is NULL. Explain the NULLs.

**H10.** Which neighborhood has the highest *rate* of fatal crashes, as fatal
crashes per 1,000 crashes? Require at least 2,000 crashes so one death in a
quiet neighborhood does not win.

**H11.** Find anomalous days: where a neighborhood's crash count was at least
three times its own 91-day rolling mean. Exclude cells whose rolling mean is
below 0.5, or you will drown in noise from quiet neighborhoods.

**H12.** Reconcile staging against the fact table. How many staged crashes are
missing from the fact, and how many crashes inside the staged date window are
missing from staging? What date window does staging currently hold?

**H13.** For each crash involving a pedestrian, pivot the person rows into
counts of pedestrians and motorists. Return the five crashes with the most
pedestrians.

**H14.** For each neighborhood, find the date by which half of all its crashes
since 2010 had occurred. Which neighborhood reached its halfway point
earliest, and what does an early date imply about its trend?

---

# Answer key

Each query below was executed against the warehouse on 2026-10-02 and the
result is shown. Your query does not have to match mine — only the answer
does.

---

## Easy

### E1

```sql
SELECT COUNT(*) AS person_rows, COUNT(DISTINCT instanceid) AS crashes
FROM crashes.fact_crash_person;
```

`433160` person-rows, `221289` crashes. The ratio is 1.96 people per crash.

### E2

```sql
SELECT MIN(crash_date) AS first_crash, MAX(crash_date) AS last_crash
FROM crashes.fact_crash_person
WHERE crash_date >= '2010-01-01';
```

`2010-02-23` to `2026-08-24`. Without the filter the minimum is `1900-02-06`,
which is two bad rows the cleaning view could not rescue.

### E3

```sql
SELECT COUNT(DISTINCT instanceid) AS crashes_2025
FROM crashes.fact_crash_person
WHERE EXTRACT(YEAR FROM crash_date) = 2025;
```

`14968`.

### E4

```sql
SELECT l.cpd_neighborhood, COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
GROUP BY 1
ORDER BY crashes DESC
LIMIT 10;
```

`WESTWOOD` 17,350 · `C. B. D. / RIVERFRONT` 15,439 · `WEST PRICE HILL` 9,903 ·
`BONDHILL` 9,317.

### E5

```sql
SELECT SUM(is_injured) AS injured_people,
       SUM(is_fatal)   AS fatalities,
       COUNT(*)        AS people
FROM crashes.fact_crash_person;
```

64,617 injured and 437 killed out of 433,160 people.

### E6

```sql
SELECT d.crash_day_name, COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_date d USING (crash_date_key)
GROUP BY 1
ORDER BY crashes DESC;
```

Friday 36,936, then Thursday 33,465, Wednesday 33,260, Tuesday 33,041. Eight
rows, not seven — the unknown-date member is in there too.

### E7

```sql
SELECT c.weather, COUNT(*) AS person_rows
FROM crashes.fact_crash_person f
JOIN crashes.dim_conditions c USING (conditions_key)
GROUP BY 1
ORDER BY 2 DESC
LIMIT 5;
```

Clear 291,497 · Cloudy 69,988 · Rain 58,542 · Snow 8,379.

### E8

```sql
SELECT p.injury_severity, p.injury_severity_rank, COUNT(*) AS people
FROM crashes.fact_crash_person f
JOIN crashes.dim_person_profile p USING (person_profile_key)
GROUP BY 1, 2
ORDER BY p.injury_severity_rank;
```

Six levels. `O - No Apparent Injury` dominates at 368,203;
`C - Possible` 31,768; `B - Suspected Minor` 28,648. `injury_severity_rank` is
the column to sort by — the labels do not sort alphabetically into severity
order.

### E9

```sql
SELECT ROUND(AVG(age), 1) AS avg_age,
       COUNT(age)         AS with_age,
       COUNT(*) - COUNT(age) AS missing_age
FROM crashes.fact_crash_person;
```

Average 37.5 over 378,153 rows; 55,007 have no usable age. `AVG` ignores
NULLs, and `COUNT(age)` versus `COUNT(*)` is how you see how many it skipped.

### E10

```sql
SELECT l.is_intersection, COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
GROUP BY 1;
```

NULL 120,913 · true 29,657 · false 70,719. They sum to exactly 221,289, which
is worth checking: if a crash's rows mapped to several locations the groups
would overlap and the sum would exceed the total.

### E11

```sql
SELECT EXTRACT(YEAR FROM crash_date) AS yr, COUNT(DISTINCT instanceid) AS crashes
FROM crashes.fact_crash_person
WHERE crash_date BETWEEN '2021-01-01' AND '2025-12-31'
GROUP BY 1
ORDER BY 1;
```

2021: 17,240 · 2022: 15,918 · 2023: 14,998 · 2024: 15,427 · 2025: 14,968.

### E12

```sql
SELECT t.hour_24, COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_time t ON f.crash_time_key = t.time_key
GROUP BY 1
ORDER BY crashes DESC
LIMIT 5;
```

16:00 is worst with 17,931, then 17:00 (17,317) and 15:00 (16,837). The
afternoon commute, not the morning one. Note this dimension joins on
`crash_time_key = time_key`, so `USING` does not work.

### E13

```sql
SELECT COUNTIF(latitude IS NULL) AS no_lat, COUNT(*) AS rows_total
FROM crashes.fact_crash_person;
```

305 of 433,160.

### E14

```sql
SELECT load_mode, status, started_at, fact_inserted, crashes_new, crashes_removed
FROM crashes.etl_load_log
ORDER BY started_at DESC
LIMIT 5;
```

Recent deltas all show 0 new and 0 removed — correct, because the feed has
published no new crash day since 2026-08-24.

### E15

```sql
SELECT COUNT(DISTINCT cell_id) AS neighborhoods,
       COUNT(DISTINCT day)     AS days,
       COUNT(*)                AS panel_rows
FROM crashes.ml_crash_panel;
```

53 × 3,882 = 205,746 exactly. The grid is complete, including days with zero
crashes, which is what makes H1 and H9 possible.

---

## Medium

### M1

```sql
WITH crash AS (
  SELECT f.instanceid,
         ANY_VALUE(EXTRACT(YEAR FROM f.crash_date)) AS yr,
         ANY_VALUE(t.is_injury_crash)               AS is_injury
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_crash_type t USING (crash_type_key)
  GROUP BY f.instanceid
)
SELECT yr, COUNT(*) AS crashes,
       ROUND(100 * AVG(CAST(is_injury AS INT64)), 1) AS pct_injury
FROM crash
WHERE yr BETWEEN 2022 AND 2025
GROUP BY yr
ORDER BY yr;
```

2022: 21.4% · 2023: 22.0% · 2024: 20.8% · 2025: 20.2%.

The CTE collapsing to one row per crash is the point. Drop it and average
`is_injury_crash` straight over person-rows and you get 27.0 / 27.4 / 25.8 /
25.1 — five to six points too high every year, because each crash is weighted
by how many people were in it and injury crashes involve more people. The
wrong answer looks completely plausible on its own.

### M2

```sql
SELECT l.cpd_neighborhood,
       COUNT(DISTINCT IF(EXTRACT(YEAR FROM f.crash_date) = 2024, f.instanceid, NULL)) AS y2024,
       COUNT(DISTINCT IF(EXTRACT(YEAR FROM f.crash_date) = 2025, f.instanceid, NULL)) AS y2025
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
GROUP BY 1
ORDER BY y2025 DESC;
```

`WESTWOOD` 1,272 → 1,164 · `C. B. D. / RIVERFRONT` 1,221 → 1,119 ·
`BONDHILL` 629 → 707.

`COUNT(DISTINCT IF(...))` is the pivot idiom: the `IF` returns NULL for the
other year and `COUNT` skips NULLs.

### M3

```sql
SELECT l.cpd_neighborhood,
       COUNT(DISTINCT f.instanceid) AS fatal_crashes,
       RANK() OVER (ORDER BY COUNT(DISTINCT f.instanceid) DESC) AS rnk
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
JOIN crashes.dim_crash_type t USING (crash_type_key)
WHERE t.is_fatal_crash
GROUP BY 1
ORDER BY rnk;
```

`WESTWOOD` 31 · `BONDHILL` 24 · `C. B. D. / RIVERFRONT` 23.

A window function may wrap an aggregate in the same `SELECT`; the window is
applied after grouping.

### M4

```sql
WITH m AS (
  SELECT DATE_TRUNC(crash_date, MONTH) AS mth, COUNT(DISTINCT instanceid) AS crashes
  FROM crashes.fact_crash_person
  WHERE crash_date >= '2024-01-01'
  GROUP BY 1
)
SELECT mth, crashes,
       ROUND(AVG(crashes) OVER (ORDER BY mth ROWS BETWEEN 2 PRECEDING AND CURRENT ROW), 1) AS ma3
FROM m
ORDER BY mth;
```

2024-01: 1,203 (ma 1,203.0) · 2024-02: 1,039 (1,121.0) · 2024-03: 1,237
(1,159.7). The first two rows average over fewer than three months — if you
want them NULL instead, you have to say so explicitly.

### M5

```sql
SELECT l.cpd_neighborhood, COUNT(*) AS person_rows,
       ROUND(100 * AVG(CAST(c.is_dark AS INT64)), 1) AS pct_dark
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
JOIN crashes.dim_conditions c USING (conditions_key)
GROUP BY 1
HAVING person_rows >= 2000
ORDER BY pct_dark DESC
LIMIT 5;
```

`FAIRVIEW` 32.8% · `OVER-THE-RHINE` 29.4% · `EAST PRICE HILL` 29.0%.

`AVG` over a boolean cast to INT64 is the compact way to write "percentage
where true".

### M6

```sql
SELECT cpd_neighborhood, manner_of_crash, crashes
FROM (
  SELECT l.cpd_neighborhood, t.manner_of_crash, COUNT(DISTINCT f.instanceid) AS crashes
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  JOIN crashes.dim_crash_type t USING (crash_type_key)
  GROUP BY 1, 2
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY l.cpd_neighborhood
    ORDER BY COUNT(DISTINCT f.instanceid) DESC) <= 3
)
ORDER BY cpd_neighborhood, crashes DESC;
```

`AVONDALE`: Angle 2,388 · Rear-End 2,104 · Not Collision Between Two Motor
Vehicles 1,479.

`QUALIFY` filters on a window function without a wrapping subquery — the
BigQuery feature most worth knowing if you come from Postgres.

### M7

```sql
SELECT t.crash_severity,
       ROUND(AVG(f.reporting_lag_hours), 1) AS avg_lag_hours,
       COUNT(*) AS person_rows
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_type t USING (crash_type_key)
WHERE f.reporting_lag_hours IS NOT NULL
GROUP BY 1
ORDER BY avg_lag_hours DESC;
```

`Serious Injury Suspected` is slowest at 50.5 hours. `Property Damage Only` is
15.4 and `Fatal` is only 12.9.

The direction is worth pausing on: fatal crashes are reported fastest because
an officer is already on scene, while serious-injury cases often get amended
later. Severity does not predict lag the way intuition suggests.

### M8

```sql
SELECT d.crash_day_name, tm.time_of_day_band, COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_date d USING (crash_date_key)
JOIN crashes.dim_time tm ON f.crash_time_key = tm.time_key
GROUP BY 1, 2
ORDER BY crashes DESC
LIMIT 5;
```

Friday Evening Rush, 9,016. The top three are all Evening Rush.

### M9

```sql
SELECT COUNT(DISTINCT f.instanceid) AS crashes_with_pedestrian
FROM crashes.fact_crash_person f
JOIN crashes.dim_person_profile p USING (person_profile_key)
WHERE p.type_of_person = 'Pedestrian';
```

`4438`. Use `type_of_person`, not `unit_category` — the latter spells it
`Pedestrian/Skater`, and `= 'Pedestrian'` there returns nothing at all without
raising an error.

### M10

```sql
WITH y AS (
  SELECT l.cpd_neighborhood AS hood,
         EXTRACT(YEAR FROM f.crash_date) AS yr,
         COUNT(DISTINCT f.instanceid) AS crashes
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  WHERE f.crash_date >= '2022-01-01'
  GROUP BY 1, 2
)
SELECT hood, yr, crashes,
       LAG(crashes) OVER (PARTITION BY hood ORDER BY yr) AS prev_yr,
       crashes - LAG(crashes) OVER (PARTITION BY hood ORDER BY yr) AS change
FROM y
ORDER BY hood, yr;
```

`AVONDALE`: 2022 587 → 2023 499 (−88) → 2024 492 (−7).

### M11

```sql
SELECT cell_id, day, crashes
FROM crashes.ml_crash_panel
ORDER BY crashes DESC, day
LIMIT 5;
```

16 crashes in one neighborhood-day, twice: `C. B. D. / RIVERFRONT` on
2018-01-17 and `WESTWOOD` on 2022-01-28. Both are winter days — see H11.

### M12

```sql
SELECT cell_id,
       ROUND(AVG(crashes), 3)  AS mean_per_day,
       ROUND(AVG(roll_28), 3)  AS mean_roll28,
       MAX(crashes)            AS worst_day
FROM crashes.ml_crash_panel
GROUP BY 1
ORDER BY mean_per_day DESC
LIMIT 5;
```

`WESTWOOD` 3.655/day · `C. B. D. / RIVERFRONT` 3.136 · `WEST PRICE HILL`
2.039. The mean of `roll_28` tracks the mean of `crashes` almost exactly,
which is a decent sanity check that the rolling feature is centred correctly.

### M13

```sql
WITH per_crash AS (
  SELECT instanceid, COUNT(*) AS people
  FROM crashes.fact_crash_person
  GROUP BY 1
)
SELECT people, COUNT(*) AS n_crashes,
       ROUND(100 * COUNT(*) / SUM(COUNT(*)) OVER (), 2) AS pct
FROM per_crash
GROUP BY 1
ORDER BY people;
```

1 person: 58,181 (26.29%) · 2: 131,749 (59.54%) · 3: 21,301 (9.63%).

`SUM(COUNT(*)) OVER ()` — an aggregate inside a window — is the idiom for
"percentage of the whole" without a second pass.

### M14

```sql
SELECT COUNT(*) AS crashes_with_multiple_unit_categories
FROM (
  SELECT f.instanceid
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_person_profile p USING (person_profile_key)
  GROUP BY f.instanceid
  HAVING COUNT(DISTINCT p.unit_category) > 1
);
```

`91025` — 41% of all crashes.

### M15

```sql
SELECT COUNT(DISTINCT f.instanceid) AS crossed_year_boundary
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_date cd USING (crash_date_key)
JOIN crashes.dim_reported_date rd ON f.reported_date_key = rd.reported_date_key
WHERE rd.reported_year > cd.crash_year;
```

`263`. Two role-playing views over the same `dim_date`, joined on different
keys — the reason they exist is so this query reads clearly instead of
needing two aliases of one table.

### M16

```sql
SELECT EXTRACT(YEAR FROM f.crash_date) AS yr, t.coding_era,
       COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_type t USING (crash_type_key)
WHERE f.crash_date BETWEEN '2018-01-01' AND '2020-12-31'
GROUP BY 1, 2
ORDER BY yr, coding_era;
```

2018 splits 18,098 `Pre-2019` against **85** already coded `2019+`. 2019 is
18,015 `2019+` against a single `Pre-2019` crash.

So the eras do mostly follow time, but the boundary is ragged in both
directions. Filtering on era to mean "before 2019" silently drops 85 crashes
and picks up one. See H3 for how bad it gets over the full history.

### M17

```sql
SELECT p.unit_category,
       APPROX_QUANTILES(f.age, 2)[OFFSET(1)] AS median_age,
       COUNT(*) AS people
FROM crashes.fact_crash_person f
JOIN crashes.dim_person_profile p USING (person_profile_key)
WHERE f.age IS NOT NULL
GROUP BY 1
ORDER BY people DESC
LIMIT 5;
```

Passenger Car 31 · SUV 36 · Pickup 42.

`APPROX_QUANTILES(x, 2)[OFFSET(1)]` is the median: split into 2 buckets, take
the middle boundary. For p90 use `APPROX_QUANTILES(x, 100)[OFFSET(90)]`.

### M18

```sql
SELECT load_mode, COUNT(*) AS loads,
       ROUND(AVG(TIMESTAMP_DIFF(finished_at, started_at, SECOND)), 1) AS avg_seconds,
       MAX(TIMESTAMP_DIFF(finished_at, started_at, SECOND))           AS slowest
FROM crashes.etl_load_log
WHERE finished_at IS NOT NULL
GROUP BY 1
ORDER BY avg_seconds DESC;
```

`reprocess` 306.0s avg · `full` 109.5s · `delta` 81.8s (slowest 197s).

---

## Hard

### H1

```sql
WITH d AS (
  SELECT cell_id, day, crashes,
         DATE_SUB(day, INTERVAL ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY day) DAY) AS grp
  FROM crashes.ml_crash_panel
  WHERE crashes = 0
)
SELECT cell_id, MIN(day) AS run_start, MAX(day) AS run_end, COUNT(*) AS quiet_days
FROM d
GROUP BY cell_id, grp
ORDER BY quiet_days DESC
LIMIT 5;
```

`O'BRYONVILLE`, 176 consecutive quiet days from 2024-12-05 to 2025-05-29. It
holds the next two places as well.

The trick: for rows that are consecutive by date, `day` minus the row number
is constant. Subtracting the row number as an interval collapses each run to a
single group key. This only works because the panel is a complete grid —
against the fact table the absent days are simply missing rows, and the
pattern breaks.

### H2

```sql
WITH r AS (
  SELECT cell_id, day,
         SUM(crashes) OVER (
           PARTITION BY cell_id ORDER BY day
           ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS roll7
  FROM crashes.ml_crash_panel
)
SELECT cell_id, day AS week_ending, roll7
FROM r
QUALIFY ROW_NUMBER() OVER (PARTITION BY cell_id ORDER BY roll7 DESC, day) = 1
ORDER BY roll7 DESC
LIMIT 5;
```

`C. B. D. / RIVERFRONT` 52 crashes in the week ending 2018-01-19 ·
`WESTWOOD` 47 ending 2019-12-14.

The CTE is not stylistic. Putting the `SUM(...) OVER (...)` directly inside
the `ORDER BY` of the `ROW_NUMBER` window fails with
`Analytic function not allowed in Window ORDER BY`. Windows cannot nest; you
must materialise the inner one first.

### H3

```sql
SELECT t.coding_era, MIN(f.crash_date) AS first_seen, MAX(f.crash_date) AS last_seen,
       COUNT(DISTINCT f.instanceid) AS crashes
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_type t USING (crash_type_key)
WHERE f.crash_date >= '2010-01-01'
GROUP BY 1
ORDER BY 1;
```

| era | first | last | crashes |
|---|---|---|---:|
| `2019+` | 2010-02-23 | 2026-08-24 | 120,870 |
| `Pre-2019` | 2012-01-02 | 2025-04-23 | 100,413 |

The ranges overlap across thirteen years. `coding_era` describes **which
coding scheme a row uses**, not when the crash happened — the same report can
carry old-scheme severity long after 2019, and some pre-2019 crashes were
entered or amended under the new scheme. Filter on `crash_date` for time, and
use `coding_era` only to avoid comparing scales that are not comparable.

### H4

```sql
SELECT l.cpd_neighborhood,
       COUNT(*)                      AS person_rows,
       COUNT(DISTINCT f.instanceid)  AS crashes,
       ROUND(COUNT(*) / COUNT(DISTINCT f.instanceid), 2) AS people_per_crash
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
GROUP BY 1
HAVING crashes >= 1000
ORDER BY people_per_crash DESC
LIMIT 5;
```

`S.. CUMMINSVILLE` and `BONDHILL` both 2.08, `CAMP WASHINGTON` 2.07. The
spread is narrow — around 2 everywhere — which is exactly why the grain trap
is dangerous. A wrong answer here is not obviously wrong; it is just
consistently about twice too big.

### H5

The natural way, with no alias on the panel:

```sql
SELECT d.year_month, SUM(crashes) AS total
FROM crashes.ml_crash_panel
JOIN crashes.dim_date d ON day = d.full_date
GROUP BY 1;
```

```
400 Aliases referenced in the from clause must refer to preceding scans,
and cannot refer to columns on those scans. crashes refers to a column and
must be qualified with a table name.
```

Nothing in that message mentions the dataset. What happened: the panel is in
scope and it has a column named `crashes`, so in `crashes.dim_date` the parser
reads `crashes` as that column and `dim_date` as a field of it.

Backtick the table reference, and alias the panel so the column is unambiguous
too:

```sql
SELECT d.year_month, SUM(p.crashes) AS total
FROM crashes.ml_crash_panel p
JOIN `crashes.dim_date` d ON p.day = d.full_date
GROUP BY 1
ORDER BY 1 DESC;
```

2026-08: 628 · 2026-07: 1,133 · 2026-06: 1,035. August is short because the
feed stops at 2026-08-24.

### H6

```sql
WITH w AS (
  SELECT l.cpd_neighborhood AS hood, EXTRACT(YEAR FROM f.crash_date) AS yr, f.instanceid
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  WHERE EXTRACT(MONTH FROM f.crash_date) BETWEEN 1 AND 8
    AND EXTRACT(YEAR FROM f.crash_date) IN (2025, 2026)
),
p AS (
  SELECT hood,
         COUNT(DISTINCT IF(yr = 2025, instanceid, NULL)) AS y2025,
         COUNT(DISTINCT IF(yr = 2026, instanceid, NULL)) AS y2026
  FROM w GROUP BY hood
)
SELECT hood, y2025, y2026, ROUND(100 * (y2026 - y2025) / y2025, 1) AS pct_change
FROM p
WHERE y2025 >= 100
ORDER BY pct_change DESC;
```

`N/A` +8.8% · `HARTWELL` +4.7% · `WEST END` +3.1%.

The month filter is the whole question. 2026 data stops at 2026-08-24, so a
full-year comparison would show every neighborhood down by a third — an
artefact of the publication lag, not a safety improvement. Restricting both
years to January–August makes them comparable, and even then August 2026 is
partial.

`N/A` appearing at the top is a reminder that it is a real neighborhood value
in this feed, not a NULL.

### H7

```sql
SELECT COUNT(*) AS crashes_with_inconsistent_type
FROM (
  SELECT instanceid
  FROM crashes.fact_crash_person
  GROUP BY instanceid
  HAVING COUNT(DISTINCT crash_type_key) > 1
);
```

`0`. Crash-level attributes are genuinely consistent across a crash's person
rows. This is the shape of query worth keeping as a data-quality assertion —
the pipeline runs several like it during a load.

### H8

```sql
SELECT EXTRACT(YEAR FROM crash_date) AS yr,
       APPROX_QUANTILES(reporting_lag_hours, 100)[OFFSET(50)] AS p50,
       APPROX_QUANTILES(reporting_lag_hours, 100)[OFFSET(90)] AS p90,
       APPROX_QUANTILES(reporting_lag_hours, 100)[OFFSET(99)] AS p99
FROM crashes.fact_crash_person
WHERE reporting_lag_hours IS NOT NULL AND crash_date >= '2023-01-01'
GROUP BY 1
ORDER BY 1;
```

2023: p50 0.02h, p90 1.42h, p99 83.9h · 2024: 0.03 / 1.52 / 104.3 · 2025:
0.03 / 1.70 / 92.4.

A median of about one minute and a p99 of three to four days: most reports are
filed on the spot, and a long tail is amended much later. Reporting lag is
**not** the reason the data arrives 39 days late — that is the publication
lag, a different quantity entirely.

### H9

```sql
WITH chk AS (
  SELECT cell_id, day, crashes_next_7,
         SUM(crashes) OVER (
           PARTITION BY cell_id ORDER BY day
           ROWS BETWEEN 1 FOLLOWING AND 7 FOLLOWING) AS forward_7
  FROM crashes.ml_crash_panel
)
SELECT COUNTIF(crashes_next_7 != forward_7) AS mismatches,
       COUNTIF(crashes_next_7 IS NULL)      AS null_labels,
       COUNT(*)                             AS rows_checked
FROM chk;
```

0 mismatches, 371 NULL labels, 205,746 rows checked.

The label is the forward sum over days *d+1* through *d+7* — note
`1 FOLLOWING`, not `CURRENT ROW`, which would leak the target day into its own
feature. The 371 NULLs are the last 7 days of each of the 53 cells
(53 × 7 = 371): their future does not exist yet, so they must be excluded from
training rather than treated as zero.

### H10

```sql
SELECT l.cpd_neighborhood,
       COUNT(DISTINCT f.instanceid) AS crashes,
       COUNT(DISTINCT IF(t.is_fatal_crash, f.instanceid, NULL)) AS fatal,
       ROUND(1000 * COUNT(DISTINCT IF(t.is_fatal_crash, f.instanceid, NULL))
             / COUNT(DISTINCT f.instanceid), 2) AS fatal_per_1000
FROM crashes.fact_crash_person f
JOIN crashes.dim_location l USING (location_key)
JOIN crashes.dim_crash_type t USING (crash_type_key)
GROUP BY 1
HAVING crashes >= 2000
ORDER BY fatal_per_1000 DESC
LIMIT 5;
```

`WINTON HILLS` 5.73 per 1,000 (14 of 2,443) · `SPRING GROVE VILLAGE` 3.10 ·
`CARTHAGE` 2.71.

Compare with M3, where `WESTWOOD` led on raw fatal counts and does not appear
here at all. Rates and counts rank differently, and the volume floor is what
keeps the rate list from being noise — 14 deaths is still a small numerator,
so treat the ordering as suggestive.

### H11

```sql
SELECT cell_id, day, crashes, ROUND(roll_91, 2) AS roll_91
FROM crashes.ml_crash_panel
WHERE roll_91 > 0.5 AND crashes >= 3 * roll_91
ORDER BY crashes - roll_91 DESC
LIMIT 5;
```

`C. B. D. / RIVERFRONT` 2018-01-17: 16 crashes against a 3.23 baseline.
`WESTWOOD` and `CLIFTON` both spike on 2022-01-28.

Several neighborhoods spiking on the same day is the signal worth noticing —
that is weather, not a local change. 2022-01-28 was a winter storm. A model
without weather features cannot explain these days, which is the argument for
loading `ml_weather_daily`.

### H12

```sql
WITH staged AS (
  SELECT DISTINCT instanceid, DATE(crash_datetime) AS d
  FROM crashes.vw_stg_crash_person_clean
),
win AS (SELECT MIN(d) AS lo, MAX(d) AS hi FROM staged)
SELECT
  (SELECT COUNT(*) FROM staged s
    WHERE NOT EXISTS (SELECT 1 FROM crashes.fact_crash_person f
                       WHERE f.instanceid = s.instanceid)) AS staged_not_in_fact,
  (SELECT COUNT(DISTINCT f.instanceid) FROM crashes.fact_crash_person f, win
    WHERE f.crash_date BETWEEN win.lo AND win.hi
      AND NOT EXISTS (SELECT 1 FROM staged s
                       WHERE s.instanceid = f.instanceid)) AS fact_not_in_staged,
  (SELECT lo FROM win) AS window_start,
  (SELECT hi FROM win) AS window_end;
```

0 and 0, over the window 2026-05-26 to 2026-08-24.

Both zeros matter and they mean different things. Nothing staged is missing
from the fact, so the last load applied completely. And nothing in the fact
inside that window is absent from staging, so no crash has silently
disappeared upstream. The window is 90 days wide because the last run was a
delta — this query against a freshly-run `--full` would cover all of history.

### H13

```sql
SELECT f.instanceid, f.crash_date,
       COUNTIF(p.type_of_person = 'Pedestrian') AS pedestrians,
       COUNTIF(p.is_motorist)                   AS motorists,
       COUNT(*)                                 AS people
FROM crashes.fact_crash_person f
JOIN crashes.dim_person_profile p USING (person_profile_key)
GROUP BY 1, 2
HAVING pedestrians > 0
ORDER BY pedestrians DESC, people DESC
LIMIT 5;
```

The worst is 4 pedestrians among 7 people, on 2025-07-30.

`COUNTIF` is the compact form of `SUM(CASE WHEN ... THEN 1 ELSE 0 END)`, and
it is how you pivot a long table into counted columns without a `PIVOT`
clause.

### H14

```sql
WITH daily AS (
  SELECT l.cpd_neighborhood AS hood, f.crash_date AS d,
         COUNT(DISTINCT f.instanceid) AS c
  FROM crashes.fact_crash_person f
  JOIN crashes.dim_location l USING (location_key)
  WHERE f.crash_date >= '2010-01-01'
  GROUP BY 1, 2
),
cum AS (
  SELECT hood, d,
         SUM(c) OVER (PARTITION BY hood ORDER BY d)
         / SUM(c) OVER (PARTITION BY hood) AS share
  FROM daily
)
SELECT hood, MIN(d) AS halfway_date
FROM cum
WHERE share >= 0.5
GROUP BY hood
ORDER BY halfway_date;
```

`O'BRYONVILLE` 2018-08-29 · `COLUMBIA / TUSCULUM` 2018-10-07 · `CALIFORNIA`
2018-10-10.

Two windows over the same partition: a running total ordered by date, divided
by an unordered total for the whole partition. Omitting `ORDER BY` is what
makes the denominator the partition sum rather than another running total.

An early halfway date means crashes were front-loaded — that neighborhood is
getting quieter over time. A date after the midpoint of the period means the
opposite.

---

## Where to go next

Questions this dataset supports that are not written above, if you want to
keep going:

- Rebuild `ml_crash_panel`'s `lag_364` from the fact table and check it
  matches, including across leap years.
- Write the staging validation checks from `Star_Schema_ETL.sql` section 3 as
  standalone queries — they are real assertions that run on every load.
- Reproduce the deletion cap: given a candidate set of removed crashes,
  express "more than 1% of the window, or more than 100, whichever is larger".
- Find crashes whose person rows map to more than one `location_key`, and work
  out whether that is the fuzzed address or a genuine amendment.
