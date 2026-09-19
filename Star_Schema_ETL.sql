-- ============================================================
-- Cincinnati Traffic Crash Reports (CPD) — Star Schema, BigQuery
-- Source: Socrata dataset rvmt-pkmq (data.cincinnati-oh.gov)
-- Manual (state-anchored) surrogate key pattern
-- Run order: Sections 1, 2, 7 once (Run_Pipeline.py --setup),
--            Sections 4, 5, 6, 9 every load (--delta or --full).
--            Section 3 is handled by the Python loader (WRITE_TRUNCATE).
--            Section 8 is retired (folded into Section 5).
--            Sections 5 and 9 take query parameters (@window_start, ...)
--            supplied by Run_Pipeline.py.
-- Dataset name `crashes` matches GCP_DATASET in .env
-- ============================================================


-- ============================================================
-- GRAIN: one row per PERSON/UNIT involved in one crash.
-- Verified against the live API (433,160 rows):
--   :id            433,160 distinct -> unique per person-row, but only within
--                                     one Socrata publish: every republish
--                                     regenerates all :id and :version values
--   instanceid     221,289 distinct -> crash level (~1.96 people per crash);
--                                     stable across publishes, so Section 5
--                                     replaces the fact crash by crash on it
--   localreportno  221,289 distinct -> crash level, 1:1 with instanceid
-- instanceid and localreportno are therefore degenerate dimensions.
-- Staging must receive the UNdeduped feed — deduping on instanceid collapses
-- the grain to one row per crash and throws away every passenger and
-- pedestrian. Section 6's persons_per_crash (~1.96) catches it if it happens.
--
-- TWO CODING ERAS: Ohio re-coded its crash report around 2019 and the feed
-- mixes both vintages in the same columns. The numeric prefix is not stable:
--   injuries         '1 - FATAL' (old) and '5 - FATAL' (new) are the same
--                    outcome — the old scale runs fatal->none, the new one
--                    none->fatal
--   crashseverityid  '1','2','3' (old) alongside '201901'..'201905' (new)
--   unittype         '03 - MID SIZE' (old) vs '03 - SPORT UTILITY VEHICLE' (new)
-- So every dimension is keyed on the RAW string and carries a canonical
-- attribute that reconciles the eras. Group by the canonical column; never
-- order by the raw prefix.
-- ============================================================


-- ============================================================
-- SECTION 1: TABLE DDL + MIGRATIONS + CLEANING VIEW (--setup; safe to rerun)
-- Note: BigQuery PK/FK constraints are metadata only (NOT ENFORCED).
-- Uniqueness and referential integrity are guaranteed by the load
-- logic in Sections 4 and 5, not by the engine.
-- ============================================================

-- Staging: raw Socrata payload, all STRING because the JSON API returns every
-- column as a string. Typing happens in the cleaning view below, so a malformed
-- value can't fail the load — it lands, and SAFE_CAST quarantines it where
-- Section 6 can count it. ':' is illegal in BigQuery column names, so the
-- Socrata system fields are renamed; the '_x' merge leftovers are dropped.
-- `dayofweek` is not staged: dim_date derives it.
CREATE TABLE IF NOT EXISTS crashes.stg_crash_person (
  socrata_id                      STRING,   -- :id
  socrata_version                 STRING,   -- :version
  socrata_created_at              STRING,   -- :created_at
  socrata_updated_at              STRING,   -- :updated_at (see watermark note, Section 3)
  instanceid                      STRING,
  localreportno                   STRING,
  crashdate                       STRING,
  datecrashreported               STRING,
  address                         STRING,   -- address_x
  latitude                        STRING,   -- latitude_x
  longitude                       STRING,   -- longitude_x
  zip                             STRING,
  community_council_neighborhood  STRING,
  cpd_neighborhood                STRING,
  sna_neighborhood                STRING,
  roadclass                       STRING,
  roadclassdesc                   STRING,
  crashlocation                   STRING,
  lightconditionsprimary          STRING,
  roadconditionsprimary           STRING,
  roadcontour                     STRING,
  roadsurface                     STRING,
  weather                         STRING,
  mannerofcrash                   STRING,
  crashseverity                   STRING,
  crashseverityid                 STRING,
  typeofperson                    STRING,
  unittype                        STRING,
  gender                          STRING,
  age                             STRING,
  injuries                        STRING
);

-- Role-playing: joined twice from the fact (crash date, reported date).
CREATE TABLE IF NOT EXISTS crashes.dim_date (
  date_key          INT64  NOT NULL,        -- smart key: YYYYMMDD
  full_date         DATE   NOT NULL,
  day_of_week       INT64  NOT NULL,        -- 1 = Sunday ... 7 = Saturday
  day_of_week_name  STRING NOT NULL,
  day_of_week_abbr  STRING NOT NULL,        -- 'MON', same form as the dropped source column
  is_weekend        BOOL   NOT NULL,
  day_of_month      INT64  NOT NULL,
  week_of_year      INT64  NOT NULL,        -- ISO week
  month_number      INT64  NOT NULL,
  month_name        STRING NOT NULL,
  year_month        STRING NOT NULL,        -- '2024-07'
  quarter           INT64  NOT NULL,
  year              INT64  NOT NULL,
  PRIMARY KEY (date_key) NOT ENFORCED
);

-- Minute grain: 1,440 fixed rows. Kept separate from dim_date so the calendar
-- isn't multiplied by 1,440 rows per day.
CREATE TABLE IF NOT EXISTS crashes.dim_time (
  time_key          INT64  NOT NULL,        -- smart key: HHMM (0 .. 2359)
  time_of_day       TIME   NOT NULL,
  hour_24           INT64  NOT NULL,
  hour_12           INT64  NOT NULL,
  am_pm             STRING NOT NULL,
  minute_of_hour    INT64  NOT NULL,
  hour_label        STRING NOT NULL,        -- '15:00-15:59'
  time_of_day_band  STRING NOT NULL,        -- Overnight / Morning Rush / ...
  is_rush_hour      BOOL   NOT NULL,
  is_overnight      BOOL   NOT NULL,
  PRIMARY KEY (time_key) NOT ENFORCED
);

-- Highest-cardinality dimension. Exact lat/long are NOT here: each unit in a
-- crash is geocoded separately, so coordinates made nearly every row unique
-- (19,999 distinct in a 20,000-row sample) and the dimension grew as large as
-- the fact. They live on the fact instead. This keeps the descriptive
-- location — block address, neighborhoods, road class — that analysts group by.
CREATE TABLE IF NOT EXISTS crashes.dim_location (
  location_key                    INT64  NOT NULL,
  location_nk                     STRING NOT NULL,  -- MD5 of the raw attribute tuple
  address                         STRING,           -- block level, '23XX BOUDINOT AV'
  zip                             STRING,           -- NULL unless exactly 5 digits
  community_council_neighborhood  STRING,
  cpd_neighborhood                STRING,           -- whitespace collapsed ('MOUNT  AUBURN')
  sna_neighborhood                STRING,
  road_class_code                 STRING,
  road_class_desc                 STRING,
  crash_location_raw              STRING,
  crash_location                  STRING,           -- prefix stripped
  is_intersection                 BOOL,
  PRIMARY KEY (location_key) NOT ENFORCED
);

-- Junk dimension: built from the combinations actually observed, not the
-- Cartesian product (~79,000 combinations, almost all of which never occur).
CREATE TABLE IF NOT EXISTS crashes.dim_conditions (
  conditions_key        INT64  NOT NULL,
  conditions_nk         STRING NOT NULL,    -- delimited raw tuple
  light_conditions_raw  STRING,
  light_conditions      STRING,
  is_dark               BOOL,
  road_conditions_raw   STRING,
  road_conditions       STRING,
  is_slick              BOOL,               -- wet / snow / ice / slush / standing water
  road_contour_raw      STRING,
  road_contour          STRING,
  is_curve              BOOL,
  is_grade              BOOL,
  road_surface_raw      STRING,
  road_surface          STRING,
  weather_raw           STRING,
  weather               STRING,
  is_adverse_weather    BOOL,               -- anything but clear / cloudy / unknown
  PRIMARY KEY (conditions_key) NOT ENFORCED
);

-- Manner + severity kept together: they're always queried together and
-- splitting severity out buys a ~6-row table and an extra join.
CREATE TABLE IF NOT EXISTS crashes.dim_crash_type (
  crash_type_key         INT64  NOT NULL,
  crash_type_nk          STRING NOT NULL,
  manner_of_crash_raw    STRING,
  manner_of_crash        STRING,
  is_collision           BOOL,              -- FALSE for 'not collision between two MV in transport'
  crash_severity_raw     STRING,
  crash_severity_id_raw  STRING,
  crash_severity         STRING,            -- canonical across both eras
  crash_severity_rank    INT64,             -- 1 = PDO ... 5 = fatal, 0 = unknown
  is_injury_crash        BOOL,
  is_fatal_crash         BOOL,
  coding_era             STRING,            -- 'Pre-2019' | '2019+' | 'Unknown'
  PRIMARY KEY (crash_type_key) NOT ENFORCED
);

-- Junk dimension. Age is banded here; the raw integer stays on the fact so it
-- can still be averaged.
CREATE TABLE IF NOT EXISTS crashes.dim_person_profile (
  person_profile_key    INT64  NOT NULL,
  person_profile_nk     STRING NOT NULL,
  type_of_person_raw    STRING,
  type_of_person        STRING,             -- Driver / Occupant / Pedestrian
  is_motorist           BOOL,
  unit_type_raw         STRING,
  unit_type             STRING,             -- prefix stripped
  unit_category         STRING,             -- coarse rollup, era-independent
  gender_raw            STRING,
  gender                STRING,             -- Male / Female / Unknown
  age_band              STRING,
  age_band_sort         INT64,              -- so bands sort by age, not alphabetically
  injury_severity_raw   STRING,
  injury_severity       STRING,             -- canonical KABCO across both eras
  injury_severity_code  STRING,             -- K / A / B / C / O / U
  injury_severity_rank  INT64,              -- 1 = none ... 5 = fatal, 0 = unknown
  PRIMARY KEY (person_profile_key) NOT ENFORCED
);

CREATE TABLE IF NOT EXISTS crashes.fact_crash_person (
  crash_date_key       INT64  NOT NULL,     -- role-playing dim_date
  reported_date_key    INT64  NOT NULL,     -- role-playing dim_date
  crash_time_key       INT64  NOT NULL,
  location_key         INT64  NOT NULL,
  conditions_key       INT64  NOT NULL,
  crash_type_key       INT64  NOT NULL,
  person_profile_key   INT64  NOT NULL,
  instanceid           STRING NOT NULL,     -- degenerate dimension
  localreportno        STRING,              -- degenerate dimension
  socrata_id           STRING NOT NULL,     -- Socrata row id; regenerated on every republish, NOT a stable key
  crash_date           DATE,                -- carried into the fact for partitioning
  crash_datetime       DATETIME,
  reporting_lag_hours  FLOAT64,             -- reported minus occurred; negative = bad data
  latitude             FLOAT64,             -- per unit; NULL if missing or outside Hamilton County
  longitude            FLOAT64,
  -- Measures are INT64, not BOOL, so they're additive: SUM(is_fatal) is a
  -- fatality count, AVG(is_injured) an injury rate.
  person_count         INT64  NOT NULL,     -- always 1
  is_injured           INT64  NOT NULL,     -- injury_severity_rank >= 2 (possible injury or worse)
  is_fatal             INT64  NOT NULL,     -- injury_severity_rank  = 5
  age                  INT64,               -- raw, validated 0-110; NULL for 'BB', 'NN', 913, ...
  socrata_version      STRING,              -- ETL audit
  socrata_updated_at   TIMESTAMP,           -- ETL audit
  loaded_at            TIMESTAMP NOT NULL,  -- ETL audit
  crash_hash           STRING,              -- ETL change detection: MD5 of the crash's raw rows (Section 5)
  FOREIGN KEY (crash_date_key)    REFERENCES crashes.dim_date(date_key)                     NOT ENFORCED,
  FOREIGN KEY (reported_date_key)  REFERENCES crashes.dim_date(date_key)                     NOT ENFORCED,
  FOREIGN KEY (crash_time_key)     REFERENCES crashes.dim_time(time_key)                     NOT ENFORCED,
  FOREIGN KEY (location_key)       REFERENCES crashes.dim_location(location_key)             NOT ENFORCED,
  FOREIGN KEY (conditions_key)     REFERENCES crashes.dim_conditions(conditions_key)         NOT ENFORCED,
  FOREIGN KEY (crash_type_key)     REFERENCES crashes.dim_crash_type(crash_type_key)         NOT ENFORCED,
  FOREIGN KEY (person_profile_key) REFERENCES crashes.dim_person_profile(person_profile_key) NOT ENFORCED
)
-- Monthly, not daily: ~82 rows/day would make ~5,000 tiny daily partitions
-- whose metadata costs more than the pruning saves.
PARTITION BY DATE_TRUNC(crash_date, MONTH)
CLUSTER BY crash_type_key, person_profile_key, location_key;

-- ETL audit: one row per pipeline load (Section 9 writes it). window_start is
-- the crash-date watermark a delta load pulled from; NULL on a full load.
-- socrata_updated_at records which Socrata publish the load read — the one
-- use :updated_at has on this feed (see Section 3).
CREATE TABLE IF NOT EXISTS crashes.etl_load_log (
  load_id              STRING    NOT NULL,
  load_mode            STRING    NOT NULL,  -- 'delta' | 'full'
  window_start         DATE,                -- delta watermark; NULL = full history
  started_at           TIMESTAMP NOT NULL,
  finished_at          TIMESTAMP NOT NULL,
  rows_staged          INT64,
  fact_inserted        INT64,               -- fact rows inserted
  fact_updated         INT64,               -- NULL since the crash-level replace: rows are never updated
  fact_deleted         INT64,               -- fact rows deleted (replaced + removed upstream)
  socrata_updated_at   STRING,              -- publish stamp of the staged feed
  crashes_new          INT64,               -- crashes loaded for the first time
  crashes_changed      INT64,               -- existing crashes rewritten because their content changed
  crashes_removed      INT64                -- crashes deleted because the feed dropped them
);

-- Migrations for tables created before the crash-level replace. CREATE TABLE
-- IF NOT EXISTS never alters an existing table, so --setup applies these; on
-- a fresh build they're no-ops.
ALTER TABLE crashes.fact_crash_person
  ADD COLUMN IF NOT EXISTS crash_hash STRING;

ALTER TABLE crashes.etl_load_log
  ADD COLUMN IF NOT EXISTS crashes_new     INT64,
  ADD COLUMN IF NOT EXISTS crashes_changed INT64,
  ADD COLUMN IF NOT EXISTS crashes_removed INT64;

-- Cleaning view: the single place normalization happens. Sections 4 and 5
-- both read it, so a dimension's natural key is computed identically when the
-- dimension is built and when the fact looks it up. Duplicating these CASE
-- ladders is how facts silently stop matching and fall through to Unknown.
CREATE OR REPLACE VIEW crashes.vw_stg_crash_person_clean AS
WITH typed AS (
  SELECT
    socrata_id,
    socrata_version,
    SAFE_CAST(socrata_updated_at AS TIMESTAMP)  AS socrata_updated_at,
    instanceid,
    localreportno,
    SAFE_CAST(crashdate         AS DATETIME)    AS crash_datetime,
    SAFE_CAST(datecrashreported AS DATETIME)    AS reported_datetime,

    -- Raw coded columns: stable natural keys and the audit trail
    lightconditionsprimary, roadconditionsprimary, roadcontour, roadsurface, weather,
    mannerofcrash, crashseverity, crashseverityid, crashlocation,
    typeofperson, unittype, gender, injuries,

    -- Labels with the leading code stripped ('3 - DUSK' -> 'DUSK'). Regex on
    -- the prefix rather than SPLIT(x, ' - '), because some labels contain
    -- their own ' - ' ('3 - DARK - LIGHTED ROADWAY').
    UPPER(TRIM(REGEXP_REPLACE(lightconditionsprimary, r'^\s*\d+\s*-\s*', ''))) AS light_lbl,
    UPPER(TRIM(REGEXP_REPLACE(roadconditionsprimary,  r'^\s*\d+\s*-\s*', ''))) AS roadcond_lbl,
    UPPER(TRIM(REGEXP_REPLACE(roadcontour,            r'^\s*\d+\s*-\s*', ''))) AS contour_lbl,
    UPPER(TRIM(REGEXP_REPLACE(roadsurface,            r'^\s*\d+\s*-\s*', ''))) AS surface_lbl,
    UPPER(TRIM(REGEXP_REPLACE(weather,                r'^\s*\d+\s*-\s*', ''))) AS weather_lbl,
    UPPER(TRIM(REGEXP_REPLACE(mannerofcrash,          r'^\s*\d+\s*-\s*', ''))) AS manner_lbl,
    UPPER(TRIM(REGEXP_REPLACE(crashseverity,          r'^\s*\d+\s*-\s*', ''))) AS severity_lbl,
    UPPER(TRIM(REGEXP_REPLACE(crashlocation,          r'^\s*\d+\s*-\s*', ''))) AS crashloc_lbl,
    UPPER(TRIM(REGEXP_REPLACE(unittype,               r'^\s*\d+\s*-\s*', ''))) AS unit_lbl,
    UPPER(TRIM(REGEXP_REPLACE(injuries,               r'^\s*\d+\s*-\s*', ''))) AS injury_lbl,
    UPPER(TRIM(REGEXP_REPLACE(typeofperson,           r'^\s*[A-Z]\s*-\s*', ''))) AS person_lbl,
    UPPER(TRIM(REGEXP_REPLACE(gender,                 r'^\s*[A-Z]\s*-\s*', ''))) AS gender_lbl,

    NULLIF(TRIM(address), '')                                        AS address,
    -- Outside Hamilton County = geocoding failure, not a real location
    CASE WHEN SAFE_CAST(latitude  AS FLOAT64) BETWEEN 38.9 AND 39.4
         THEN SAFE_CAST(latitude  AS FLOAT64) END                    AS latitude,
    CASE WHEN SAFE_CAST(longitude AS FLOAT64) BETWEEN -84.9 AND -84.2
         THEN SAFE_CAST(longitude AS FLOAT64) END                    AS longitude,
    -- Feed contains '454229', '4202', '452', '31'
    CASE WHEN REGEXP_CONTAINS(zip, r'^\d{5}$') THEN zip END          AS zip,
    NULLIF(TRIM(community_council_neighborhood), '')                 AS community_council_neighborhood,
    NULLIF(TRIM(REGEXP_REPLACE(cpd_neighborhood, r'\s+', ' ')), '')  AS cpd_neighborhood,
    NULLIF(TRIM(sna_neighborhood), '')                               AS sna_neighborhood,
    NULLIF(TRIM(roadclass), '')                                      AS road_class_code,
    NULLIF(TRIM(roadclassdesc), '')                                  AS road_class_desc,
    -- Feed contains 'BB', 'NN', 'NB', 913, 933, 122
    CASE WHEN SAFE_CAST(age AS INT64) BETWEEN 0 AND 110
         THEN SAFE_CAST(age AS INT64) END                            AS age
  FROM crashes.stg_crash_person
),
canonical AS (
  SELECT
    *,
    -- 'NOT LIGHTED' must be tested before 'LIGHTED'. REPLACE fixes the source
    -- misspelling 'LIGHTIED'; the mangled dash in 'DARK <?> ROADWAY' doesn't
    -- matter because matching is by substring.
    CASE
      WHEN light_lbl IS NULL OR light_lbl = ''                         THEN 'Unknown'
      WHEN light_lbl LIKE 'DAYLIGHT%'                                  THEN 'Daylight'
      WHEN light_lbl LIKE 'DAWN%'                                      THEN 'Dawn'
      WHEN light_lbl LIKE 'DUSK%'                                      THEN 'Dusk'
      WHEN REPLACE(light_lbl, 'LIGHTIED', 'LIGHTED') LIKE 'DARK%NOT LIGHTED%' THEN 'Dark - Not Lighted'
      WHEN light_lbl LIKE 'DARK%UNKNOWN%'                              THEN 'Dark - Unknown Lighting'
      WHEN light_lbl LIKE 'DARK%LIGHTED%'                              THEN 'Dark - Lighted'
      WHEN light_lbl LIKE 'DARK%'                                      THEN 'Dark - Unknown Lighting'
      WHEN light_lbl LIKE 'UNKNOWN%'                                   THEN 'Unknown'
      ELSE 'Other'
    END AS light_conditions,

    CASE
      WHEN roadcond_lbl IS NULL OR roadcond_lbl = ''  THEN 'Unknown'
      WHEN roadcond_lbl LIKE 'DRY%'                   THEN 'Dry'
      WHEN roadcond_lbl LIKE 'WET%'                   THEN 'Wet'
      WHEN roadcond_lbl LIKE 'SNOW%'                  THEN 'Snow'
      WHEN roadcond_lbl LIKE 'ICE%'                   THEN 'Ice'
      WHEN roadcond_lbl LIKE 'SLUSH%'                 THEN 'Slush'
      WHEN roadcond_lbl LIKE 'WATER%'                 THEN 'Standing/Moving Water'
      WHEN roadcond_lbl LIKE 'SAND%'                  THEN 'Sand/Mud/Dirt/Oil/Gravel'
      WHEN roadcond_lbl LIKE 'UNKNOWN%'               THEN 'Unknown'
      ELSE 'Other'
    END AS road_conditions,

    CASE contour_lbl
      WHEN 'STRAIGHT LEVEL' THEN 'Straight Level'
      WHEN 'STRAIGHT GRADE' THEN 'Straight Grade'
      WHEN 'CURVE LEVEL'    THEN 'Curve Level'
      WHEN 'CURVE GRADE'    THEN 'Curve Grade'
      ELSE 'Unknown'
    END AS road_contour,

    CASE
      WHEN surface_lbl IS NULL OR surface_lbl = ''    THEN 'Unknown'
      WHEN surface_lbl LIKE 'BLACKTOP%'               THEN 'Blacktop/Asphalt'
      WHEN surface_lbl LIKE 'CONCRETE%'               THEN 'Concrete'
      WHEN surface_lbl LIKE 'BRICK%'                  THEN 'Brick/Block'
      WHEN surface_lbl LIKE 'SLAG%'                   THEN 'Slag/Gravel/Stone'
      WHEN surface_lbl LIKE 'DIRT%'                   THEN 'Dirt'
      WHEN surface_lbl LIKE 'UNKNOWN%'                THEN 'Unknown'
      ELSE 'Other'
    END AS road_surface,

    CASE
      WHEN weather_lbl IS NULL OR weather_lbl = ''    THEN 'Unknown'
      WHEN weather_lbl LIKE 'CLEAR%'                  THEN 'Clear'
      WHEN weather_lbl LIKE 'CLOUDY%'                 THEN 'Cloudy'
      WHEN weather_lbl LIKE 'RAIN%'                   THEN 'Rain'
      WHEN weather_lbl LIKE 'SNOW%'                   THEN 'Snow'
      WHEN weather_lbl LIKE 'FREEZING%'               THEN 'Freezing Rain/Drizzle'
      WHEN weather_lbl LIKE 'SLEET%'                  THEN 'Sleet/Hail'   -- 'SLEET, HAIL' and 'SLEET,HAIL'
      WHEN weather_lbl LIKE 'FOG%'                    THEN 'Fog/Smog/Smoke'
      WHEN weather_lbl LIKE 'SEVERE CROSSWINDS%'      THEN 'Severe Crosswinds'
      WHEN weather_lbl LIKE 'BLOWING%'                THEN 'Blowing Sand/Soil/Dirt/Snow'
      ELSE 'Other/Unknown'
    END AS weather_norm,

    CASE
      WHEN manner_lbl IS NULL OR manner_lbl = ''      THEN 'Unknown'
      WHEN manner_lbl LIKE 'ANGLE%'                   THEN 'Angle'
      WHEN manner_lbl LIKE 'REAR-END%'                THEN 'Rear-End'
      WHEN manner_lbl LIKE 'REAR-TO-REAR%'            THEN 'Rear-to-Rear'
      WHEN manner_lbl LIKE 'HEAD-ON%'                 THEN 'Head-On'
      WHEN manner_lbl LIKE 'BACKING%'                 THEN 'Backing'
      WHEN manner_lbl LIKE 'SIDESWIPE, SAME%'         THEN 'Sideswipe - Same Direction'
      WHEN manner_lbl LIKE 'SIDESWIPE, OPPOSITE%'     THEN 'Sideswipe - Opposite Direction'
      WHEN manner_lbl LIKE 'NOT COLLISION%'           THEN 'Not Collision Between Two Motor Vehicles'
      ELSE 'Unknown'
    END AS manner_of_crash,

    CASE
      WHEN crashloc_lbl IS NULL OR crashloc_lbl = ''  THEN NULL
      ELSE INITCAP(crashloc_lbl)
    END AS crash_location,

    -- Pre-2019 'INJURY' is one bucket the 2019+ coding splits three ways, so
    -- it's labelled as unspecified rather than passed off as 'minor'.
    CASE
      WHEN severity_lbl IS NULL OR severity_lbl = ''   THEN 'Unknown'
      WHEN severity_lbl LIKE 'FATAL%'                  THEN 'Fatal'
      WHEN severity_lbl LIKE 'SERIOUS INJURY%'         THEN 'Serious Injury Suspected'
      WHEN severity_lbl LIKE 'MINOR INJURY%'           THEN 'Minor Injury Suspected'
      WHEN severity_lbl LIKE 'INJURY POSSIBLE%'        THEN 'Possible Injury'
      WHEN severity_lbl = 'INJURY'                     THEN 'Injury (Severity Unspecified)'
      WHEN severity_lbl LIKE 'PROPERTY DAMAGE ONLY%'   THEN 'Property Damage Only'
      ELSE 'Unknown'
    END AS crash_severity,

    -- KABCO ladder; both eras collapse onto it
    CASE
      WHEN injury_lbl IS NULL OR injury_lbl = ''       THEN 'Unknown'
      WHEN injury_lbl LIKE 'FATAL%'                    THEN 'K - Fatal'
      WHEN injury_lbl LIKE 'INCAPACITATING%'
        OR injury_lbl LIKE 'SUSPECTED SERIOUS%'        THEN 'A - Suspected Serious'
      WHEN injury_lbl LIKE 'NON-INCAPACITATING%'
        OR injury_lbl LIKE 'SUSPECTED MINOR%'          THEN 'B - Suspected Minor'
      WHEN injury_lbl LIKE 'POSSIBLE%'                 THEN 'C - Possible'
      WHEN injury_lbl LIKE 'NO %'                      THEN 'O - No Apparent Injury'  -- 'NO APPARENTY INJURY' (sic), 'NO INJURY / NONE REPORTED'
      ELSE 'Unknown'
    END AS injury_severity,

    CASE
      WHEN person_lbl LIKE 'DRIVER%'                   THEN 'Driver'
      WHEN person_lbl LIKE 'OCCUPANT%'                 THEN 'Occupant'
      WHEN person_lbl LIKE 'PEDESTRIAN%'               THEN 'Pedestrian'
      ELSE 'Unknown'
    END AS type_of_person,

    -- Rolled up from the LABEL, never the code ('03' is MID SIZE in one era
    -- and SPORT UTILITY VEHICLE in the other). Order matters: MOPED is tested
    -- before BICYCLE ('MOPED OR MOTORIZED BICYCLE'), and the van rules are
    -- prefix-anchored so 'BUS /VAN' still lands in Bus.
    CASE
      WHEN unit_lbl IS NULL OR unit_lbl = ''           THEN 'Unknown'
      WHEN unit_lbl LIKE '%SPORT UTILITY%'             THEN 'SUV'
      WHEN unit_lbl LIKE 'PICK%'                       THEN 'Pickup'
      WHEN unit_lbl LIKE '%MINIVAN%'
        OR unit_lbl LIKE 'VAN%'
        OR unit_lbl LIKE 'CARGO VAN%'                  THEN 'Van'
      WHEN unit_lbl LIKE 'BUS%'                        THEN 'Bus'
      WHEN unit_lbl LIKE '%SEMI%'
        OR unit_lbl LIKE 'TRACTOR%'                    -- TRACTOR/DOUBLES, TRACTOR/TRIPLES
        OR unit_lbl LIKE '%TRUCK%'
        OR unit_lbl LIKE '%HEAVY%'                     THEN 'Truck/Heavy'
      WHEN unit_lbl LIKE 'MOTORCYCLE%'
        OR unit_lbl LIKE 'MOPED%'
        OR unit_lbl LIKE 'MOTORIZED BICYCLE%'
        OR unit_lbl LIKE 'AUTOCYCLE%'                  THEN 'Motorcycle/Moped'
      WHEN unit_lbl LIKE 'BICYCLE%'                    THEN 'Bicycle'
      WHEN unit_lbl LIKE 'PEDESTRIAN%'                 THEN 'Pedestrian/Skater'
      WHEN unit_lbl LIKE '%NON-MOTORIST%'
        OR unit_lbl LIKE 'WHEELCHAIR%'                 THEN 'Other Non-Motorist'
      WHEN unit_lbl LIKE 'PASSENGER CAR%'
        OR unit_lbl LIKE '%COMPACT%'
        OR unit_lbl LIKE 'MID SIZE%'
        OR unit_lbl LIKE 'FULL SIZE%'
        OR unit_lbl LIKE '%PASSENGER VEHICLE%'         THEN 'Passenger Car'
      WHEN unit_lbl LIKE 'UNKNOWN%'                    THEN 'Unknown'
      ELSE 'Other'
    END AS unit_category,

    -- 'M - MALE' and bare 'MALE' both occur
    CASE
      WHEN gender_lbl LIKE 'M%'                        THEN 'Male'
      WHEN gender_lbl LIKE 'F%'                        THEN 'Female'
      ELSE 'Unknown'
    END AS gender_norm,

    CASE
      WHEN age IS NULL THEN 'Unknown'
      WHEN age < 16    THEN '00-15'
      WHEN age < 21    THEN '16-20'
      WHEN age < 25    THEN '21-24'
      WHEN age < 35    THEN '25-34'
      WHEN age < 45    THEN '35-44'
      WHEN age < 55    THEN '45-54'
      WHEN age < 65    THEN '55-64'
      WHEN age < 75    THEN '65-74'
      ELSE '75+'
    END AS age_band,

    CASE
      WHEN crashseverityid IS NULL      THEN 'Unknown'
      WHEN crashseverityid LIKE '2019%' THEN '2019+'
      ELSE 'Pre-2019'
    END AS coding_era
  FROM typed
)
SELECT
  c.*,

  CASE injury_severity
    WHEN 'K - Fatal'              THEN 5
    WHEN 'A - Suspected Serious'  THEN 4
    WHEN 'B - Suspected Minor'    THEN 3
    WHEN 'C - Possible'           THEN 2
    WHEN 'O - No Apparent Injury' THEN 1
    ELSE 0
  END AS injury_severity_rank,

  IF(injury_severity = 'Unknown', 'U', SUBSTR(injury_severity, 1, 1)) AS injury_severity_code,

  CASE crash_severity
    WHEN 'Fatal'                         THEN 5
    WHEN 'Serious Injury Suspected'      THEN 4
    WHEN 'Minor Injury Suspected'        THEN 3
    WHEN 'Injury (Severity Unspecified)' THEN 3
    WHEN 'Possible Injury'               THEN 2
    WHEN 'Property Damage Only'          THEN 1
    ELSE 0
  END AS crash_severity_rank,

  CASE age_band
    WHEN '00-15' THEN 1 WHEN '16-20' THEN 2 WHEN '21-24' THEN 3
    WHEN '25-34' THEN 4 WHEN '35-44' THEN 5 WHEN '45-54' THEN 6
    WHEN '55-64' THEN 7 WHEN '65-74' THEN 8 WHEN '75+'   THEN 9
    ELSE 99
  END AS age_band_sort,

  -- Natural keys, built from RAW values. If a canonicalization rule is later
  -- corrected, a raw-keyed member keeps its surrogate key and its facts stay
  -- attached. IFNULL(x, '~') because NULL propagates through FORMAT/CONCAT.
  FORMAT('%s|%s|%s|%s|%s',
    IFNULL(lightconditionsprimary, '~'), IFNULL(roadconditionsprimary, '~'),
    IFNULL(roadcontour, '~'), IFNULL(roadsurface, '~'), IFNULL(weather, '~')
  ) AS conditions_nk,

  FORMAT('%s|%s|%s',
    IFNULL(mannerofcrash, '~'), IFNULL(crashseverityid, '~'), IFNULL(crashseverity, '~')
  ) AS crash_type_nk,

  FORMAT('%s|%s|%s|%s|%s',
    IFNULL(typeofperson, '~'), IFNULL(unittype, '~'), IFNULL(gender, '~'),
    age_band, IFNULL(injuries, '~')
  ) AS person_profile_nk,

  -- Hashed: the readable tuple runs ~150 chars on the widest dimension.
  TO_HEX(MD5(FORMAT('%s|%s|%s|%s|%s|%s|%s|%s',
    IFNULL(address, '~'),
    IFNULL(zip, '~'),
    IFNULL(community_council_neighborhood, '~'), IFNULL(cpd_neighborhood, '~'),
    IFNULL(sna_neighborhood, '~'),
    IFNULL(road_class_code, '~'), IFNULL(road_class_desc, '~'),
    IFNULL(crashlocation, '~')
  ))) AS location_nk
FROM canonical c;


-- ============================================================
-- SECTION 2: PRE-POPULATED DIMENSIONS (setup; guarded so
-- reruns are no-ops — a bare INSERT run twice would duplicate
-- every row and fan out every fact join).
-- dim_date and dim_time are generated, never merged.
-- Unknown members get -1 / 19000101.
-- ============================================================

-- Earliest real reported date is 2012-01-02. Two source rows carry a
-- crashdate of 1900-02-06 (entry damage); they fall through to 19000101.
INSERT INTO crashes.dim_date
SELECT
  CAST(FORMAT_DATE('%Y%m%d', d) AS INT64)   AS date_key,
  d                                         AS full_date,
  EXTRACT(DAYOFWEEK FROM d)                 AS day_of_week,
  FORMAT_DATE('%A', d)                      AS day_of_week_name,
  UPPER(FORMAT_DATE('%a', d))               AS day_of_week_abbr,
  EXTRACT(DAYOFWEEK FROM d) IN (1, 7)       AS is_weekend,
  EXTRACT(DAY FROM d)                       AS day_of_month,
  EXTRACT(ISOWEEK FROM d)                   AS week_of_year,
  EXTRACT(MONTH FROM d)                     AS month_number,
  FORMAT_DATE('%B', d)                      AS month_name,
  FORMAT_DATE('%Y-%m', d)                   AS year_month,
  EXTRACT(QUARTER FROM d)                   AS quarter,
  EXTRACT(YEAR FROM d)                      AS year
FROM UNNEST(GENERATE_DATE_ARRAY('2010-01-01', '2035-12-31')) AS d
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_date);

-- Rush-hour bands follow the observed hour histogram: the real peaks are
-- 07:00-08:59 and 15:00-17:59 (not the textbook 16-18).
INSERT INTO crashes.dim_time
SELECT
  h * 100 + m                                           AS time_key,
  TIME(h, m, 0)                                         AS time_of_day,
  h                                                     AS hour_24,
  IF(MOD(h, 12) = 0, 12, MOD(h, 12))                    AS hour_12,
  IF(h < 12, 'AM', 'PM')                                AS am_pm,
  m                                                     AS minute_of_hour,
  FORMAT('%02d:00-%02d:59', h, h)                       AS hour_label,
  CASE
    WHEN h < 5  THEN 'Overnight'
    WHEN h < 7  THEN 'Early Morning'
    WHEN h < 9  THEN 'Morning Rush'
    WHEN h < 12 THEN 'Midday'
    WHEN h < 15 THEN 'Afternoon'
    WHEN h < 18 THEN 'Evening Rush'
    WHEN h < 22 THEN 'Evening'
    ELSE 'Night'
  END                                                   AS time_of_day_band,
  h IN (7, 8, 15, 16, 17)                               AS is_rush_hour,
  h < 5 OR h >= 22                                      AS is_overnight
FROM UNNEST(GENERATE_ARRAY(0, 23)) AS h,
     UNNEST(GENERATE_ARRAY(0, 59)) AS m
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_time);

-- Unknown members (one per dimension)
INSERT INTO crashes.dim_date
SELECT 19000101, DATE '1900-01-01', 2, 'Unknown', 'UNK', FALSE, 1, 1, 1, 'Unknown', 'Unknown', 1, 1900
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_date WHERE date_key = 19000101);

INSERT INTO crashes.dim_time
SELECT -1, TIME '00:00:00', -1, -1, 'NA', -1, 'Unknown', 'Unknown', FALSE, FALSE
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_time WHERE time_key = -1);

INSERT INTO crashes.dim_location
SELECT -1, '~UNKNOWN~', NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_location WHERE location_key = -1);

INSERT INTO crashes.dim_conditions
SELECT -1, '~UNKNOWN~', NULL, 'Unknown', NULL, NULL, 'Unknown', NULL,
       NULL, 'Unknown', NULL, NULL, NULL, 'Unknown', NULL, 'Unknown', NULL
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_conditions WHERE conditions_key = -1);

INSERT INTO crashes.dim_crash_type
SELECT -1, '~UNKNOWN~', NULL, 'Unknown', NULL, NULL, NULL, 'Unknown', 0, NULL, NULL, 'Unknown'
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_crash_type WHERE crash_type_key = -1);

INSERT INTO crashes.dim_person_profile
SELECT -1, '~UNKNOWN~', NULL, 'Unknown', NULL, NULL, NULL, 'Unknown',
       NULL, 'Unknown', 'Unknown', 99, NULL, 'Unknown', 'U', 0
FROM UNNEST([1]) AS one
WHERE NOT EXISTS (SELECT 1 FROM crashes.dim_person_profile WHERE person_profile_key = -1);


-- ============================================================
-- SECTION 3: STAGE (every load)
-- Python pulls the feed and writes raw rows here with WRITE_TRUNCATE.
-- If loading via SQL for practice, truncate first so reruns don't duplicate.
--
-- WATERMARK: :updated_at does not work as a per-row watermark on this feed.
-- Measured on the live API: COUNT(DISTINCT :updated_at) = 1 and
-- COUNT(DISTINCT :created_at) = 1 across all 433,160 rows, while
-- COUNT(DISTINCT :version) = 433,160. Socrata restamps every row on each
-- publish, so `:updated_at > last_run` returns everything or nothing.
-- It's still stored on the fact (it identifies the publish a row came from).
-- :version is unique per row but random ('rv-ei6x_bj7f_buq7'), so it can't
-- be ordered into a watermark either. Neither :id nor :version survives a
-- republish: both are regenerated for every row.
--
-- DELTA LOADS therefore window on crash date. Run_Pipeline.py --delta reads
--   DATE_SUB(MAX(crash_date), INTERVAL 90 DAY)   (DELTA_LOOKBACK_DAYS in .env)
-- from the fact and pulls only crashes on or after it. 90 days covers
-- late-filed reports: measured lag p99 = 3.7 days, 0.08% of rows over 90.
-- Section 5 then skips crashes whose content hash hasn't changed, so a delta
-- rewrites only new and amended crashes. Amendments to crashes older than
-- the window are picked up by the next --full load.
-- ============================================================

TRUNCATE TABLE crashes.stg_crash_person;
-- ... Python client library load lands here ...


-- ============================================================
-- SECTION 4: DIMENSION MERGES (every load)
-- The state-anchored pattern. Three parts to notice in each:
--   1. The USING subquery finds ONLY genuinely new natural keys
--      (NOT EXISTS against the dimension).
--   2. ROW_NUMBER() numbers just those new members 1..N.
--   3. Adding the current max key offsets them past existing keys.
--      GREATEST(..., 0) keeps the first real key at 1 rather than 0,
--      since MAX() over a dimension holding only the -1 member is -1.
-- Reruns are safe: NOT EXISTS finds nothing new and the merge is a no-op.
-- Insert-only is correct: every attribute is derived from the natural key,
-- so a changed attribute is a new member, not an update.
-- ============================================================

MERGE crashes.dim_location AS t
USING (
  SELECT
    (SELECT GREATEST(IFNULL(MAX(location_key), 0), 0) FROM crashes.dim_location)
      + ROW_NUMBER() OVER (ORDER BY location_nk)  AS location_key,
    s.*
  FROM (
    SELECT DISTINCT
      v.location_nk,
      v.address,
      v.zip,
      v.community_council_neighborhood, v.cpd_neighborhood, v.sna_neighborhood,
      v.road_class_code, v.road_class_desc,
      v.crashlocation AS crash_location_raw,
      v.crash_location,
      CASE
        WHEN v.crashloc_lbl IS NULL OR v.crashloc_lbl LIKE 'UNKNOWN%' THEN NULL
        WHEN v.crashloc_lbl LIKE 'NOT AN INTERSECTION%'               THEN FALSE
        ELSE REGEXP_CONTAINS(v.crashloc_lbl, r'INTERSECTION|FIVE-POINT|TRAFFIC CIRCLE')
      END AS is_intersection
    FROM crashes.vw_stg_crash_person_clean v
    WHERE NOT EXISTS (
      SELECT 1 FROM crashes.dim_location d WHERE d.location_nk = v.location_nk
    )
  ) AS s
) AS src
ON t.location_nk = src.location_nk
WHEN NOT MATCHED THEN
  INSERT (location_key, location_nk, address, zip,
          community_council_neighborhood, cpd_neighborhood, sna_neighborhood,
          road_class_code, road_class_desc, crash_location_raw, crash_location, is_intersection)
  VALUES (src.location_key, src.location_nk, src.address, src.zip,
          src.community_council_neighborhood, src.cpd_neighborhood, src.sna_neighborhood,
          src.road_class_code, src.road_class_desc,
          src.crash_location_raw, src.crash_location, src.is_intersection);

MERGE crashes.dim_conditions AS t
USING (
  SELECT
    (SELECT GREATEST(IFNULL(MAX(conditions_key), 0), 0) FROM crashes.dim_conditions)
      + ROW_NUMBER() OVER (ORDER BY conditions_nk)  AS conditions_key,
    s.*
  FROM (
    SELECT DISTINCT
      v.conditions_nk,
      v.lightconditionsprimary  AS light_conditions_raw,
      v.light_conditions,
      STARTS_WITH(v.light_conditions, 'Dark')  AS is_dark,
      v.roadconditionsprimary   AS road_conditions_raw,
      v.road_conditions,
      v.road_conditions IN ('Wet', 'Snow', 'Ice', 'Slush', 'Standing/Moving Water') AS is_slick,
      v.roadcontour             AS road_contour_raw,
      v.road_contour,
      STARTS_WITH(v.road_contour, 'Curve')     AS is_curve,
      ENDS_WITH(v.road_contour, 'Grade')       AS is_grade,
      v.roadsurface             AS road_surface_raw,
      v.road_surface,
      v.weather                 AS weather_raw,
      v.weather_norm            AS weather,
      v.weather_norm NOT IN ('Clear', 'Cloudy', 'Other/Unknown', 'Unknown') AS is_adverse_weather
    FROM crashes.vw_stg_crash_person_clean v
    WHERE NOT EXISTS (
      SELECT 1 FROM crashes.dim_conditions d WHERE d.conditions_nk = v.conditions_nk
    )
  ) AS s
) AS src
ON t.conditions_nk = src.conditions_nk
WHEN NOT MATCHED THEN
  INSERT (conditions_key, conditions_nk,
          light_conditions_raw, light_conditions, is_dark,
          road_conditions_raw, road_conditions, is_slick,
          road_contour_raw, road_contour, is_curve, is_grade,
          road_surface_raw, road_surface,
          weather_raw, weather, is_adverse_weather)
  VALUES (src.conditions_key, src.conditions_nk,
          src.light_conditions_raw, src.light_conditions, src.is_dark,
          src.road_conditions_raw, src.road_conditions, src.is_slick,
          src.road_contour_raw, src.road_contour, src.is_curve, src.is_grade,
          src.road_surface_raw, src.road_surface,
          src.weather_raw, src.weather, src.is_adverse_weather);

MERGE crashes.dim_crash_type AS t
USING (
  SELECT
    (SELECT GREATEST(IFNULL(MAX(crash_type_key), 0), 0) FROM crashes.dim_crash_type)
      + ROW_NUMBER() OVER (ORDER BY crash_type_nk)  AS crash_type_key,
    s.*
  FROM (
    SELECT DISTINCT
      v.crash_type_nk,
      v.mannerofcrash    AS manner_of_crash_raw,
      v.manner_of_crash,
      IF(v.manner_of_crash = 'Unknown', NULL,
         v.manner_of_crash != 'Not Collision Between Two Motor Vehicles')  AS is_collision,
      v.crashseverity    AS crash_severity_raw,
      v.crashseverityid  AS crash_severity_id_raw,
      v.crash_severity,
      v.crash_severity_rank,
      IF(v.crash_severity_rank = 0, NULL, v.crash_severity_rank >= 2)      AS is_injury_crash,
      IF(v.crash_severity_rank = 0, NULL, v.crash_severity_rank  = 5)      AS is_fatal_crash,
      v.coding_era
    FROM crashes.vw_stg_crash_person_clean v
    WHERE NOT EXISTS (
      SELECT 1 FROM crashes.dim_crash_type d WHERE d.crash_type_nk = v.crash_type_nk
    )
  ) AS s
) AS src
ON t.crash_type_nk = src.crash_type_nk
WHEN NOT MATCHED THEN
  INSERT (crash_type_key, crash_type_nk, manner_of_crash_raw, manner_of_crash, is_collision,
          crash_severity_raw, crash_severity_id_raw, crash_severity, crash_severity_rank,
          is_injury_crash, is_fatal_crash, coding_era)
  VALUES (src.crash_type_key, src.crash_type_nk, src.manner_of_crash_raw, src.manner_of_crash,
          src.is_collision, src.crash_severity_raw, src.crash_severity_id_raw,
          src.crash_severity, src.crash_severity_rank,
          src.is_injury_crash, src.is_fatal_crash, src.coding_era);

MERGE crashes.dim_person_profile AS t
USING (
  SELECT
    (SELECT GREATEST(IFNULL(MAX(person_profile_key), 0), 0) FROM crashes.dim_person_profile)
      + ROW_NUMBER() OVER (ORDER BY person_profile_nk)  AS person_profile_key,
    s.*
  FROM (
    SELECT DISTINCT
      v.person_profile_nk,
      v.typeofperson  AS type_of_person_raw,
      v.type_of_person,
      IF(v.type_of_person = 'Unknown', NULL, v.type_of_person IN ('Driver', 'Occupant')) AS is_motorist,
      v.unittype      AS unit_type_raw,
      v.unit_lbl      AS unit_type,
      v.unit_category,
      v.gender        AS gender_raw,
      v.gender_norm   AS gender,
      v.age_band,
      v.age_band_sort,
      v.injuries      AS injury_severity_raw,
      v.injury_severity,
      v.injury_severity_code,
      v.injury_severity_rank
    FROM crashes.vw_stg_crash_person_clean v
    WHERE NOT EXISTS (
      SELECT 1 FROM crashes.dim_person_profile d WHERE d.person_profile_nk = v.person_profile_nk
    )
  ) AS s
) AS src
ON t.person_profile_nk = src.person_profile_nk
WHEN NOT MATCHED THEN
  INSERT (person_profile_key, person_profile_nk, type_of_person_raw, type_of_person, is_motorist,
          unit_type_raw, unit_type, unit_category, gender_raw, gender, age_band, age_band_sort,
          injury_severity_raw, injury_severity, injury_severity_code, injury_severity_rank)
  VALUES (src.person_profile_key, src.person_profile_nk, src.type_of_person_raw,
          src.type_of_person, src.is_motorist,
          src.unit_type_raw, src.unit_type, src.unit_category, src.gender_raw, src.gender,
          src.age_band, src.age_band_sort,
          src.injury_severity_raw, src.injury_severity, src.injury_severity_code,
          src.injury_severity_rank);


-- ============================================================
-- SECTION 5: FACT LOAD (every load; parameter @window_start) — crash-level
-- replace on instanceid, with delete reconciliation, in one transaction.
--
-- Why not MERGE on :id: Socrata regenerates every :id and :version when it
-- republishes the dataset. On 2026-09-19 all 6,791 rows of a delta window
-- came back under new ids with unchanged content, and a MERGE on :id
-- inserted every one of them as a duplicate. instanceid survives republishes,
-- but nothing identifies a PERSON within a crash across publishes, so the unit
-- of change is the whole crash: when anything about a crash changes, all of
-- its rows are deleted and reinserted.
--
-- Change detection: crash_hash is an MD5 over the crash's raw staged rows,
-- sorted so row order doesn't matter, leaving out the Socrata system columns
-- that change on every publish. A crash is rewritten only when it's new, its
-- hash differs, or its fact rows disagree with staging (row count, or rows
-- missing a hash, like those loaded before crash_hash existed). A rerun, or a
-- republish with no content changes, rewrites nothing. The hash is built from
-- RAW values, so after changing logic in the cleaning view, TRUNCATE the fact
-- and run --full, or unchanged crashes keep the old derivation.
--
-- Delete reconciliation (formerly Section 8): staging holds every current
-- crash in the loaded window, so a fact crash in that window missing from
-- staging was deleted upstream.
--   @window_start = NULL   full load: the window is the whole fact
--   @window_start = date   delta load: only crash_date >= @window_start,
--                          which is exactly what the delta pulled
-- Crashes with a NULL crash_date sit outside every delta window and are only
-- reconciled on a full load. Edge case: a crash whose date is amended to
-- before the window gets deleted here, and the next --full restores it.
--
-- Guards, all of which fail the load rather than write a partial result:
--   * Staging must come from one publish. A republish mid-fetch reshuffles
--     :id, and the pages (ordered by :id) then skip or repeat rows.
--   * Staging must hold at least 90% of the crashes the fact has in the
--     window, counted BEFORE anything changes, so a partial fetch or a bad
--     filter can't delete the rest.
--   * After the load, every staged crash must have exactly its staged rows in
--     the fact, all carrying its hash. Otherwise the transaction rolls back.
--
-- Natural keys are exchanged for surrogate keys here. LEFT JOIN + IFNULL
-- routes lookup failures to the Unknown members instead of silently dropping
-- rows (which an INNER JOIN would do). QUALIFY drops a row that overlapping
-- API pages staged twice. The final SELECT returns the counts Section 9 logs.
-- ============================================================

DECLARE staged_crashes, window_crashes INT64;
DECLARE rows_inserted, rows_deleted, crashes_removed INT64 DEFAULT 0;

ASSERT (SELECT COUNT(DISTINCT socrata_updated_at) FROM crashes.stg_crash_person) <= 1
  AS 'Staging spans more than one Socrata publish (the feed republished mid-fetch). Rerun the load.';

-- One row per staged person
CREATE TEMP TABLE stg_rows AS
SELECT *
FROM crashes.stg_crash_person
WHERE socrata_id IS NOT NULL
  AND instanceid IS NOT NULL
QUALIFY ROW_NUMBER() OVER (PARTITION BY socrata_id ORDER BY socrata_version DESC) = 1;

-- One row per staged crash: content hash and person-row count
CREATE TEMP TABLE stg_crash AS
SELECT
  instanceid,
  TO_HEX(MD5(STRING_AGG(row_json, '\n' ORDER BY row_json))) AS crash_hash,
  COUNT(*)                                                  AS person_rows
FROM (
  SELECT
    instanceid,
    TO_JSON_STRING(STRUCT(
      instanceid, localreportno, crashdate, datecrashreported,
      address, latitude, longitude, zip,
      community_council_neighborhood, cpd_neighborhood, sna_neighborhood,
      roadclass, roadclassdesc, crashlocation,
      lightconditionsprimary, roadconditionsprimary, roadcontour, roadsurface, weather,
      mannerofcrash, crashseverity, crashseverityid,
      typeofperson, unittype, gender, age, injuries
    )) AS row_json
  FROM stg_rows
)
GROUP BY instanceid;

-- Staged crashes to (re)write. A fact crash whose rows don't agree on a
-- single non-NULL hash compares as NULL, so it's always rewritten.
CREATE TEMP TABLE changed_crash AS
SELECT s.instanceid, s.crash_hash, f.instanceid IS NULL AS is_new
FROM stg_crash s
LEFT JOIN (
  SELECT
    instanceid,
    IF(COUNT(DISTINCT crash_hash) = 1 AND COUNTIF(crash_hash IS NULL) = 0,
       ANY_VALUE(crash_hash), NULL) AS crash_hash,
    COUNT(*)                        AS person_rows
  FROM crashes.fact_crash_person
  GROUP BY instanceid
) f ON f.instanceid = s.instanceid
WHERE f.instanceid IS NULL
   OR f.crash_hash IS DISTINCT FROM s.crash_hash
   OR f.person_rows != s.person_rows;

SET staged_crashes = (SELECT COUNT(*) FROM stg_crash);
SET window_crashes = (
  SELECT COUNT(DISTINCT instanceid) FROM crashes.fact_crash_person
  WHERE @window_start IS NULL OR crash_date >= @window_start
);
IF staged_crashes < 0.9 * window_crashes THEN
  RAISE USING MESSAGE = FORMAT(
    'Staging holds %d crashes but the fact has %d in the load window. Refusing to load: a partial fetch would delete the difference.',
    staged_crashes, window_crashes);
END IF;

BEGIN
  BEGIN TRANSACTION;

  -- Replace: drop every row of a changed crash (a new crash has none) ...
  DELETE FROM crashes.fact_crash_person
  WHERE instanceid IN (SELECT instanceid FROM changed_crash);
  SET rows_deleted = @@row_count;

  -- ... and insert its current rows
  INSERT INTO crashes.fact_crash_person
    (crash_date_key, reported_date_key, crash_time_key, location_key, conditions_key,
     crash_type_key, person_profile_key, instanceid, localreportno, socrata_id,
     crash_date, crash_datetime, reporting_lag_hours, latitude, longitude,
     person_count, is_injured, is_fatal, age,
     socrata_version, socrata_updated_at, loaded_at, crash_hash)
  SELECT
    IFNULL(dd_c.date_key,  19000101)        AS crash_date_key,
    IFNULL(dd_r.date_key,  19000101)        AS reported_date_key,
    IFNULL(dt.time_key,          -1)        AS crash_time_key,
    IFNULL(dl.location_key,      -1)        AS location_key,
    IFNULL(dc.conditions_key,    -1)        AS conditions_key,
    IFNULL(dct.crash_type_key,   -1)        AS crash_type_key,
    IFNULL(dpp.person_profile_key, -1)      AS person_profile_key,
    v.instanceid,
    v.localreportno,
    v.socrata_id,
    DATE(v.crash_datetime)                  AS crash_date,
    v.crash_datetime,
    -- Signed on purpose: negative lags are bad data that Section 6 counts
    DATETIME_DIFF(v.reported_datetime, v.crash_datetime, MINUTE) / 60.0 AS reporting_lag_hours,
    v.latitude,
    v.longitude,
    1                                       AS person_count,
    IF(v.injury_severity_rank >= 2, 1, 0)   AS is_injured,
    IF(v.injury_severity_rank  = 5, 1, 0)   AS is_fatal,
    v.age,
    v.socrata_version,
    v.socrata_updated_at,
    CURRENT_TIMESTAMP()                     AS loaded_at,
    c.crash_hash
  FROM crashes.vw_stg_crash_person_clean v
  JOIN changed_crash c ON c.instanceid = v.instanceid
  LEFT JOIN crashes.dim_date           dd_c ON dd_c.full_date = DATE(v.crash_datetime)
  LEFT JOIN crashes.dim_date           dd_r ON dd_r.full_date = DATE(v.reported_datetime)
  LEFT JOIN crashes.dim_time           dt   ON dt.time_key = EXTRACT(HOUR FROM v.crash_datetime) * 100
                                                           + EXTRACT(MINUTE FROM v.crash_datetime)
  LEFT JOIN crashes.dim_location       dl   ON dl.location_nk        = v.location_nk
  LEFT JOIN crashes.dim_conditions     dc   ON dc.conditions_nk      = v.conditions_nk
  LEFT JOIN crashes.dim_crash_type     dct  ON dct.crash_type_nk     = v.crash_type_nk
  LEFT JOIN crashes.dim_person_profile dpp  ON dpp.person_profile_nk = v.person_profile_nk
  WHERE v.socrata_id IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY v.socrata_id ORDER BY v.socrata_version DESC) = 1;
  SET rows_inserted = @@row_count;

  -- Reconcile upstream deletes: crashes in the window that staging lacks
  SET crashes_removed = (
    SELECT COUNT(DISTINCT instanceid) FROM crashes.fact_crash_person f
    WHERE (@window_start IS NULL OR f.crash_date >= @window_start)
      AND NOT EXISTS (SELECT 1 FROM stg_crash s WHERE s.instanceid = f.instanceid)
  );
  DELETE FROM crashes.fact_crash_person f
  WHERE (@window_start IS NULL OR f.crash_date >= @window_start)
    AND NOT EXISTS (SELECT 1 FROM stg_crash s WHERE s.instanceid = f.instanceid);
  SET rows_deleted = rows_deleted + @@row_count;

  -- Every staged crash now has exactly its staged rows, all with its hash
  ASSERT NOT EXISTS (
    SELECT 1
    FROM stg_crash s
    LEFT JOIN (
      SELECT
        instanceid,
        IF(COUNT(DISTINCT crash_hash) = 1 AND COUNTIF(crash_hash IS NULL) = 0,
           ANY_VALUE(crash_hash), NULL) AS crash_hash,
        COUNT(*)                        AS person_rows
      FROM crashes.fact_crash_person
      GROUP BY instanceid
    ) f ON f.instanceid = s.instanceid
    WHERE f.crash_hash IS DISTINCT FROM s.crash_hash
       OR f.person_rows IS DISTINCT FROM s.person_rows
  ) AS 'Post-load check failed: a staged crash does not match its fact rows.';

  COMMIT TRANSACTION;
EXCEPTION WHEN ERROR THEN
  ROLLBACK TRANSACTION;
  RAISE USING MESSAGE = FORMAT('Fact load rolled back: %s', @@error.message);
END;

SELECT
  (SELECT COUNTIF(is_new)     FROM changed_crash) AS crashes_new,
  (SELECT COUNTIF(NOT is_new) FROM changed_crash) AS crashes_changed,
  crashes_removed,
  rows_inserted,
  rows_deleted;


-- ============================================================
-- SECTION 6: POST-LOAD SANITY CHECKS (run after each load)
-- ============================================================

-- Rows that fell through to Unknown members. Expected on the current feed:
-- crash dates 7 (5 NULL crashdate + 2 dated 1900-02-06), reported dates 7
-- (NULL datecrashreported), times 5 (NULL crashdate). The four junk and
-- location dimensions must be 0 — anything else means a natural key is
-- computed differently in Sections 4 and 5.
SELECT COUNTIF(crash_date_key     = 19000101) AS unknown_crash_dates,
       COUNTIF(reported_date_key  = 19000101) AS unknown_reported_dates,
       COUNTIF(crash_time_key     = -1)       AS unknown_times,
       COUNTIF(location_key       = -1)       AS unknown_locations,
       COUNTIF(conditions_key     = -1)       AS unknown_conditions,
       COUNTIF(crash_type_key     = -1)       AS unknown_crash_types,
       COUNTIF(person_profile_key = -1)       AS unknown_person_profiles
FROM crashes.fact_crash_person;

-- Duplicate natural keys in a dimension = broken MERGE logic.
SELECT 'dim_location' AS dim, location_nk AS nk, COUNT(*) AS n
FROM crashes.dim_location GROUP BY location_nk HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_conditions', conditions_nk, COUNT(*)
FROM crashes.dim_conditions GROUP BY conditions_nk HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_crash_type', crash_type_nk, COUNT(*)
FROM crashes.dim_crash_type GROUP BY crash_type_nk HAVING COUNT(*) > 1
UNION ALL
SELECT 'dim_person_profile', person_profile_nk, COUNT(*)
FROM crashes.dim_person_profile GROUP BY person_profile_nk HAVING COUNT(*) > 1;

-- Grain check. fact_rows must equal distinct_socrata_ids (else double-load).
-- A republish's regenerated ids would pass this check, which is why
-- Section 5 also asserts each staged crash's row count before committing.
-- persons_per_crash should sit near 1.96 — near 1.0 means staging was
-- deduped on instanceid upstream.
SELECT
  (SELECT COUNT(*) FROM crashes.stg_crash_person)  AS staged,
  COUNT(*)                                         AS fact_rows,
  COUNT(DISTINCT socrata_id)                       AS distinct_socrata_ids,
  COUNT(DISTINCT instanceid)                       AS distinct_crashes,
  ROUND(SAFE_DIVIDE(COUNT(*), COUNT(DISTINCT instanceid)), 3) AS persons_per_crash
FROM crashes.fact_crash_person;

-- Source data quality. Expected to be nonzero — these measure the feed,
-- not a bug — but a sudden jump means the feed changed.
SELECT COUNTIF(age IS NULL)                   AS null_or_invalid_age,
       COUNTIF(crash_datetime IS NULL)        AS unparseable_crash_datetime,
       COUNTIF(reporting_lag_hours < 0)       AS reported_before_crash,
       COUNTIF(reporting_lag_hours > 24 * 30) AS reported_over_30_days_later
FROM crashes.fact_crash_person;

-- Canonicalization coverage. A growing count here means the feed introduced
-- a label the cleaning view's CASE ladders don't recognize.
SELECT 'injury_severity' AS attribute, p.injury_severity AS value, COUNT(*) AS n
FROM crashes.fact_crash_person f JOIN crashes.dim_person_profile p USING (person_profile_key)
WHERE p.injury_severity = 'Unknown' AND p.injury_severity_raw IS NOT NULL
GROUP BY p.injury_severity
UNION ALL
SELECT 'unit_category', p.unit_category, COUNT(*)
FROM crashes.fact_crash_person f JOIN crashes.dim_person_profile p USING (person_profile_key)
WHERE p.unit_category = 'Other'
GROUP BY p.unit_category
UNION ALL
SELECT 'crash_severity', ct.crash_severity, COUNT(*)
FROM crashes.fact_crash_person f JOIN crashes.dim_crash_type ct USING (crash_type_key)
WHERE ct.crash_severity = 'Unknown' AND ct.crash_severity_raw IS NOT NULL
GROUP BY ct.crash_severity;

-- Coding-era split. Both eras should be present, with comparable fatality
-- rates — a large gap means the two scales aren't being reconciled.
SELECT ct.coding_era,
       COUNT(*)                                                   AS person_rows,
       SUM(f.is_fatal)                                            AS fatalities,
       ROUND(100 * SAFE_DIVIDE(SUM(f.is_injured), COUNT(*)), 2)   AS pct_injured
FROM crashes.fact_crash_person f
JOIN crashes.dim_crash_type ct USING (crash_type_key)
GROUP BY ct.coding_era
ORDER BY person_rows DESC;


-- ============================================================
-- SECTION 7: ROLE-PLAYING VIEWS (run once, after Section 1)
-- One physical dim_date, two logical roles. Each view renames the columns
-- so a query joining both dates never has an ambiguous `year`.
-- ============================================================

CREATE OR REPLACE VIEW crashes.dim_crash_date AS
SELECT date_key         AS crash_date_key,
       full_date        AS crash_full_date,   -- not crash_date: that name is on the fact
       day_of_week_name AS crash_day_name,
       is_weekend       AS crash_is_weekend,
       month_name       AS crash_month_name,
       year_month       AS crash_year_month,
       quarter          AS crash_quarter,
       year             AS crash_year
FROM crashes.dim_date;

CREATE OR REPLACE VIEW crashes.dim_reported_date AS
SELECT date_key         AS reported_date_key,
       full_date        AS reported_full_date,
       day_of_week_name AS reported_day_name,
       is_weekend       AS reported_is_weekend,
       month_name       AS reported_month_name,
       year_month       AS reported_year_month,
       quarter          AS reported_quarter,
       year             AS reported_year
FROM crashes.dim_date;


-- ============================================================
-- SECTION 8: RETIRED — delete reconciliation now runs inside Section 5's
-- transaction, so a load can't leave replaced crashes without their
-- matching deletes (or the reverse). Kept so section numbers stay stable.
-- ============================================================


-- ============================================================
-- SECTION 9: ETL LOAD LOG (every load, last)
-- Parameters come from Run_Pipeline.py; the counts are the ones Section 5's
-- final SELECT returns. fact_updated stays NULL: rows are replaced, never
-- updated.
-- ============================================================

INSERT INTO crashes.etl_load_log
  (load_id, load_mode, window_start, started_at, finished_at,
   rows_staged, fact_inserted, fact_updated, fact_deleted, socrata_updated_at,
   crashes_new, crashes_changed, crashes_removed)
SELECT GENERATE_UUID(), @load_mode, @window_start, @started_at, CURRENT_TIMESTAMP(),
       @rows_staged, @fact_inserted, NULL, @fact_deleted,
       (SELECT MAX(socrata_updated_at) FROM crashes.stg_crash_person),
       @crashes_new, @crashes_changed, @crashes_removed;
