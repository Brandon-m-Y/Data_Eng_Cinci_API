# Spec: Cincinnati Crash Space-Time Panel

## Objective

Build a complete space-time panel from the Cincinnati Traffic Crash Reports dataset
(Socrata `rvmt-pkmq`) so that a count model can be trained on crash **rates**, not
crash composition.

The source data contains only crashes. Every row is a positive event, so it cannot
answer "how likely is a crash." Aggregating to a complete grid of
`(location x time)` cells creates the missing negative observations: cells where no
crash occurred appear as `crashes = 0`.

**Target grain for v1: neighborhood x day.** Roughly 50 neighborhoods x 3,650 days
= ~180k rows, mean ~0.8 crashes/cell. Do not jump to finer grain until this works.

---

## Step 1 — Ingest

- Endpoint: `https://data.cincinnati-oh.gov/resource/rvmt-pkmq.json`
- Page with `$limit` / `$offset`, ordered by `:id`. Use `$limit=50000`.
- Socrata returns all fields as strings. Nothing is typed until you type it.
- **Do not hardcode column names from this spec.** Inspect the actual response
  first and map real field names to the roles below.

Cache raw pulls to local parquet so iteration doesn't re-hit the API.

### Field roles to identify

| Role | Notes |
|---|---|
| crash id | unique per collision; used for dedup |
| crash timestamp | parse to datetime |
| neighborhood | CPD neighborhood name |
| latitude / longitude | for later hex-grid versions |
| injury severity | not used in v1, keep for later |
| person attributes | age, gender, unit type — signals person-level grain |

---

## Step 2 — Resolve the grain (critical)

Check whether rows are crash-level or person/unit-level:

```python
len(raw), raw[id_col].nunique()
```

If these differ, each row is one person or vehicle involved, not one collision.
Counting raw rows would inflate multi-occupant crashes and bias the panel toward
severe events.

Deduplicate to one row per crash id for the count panel. Keep the person-level
frame separately — it is the basis for a future severity label.

---

## Step 3 — Type and filter

- Parse timestamp; drop unparseable rows.
- Derive `day` (normalized date) and `hood` (stripped, uppercased).
- Plot monthly crash volume across the full history. Look for level shifts that
  indicate reporting-system changes rather than real trend changes.
- Trim to the period where reporting volume is stable. Start with 2016-01-01
  onward and adjust based on what the plot shows. Record the chosen bounds and
  the reason.

---

## Step 4 — Build the spine

The panel is an index, not a table. Build the complete index, then reindex the
sparse aggregation onto it.

```python
hoods = sorted(crashes["hood"].dropna().unique())
days  = pd.date_range(crashes["day"].min(), crashes["day"].max(), freq="D")

spine  = pd.MultiIndex.from_product([hoods, days], names=["hood", "day"])
counts = crashes.groupby(["hood", "day"]).size().rename("crashes")
panel  = counts.reindex(spine, fill_value=0).reset_index()
```

`from_product` is the cross join. `reindex(fill_value=0)` is the left join and the
zero-fill in one operation — faster than materializing a frame and merging.

### Assertions

```python
assert panel["crashes"].sum() == len(crashes.dropna(subset=["hood"]))
assert len(panel) == len(hoods) * len(days)
```

Report zero fraction and mean. Expect ~50-70% zeros at this grain. Above 95%
means the grain is too fine.

---

## Step 5 — Features

### Lags — must be within-cell and strictly backward-looking

```python
panel = panel.sort_values(["hood", "day"])
g = panel.groupby("hood")["crashes"]

panel["lag_7"]   = g.shift(7)
panel["lag_364"] = g.shift(364)                                   # same weekday, prior year
panel["roll_28"] = g.shift(1).rolling(28, min_periods=14).mean()
panel["roll_91"] = g.shift(1).rolling(91, min_periods=30).mean()
```

**The `.shift(1)` before `.rolling()` is mandatory.** Without it the window
includes the current day and the model sees its own target. This is the primary
failure mode for this project: validation looks excellent and the model is
worthless.

### Calendar

`dow`, `month`, `is_weekend`, US holiday flag, and cyclical day-of-year:

```python
panel["doy_sin"] = np.sin(2*np.pi*panel["day"].dt.dayofyear/365.25)
panel["doy_cos"] = np.cos(2*np.pi*panel["day"].dt.dayofyear/365.25)
```

### External joins (later phases, but design the schema for them now)

- **Weather** — NOAA daily observations, CVG or Lunken. Joins on `day` alone,
  broadcasts across all neighborhoods.
- **Static neighborhood attributes** — road miles, intersection count, ODOT AADT,
  ACS block-group demographics. Join on `hood` alone.

Both are one-key merges because the spine already exists. That is the payoff for
building the index first.

---

## Step 6 — Exposure

Without an exposure denominator the model learns which neighborhoods are busy, not
which are dangerous. Downtown has more crashes and also far more traffic.

Add an `exposure` column. Ideal: `AADT x road_miles x hours_in_cell`. Until ODOT
AADT is joined, use road-miles from OSM as a stand-in so the plumbing exists.

Pass as an offset so predictions are rates rather than counts:

- statsmodels GLM: `offset=np.log(exposure)`
- LightGBM/XGBoost: `init_score` / `base_margin` = `np.log(exposure)`

---

## Step 7 — Split and model

### Split temporally. Never shuffle.

```python
train = panel[panel.day <  "2024-01-01"]
valid = panel[(panel.day >= "2024-01-01") & (panel.day < "2025-01-01")]
test  = panel[panel.day >= "2025-01-01"]
```

Random splits leak: adjacent days of the same neighborhood are nearly identical,
so the model memorizes cell identity and reports a score that will not reproduce.

### Model

Counts are overdispersed, so Poisson is the floor, not the answer.

- Baseline: negative binomial GLM (statsmodels), for interpretable coefficients.
- Main: LightGBM with `objective="poisson"`, `init_score=log(exposure)`.
- Compare against a naive baseline of `roll_28` alone. If the model doesn't beat
  it, stop and fix features before tuning.

### Metrics

- **Poisson deviance** as the primary loss (not RMSE — wrong for counts).
- **Prediction Accuracy Index (PAI)** for the writeup: share of next period's
  actual crashes falling in the model's top 1% of cells, divided by 1%. A PAI of
  15 means the top 1% of the city captures 15% of crashes. This is the number a
  traffic engineer understands.

---

## Deliverables

1. `ingest.py` — paged Socrata pull, cached to parquet.
2. `build_panel.py` — dedup, type, spine construction, feature engineering.
   Grain must be a parameter, not hardcoded.
3. `train.py` — temporal split, baseline + LightGBM, deviance and PAI.
4. `notebooks/eda.ipynb` — grain check, monthly volume plot with the coverage
   trim justified, zero-fraction and count distribution.
5. Tests covering: the sum assertion, panel row count, and a leakage test
   confirming lag features for a given row use only strictly prior days.

## Definition of done for v1

- Panel builds reproducibly with assertions passing.
- Zero fraction reported and within the expected band.
- Leakage test passes.
- LightGBM beats the `roll_28` baseline on held-out Poisson deviance.
- PAI computed on the test year.

## Explicitly out of scope for v1

Hex grids, sub-daily bins, road-segment grain, severity modeling, BigQuery
migration. Each is a follow-on once neighborhood-day is validated. Keep grain
parameterized so scaling up is a config change, not a rewrite.
