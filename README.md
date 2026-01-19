# NSW Census Travel-to-Work Analytics (2011 / 2016 / 2021)

This repository contains a reproducible SQL view layer for analysing Australian Census **travel-to-work** patterns in **New South Wales (NSW)** across the 2011, 2016, and 2021 census years.

The core outputs support:
- **Mode share trends** (public transport, private vehicle, WFH, mixed modes, etc.)
- **Origin–destination (OD) commuting flows** between SA2s
- **CBD dependency** metrics (flows into the Sydney Inner City hub)
- A transparent **2021 bus-to-CBD candidate ranking** based on demand, growth, and mode mix

---

## Repository Contents

- **SQL view layer** (Oracle SQL): builds a consistent dimension + fact model and produces analysis-ready metrics.
- (Optional) Visualisation notebooks / slides: downstream analysis using exported metric tables (not required to run the SQL).

---

## Data Sources

### Geography (SA2 dimension tables)
- `SA2_2011_AUST`
- `SA2_2016_AUST`
- `SA2_2021_AUST`

### Travel mode by SA2 of residence
- `SA2RESIDENTXMETHODOFTRANSPORT2011`
- `SA2RESIDENTXMETHODOFTRANSPORT2016`
- `SA2RESIDENTXMETHODOFTRANSPORT2021`

### SA2 residence to SA2 workplace flows (OD)
- `SA2RESIDENTXSA2WORK2011`
- `SA2RESIDENTXSA2WORK2016`
- `SA2RESIDENTXSA2WORK2021`

---

## Key Data Assumptions

1. SA2 names are joinable via normalised text keys (`UPPER(TRIM(name))`).
2. Census `COUNT` fields represent comparable commuter counts within each year.
3. Non-informative travel categories are excluded from mode share calculations.
4. “Sydney Inner City” SA3 is a valid proxy for CBD destinations.
5. 2021 boundary change metadata provides context but not a full concordance.
6. Non-geographic OD destinations are excluded from analysis.
7. Thresholds applied to the 2021 bus-candidate shortlist reduce noise from small bases.

---

## Key Concepts (definitions)

### SA2 / SA3 / SA4
Australian Statistical Geography Standard (ASGS) areas. SA2 is used as the primary unit of residential analysis, with SA3/SA4 attached for aggregation and slicing.

### Informative commuters
The transport tables include non-informative categories such as:
- `Not stated`
- `Not applicable`
- `Did not go to work`

For mode share analysis, these are excluded via `is_informative = 1`.

### Mode groups (standardised)
Raw transport mode strings are grouped into:
- `PUBLIC_TRANSPORT` (bus/train/ferry/tram/light rail)
- `PRIVATE_VEHICLE` (car/truck/motorbike/scooter/taxi)
- `WFH` (worked at home)
- `MIXED_PT_PRIVATE` (PT + car combinations)
- `ACTIVE_TRANSPORT` (walk only / bicycle)
- `OTHER`
- `NON_INFORMATIVE`

### CBD definition
CBD destinations are defined as SA2s that belong to the SA3:
- **Sydney Inner City**

This is materialised in `V_DIM_CBD_SA2` and used to calculate `CBD dependency` and `CBD volume`.

---

## Design Principles

- **Consistency across years:** builds a unified SA2 dimension `V_DIM_SA2` (2011/2016/2021)
- **Text joins via normalised keys:** `UPPER(TRIM(name))` to align SA2 labels across tables
- **Union compatibility:** `UNION ALL` views explicitly align data types (`CAST`, `TO_CHAR`) to avoid cross-year incompatibilities
- **Transparent QA:** `SELECT *` and `COUNT(*)` checks are kept after key steps for validation

---

## Pipeline Overview (Views Produced)

### 1) Dimension layer (SA2)
Creates year-specific SA2 dimension views and a unified dimension:
- `V_DIM_SA2_2011`, `V_DIM_SA2_2016`, `V_DIM_SA2_2021`
- `V_DIM_SA2` (union across years)

Includes 2021 boundary change metadata:
- `change_flag`, `change_label` (only available in 2021; null for prior years)

### 2) Transport (mode) facts
Raw → clean → NSW-only analysis subset:
- `V_TRANSPORT_RAW_2011/2016/2021`
- `V_TRANSPORT_CLEAN_2011/2016/2021`
- `V_TRANSPORT_CLEAN` (union)
- `V_TRANSPORT_CLEAN_NSW` (NSW + informative commuters only)

### 3) OD flow facts (residence → workplace)
Raw → clean → NSW-only analysis subset:
- `V_FLOW_RAW_2011/2016/2021`
- `V_FLOW_CLEAN_2011/2016/2021`
- `V_FLOW_CLEAN` (union)
- `V_FLOW_CLEAN_NSW`
- `V_FLOW_ANALYSIS_NSW` (applies stable exclusions to remove non-geographic destinations)

### 4) QA / validation
- `V_QA_UNMATCHED_TRANSPORT_2021`
- `V_QA_UNMATCHED_FLOW_2021`
Used to detect unmapped SA2 labels (primarily relevant for 2021 special categories).

### 5) Metrics layer
Key metric views include:
- `V_METRIC_MODE_SHARE_NSW` (SA2-year mode group shares)
- `V_METRIC_WFH_NSW`
- `V_METRIC_MIXED_MODE_NSW`
- `V_METRIC_SELF_CONTAINMENT_NSW`
- `V_METRIC_CBD_DEPENDENCY_NSW`
- `V_METRIC_MODE_TREND_NSW` (NSW-wide year-level mode trend)
- `V_METRIC_SA2_MODE_FEATURES_NSW` (SA2-year feature table for modelling/scoring)
- `V_METRIC_CBD_VOLUME_NSW`
- `V_METRIC_CBD_GROWTH_16_21_NSW`

### 6) 2021 bus-to-CBD candidate ranking
- `V_METRIC_BUS_CANDIDATE_2021_NSW_NORMALIZED`

This produces a ranked shortlist of SA2 origins where additional express capacity into the CBD may be considered.

---

## Bus Candidate Scoring (2021)

The 2021 shortlist applies minimum thresholds:
- `CBD commuters >= 50`
- `Total commuters >= 500`
- Required fields present (`pt_share`, `wfh_share`, `mixed_share`)

Score formula (normalised components):
- **40%**: CBD commuter volume (demand)
- **20%**: CBD growth rate (2016 → 2021) (trend / emerging demand)
- **20%**: Low public transport share (service gap)
- **10%**: Low WFH share (higher likelihood of physical commuting)
- **10%**: Mixed-mode share (signals park-and-ride / multi-stage journeys)

> The weights are designed to balance demand, growth, and service gap signals. They are intended as a transparent prioritisation heuristic, not a final investment model.

---

## How to Run

1. Load required tables into Oracle (or adapt the script for your SQL dialect).
2. Execute the SQL file from top to bottom to create views in dependency order.
3. Query final outputs, for example:
   - `SELECT * FROM V_METRIC_MODE_TREND_NSW ORDER BY census_year;`
   - `SELECT * FROM V_METRIC_BUS_CANDIDATE_2021_NSW_NORMALIZED;`

---

## Limitations

- **Text joins:** relies on consistent SA2 naming in raw tables; QA views highlight unmapped keys.
- **Boundary changes:** 2021 includes change metadata, but cross-year comparability may still require additional concordance logic if strict geographic alignment is needed.
- **CBD definition:** uses Sydney Inner City SA3 as a practical proxy; alternate CBD definitions may change results.
- **Heuristic ranking:** bus candidate score is a prioritisation aid and should be paired with network constraints, travel time, capacity, and feasibility checks.

---

## Author

Kevin Tran  
