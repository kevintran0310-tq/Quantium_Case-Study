/* =====================================================================
   NSW Census Travel-to-Work Analytics — SQL Views (2011 / 2016 / 2021)
   ---------------------------------------------------------------------
   What this script does
   1) Builds a consistent SA2 dimension across years (incl. 2021 change metadata)
   2) Cleans transport-by-residence (mode groups + informative filter)
   3) Cleans OD flows (residence -> workplace) and applies stable analysis exclusions
   4) Derives metrics used in analysis (mode shares, WFH, mixed mode, CBD dependency)
   5) Produces a transparent 2021 bus-to-CBD candidate ranking

   Conventions
   - Text joins use normalised keys: UPPER(TRIM(name))
   - UNION ALL views align datatypes explicitly (TO_CHAR / CAST) for compatibility
   - QA checks (SELECT * / COUNT) are kept after each step for validation
   ===================================================================== */


/* =====================================================================
   1) DIMENSION LAYER: SA2 LOOKUP TABLES (2011 / 2016 / 2021)
   Purpose
   - Standardise SA2 geography fields across census years
   - Provide consistent join keys (sa2_name_key) for text-based joins
   - Attach hierarchy (SA3/SA4/GCCSA/State) for slicing
   - Attach area (sqkm) as supporting proxy
   - Preserve boundary-change metadata where available (2021)
   ===================================================================== */

CREATE OR REPLACE VIEW V_DIM_SA2_2011 AS
SELECT
  2011                        AS census_year,
  SA2_MAINCODE_2011           AS sa2_code,
  SA2_NAME_2011               AS sa2_name,
  UPPER(TRIM(SA2_NAME_2011))  AS sa2_name_key,

  SA3_CODE_2011               AS sa3_code,
  SA3_NAME_2011               AS sa3_name,
  SA4_CODE_2011               AS sa4_code,
  SA4_NAME_2011               AS sa4_name,
  GCCSA_CODE_2011             AS gccsa_code,
  GCCSA_NAME_2011             AS gccsa_name,
  STATE_CODE_2011             AS state_code,
  STATE_NAME_2011             AS state_name,

  (AREA_ALBERS_SQM / 1000000) AS area_sqkm,     -- sqm -> sqkm (2011 schema)

  CAST(NULL AS VARCHAR2(10))  AS change_flag,
  CAST(NULL AS VARCHAR2(200)) AS change_label
FROM SA2_2011_AUST;

SELECT * FROM V_DIM_SA2_2011;
SELECT COUNT(*) AS row_cnt FROM V_DIM_SA2_2011;


CREATE OR REPLACE VIEW V_DIM_SA2_2016 AS
SELECT
  2016                        AS census_year,
  SA2_MAINCODE_2016           AS sa2_code,
  SA2_NAME_2016               AS sa2_name,
  UPPER(TRIM(SA2_NAME_2016))  AS sa2_name_key,

  SA3_CODE_2016               AS sa3_code,
  SA3_NAME_2016               AS sa3_name,
  SA4_CODE_2016               AS sa4_code,
  SA4_NAME_2016               AS sa4_name,
  GCCSA_CODE_2016             AS gccsa_code,
  GCCSA_NAME_2016             AS gccsa_name,
  STATE_CODE_2016             AS state_code,
  STATE_NAME_2016             AS state_name,

  (AREA_ALBERS_SQM / 1000000) AS area_sqkm,     -- sqm -> sqkm (2016 schema)

  CAST(NULL AS VARCHAR2(10))  AS change_flag,
  CAST(NULL AS VARCHAR2(200)) AS change_label
FROM SA2_2016_AUST;

SELECT * FROM V_DIM_SA2_2016;
SELECT COUNT(*) AS row_cnt FROM V_DIM_SA2_2016;


CREATE OR REPLACE VIEW V_DIM_SA2_2021 AS
SELECT
  2021                        AS census_year,
  SA2_CODE_2021               AS sa2_code,
  SA2_NAME_2021               AS sa2_name,
  UPPER(TRIM(SA2_NAME_2021))  AS sa2_name_key,

  SA3_CODE_2021               AS sa3_code,
  SA3_NAME_2021               AS sa3_name,
  SA4_CODE_2021               AS sa4_code,
  SA4_NAME_2021               AS sa4_name,
  GCCSA_CODE_2021             AS gccsa_code,
  GCCSA_NAME_2021             AS gccsa_name,
  STATE_CODE_2021             AS state_code,
  STATE_NAME_2021             AS state_name,

  AREA_ALBERS_SQKM            AS area_sqkm,     -- already sqkm (2021 schema)

  CHANGE_FLAG_2021            AS change_flag,
  CHANGE_LABEL_2021           AS change_label
FROM SA2_2021_AUST;

SELECT * FROM V_DIM_SA2_2021;
SELECT COUNT(*) AS row_cnt FROM V_DIM_SA2_2021;


-- Unified SA2 dimension across years (type-aligned for UNION ALL)
CREATE OR REPLACE VIEW V_DIM_SA2 AS
SELECT
  census_year,
  sa2_code,
  sa2_name,
  sa2_name_key,
  sa3_code,
  sa3_name,
  sa4_code,
  sa4_name,
  gccsa_code,
  gccsa_name,
  state_code,
  state_name,
  area_sqkm,
  change_flag,
  change_label
FROM V_DIM_SA2_2011

UNION ALL
SELECT
  census_year,
  sa2_code,
  sa2_name,
  sa2_name_key,
  sa3_code,
  sa3_name,
  sa4_code,
  sa4_name,
  gccsa_code,
  gccsa_name,
  state_code,
  state_name,
  area_sqkm,
  change_flag,
  change_label
FROM V_DIM_SA2_2016

UNION ALL
SELECT
  census_year,
  sa2_code,
  sa2_name,
  sa2_name_key,
  sa3_code,
  sa3_name,
  sa4_code,
  sa4_name,
  gccsa_code,
  gccsa_name,
  state_code,
  state_name,
  area_sqkm,
  TO_CHAR(change_flag) AS change_flag,
  change_label
FROM V_DIM_SA2_2021;

SELECT * FROM V_DIM_SA2;
SELECT COUNT(*) AS row_cnt FROM V_DIM_SA2;
SELECT census_year, COUNT(*) AS row_cnt
FROM V_DIM_SA2
GROUP BY census_year
ORDER BY census_year;

/* =====================================================================
   2) RAW TRANSPORT TABLES (SA2 Residence x Method of Travel)
   Purpose
   - Extract raw transport counts by SA2 residence
   - Create normalised join key for SA2 names (sa2_res_key)
   ===================================================================== */

CREATE OR REPLACE VIEW V_TRANSPORT_RAW_2011 AS
SELECT
  2011                      AS census_year,
  SA2RESIDENCE              AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE)) AS sa2_res_key,
  METHODOFTRAVEL            AS mode_raw,
  UPPER(TRIM(METHODOFTRAVEL)) AS mode_key,
  COUNT                     AS commuter_count
FROM SA2RESIDENTXMETHODOFTRANSPORT2011;

SELECT * FROM V_TRANSPORT_RAW_2011;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_RAW_2011;


CREATE OR REPLACE VIEW V_TRANSPORT_RAW_2016 AS
SELECT
  2016                      AS census_year,
  SA2RESIDENCE              AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE)) AS sa2_res_key,
  METHODOFTRAVEL            AS mode_raw,
  UPPER(TRIM(METHODOFTRAVEL)) AS mode_key,
  COUNT                     AS commuter_count
FROM SA2RESIDENTXMETHODOFTRANSPORT2016;

SELECT * FROM V_TRANSPORT_RAW_2016;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_RAW_2016;


CREATE OR REPLACE VIEW V_TRANSPORT_RAW_2021 AS
SELECT
  2021                      AS census_year,
  SA2RESIDENCE              AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE)) AS sa2_res_key,
  METHODOFTRAVEL            AS mode_raw,
  UPPER(TRIM(METHODOFTRAVEL)) AS mode_key,
  COUNT                     AS commuter_count
FROM SA2RESIDENTXMETHODOFTRANSPORT2021;

SELECT * FROM V_TRANSPORT_RAW_2021;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_RAW_2021;

/* =====================================================================
   3) TRANSPORT CLEAN LAYER
   Purpose
   - Join raw transport to SA2 dimension (adds codes + geography)
   - Standardise mode text and group into planning categories:
     NON_INFORMATIVE, WFH, MIXED_PT_PRIVATE, PUBLIC_TRANSPORT,
     ACTIVE_TRANSPORT, PRIVATE_VEHICLE, OTHER
   - Provide is_informative flag to exclude "not stated / not applicable"
   ===================================================================== */

CREATE OR REPLACE VIEW V_TRANSPORT_CLEAN_2011 AS
SELECT
  r.census_year,
  d.sa2_code  AS sa2_res_code,
  d.sa2_name  AS sa2_res_name_std,
  d.state_name,
  d.sa4_name,
  d.gccsa_name,

  UPPER(TRIM(
    REPLACE(
      REPLACE(r.mode_raw, 'Car, as driver',    'Car as driver'),
                       'Car, as passenger', 'Car as passenger'
    )
  )) AS mode_raw_norm,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED')
      THEN 'NON_INFORMATIVE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WORKED AT HOME')
      THEN 'WFH'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
     AND REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|DRIVER|PASSENGER')
      THEN 'MIXED_PT_PRIVATE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
      THEN 'PUBLIC_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WALKED ONLY|BICYCLE')
     AND NOT REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL|CAR|TRUCK|MOTORBIKE|SCOOTER')
      THEN 'ACTIVE_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|TAXI')
      THEN 'PRIVATE_VEHICLE'
    ELSE 'OTHER'
  END AS mode_group,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED') THEN 0
    ELSE 1
  END AS is_informative,

  r.commuter_count
FROM V_TRANSPORT_RAW_2011 r
JOIN V_DIM_SA2_2011 d
  ON d.sa2_name_key = r.sa2_res_key;

SELECT * FROM V_TRANSPORT_CLEAN_2011;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_CLEAN_2011;


CREATE OR REPLACE VIEW V_TRANSPORT_CLEAN_2016 AS
SELECT
  r.census_year,
  d.sa2_code  AS sa2_res_code,
  d.sa2_name  AS sa2_res_name_std,
  d.state_name,
  d.sa4_name,
  d.gccsa_name,

  UPPER(TRIM(
    REPLACE(
      REPLACE(r.mode_raw, 'Car, as driver',    'Car as driver'),
                       'Car, as passenger', 'Car as passenger'
    )
  )) AS mode_raw_norm,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED')
      THEN 'NON_INFORMATIVE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WORKED AT HOME')
      THEN 'WFH'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
     AND REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|DRIVER|PASSENGER')
      THEN 'MIXED_PT_PRIVATE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
      THEN 'PUBLIC_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WALKED ONLY|BICYCLE')
     AND NOT REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL|CAR|TRUCK|MOTORBIKE|SCOOTER')
      THEN 'ACTIVE_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|TAXI')
      THEN 'PRIVATE_VEHICLE'
    ELSE 'OTHER'
  END AS mode_group,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED') THEN 0
    ELSE 1
  END AS is_informative,

  r.commuter_count
FROM V_TRANSPORT_RAW_2016 r
JOIN V_DIM_SA2_2016 d
  ON d.sa2_name_key = r.sa2_res_key;

SELECT * FROM V_TRANSPORT_CLEAN_2016;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_CLEAN_2016;


CREATE OR REPLACE VIEW V_TRANSPORT_CLEAN_2021 AS
SELECT
  r.census_year,
  d.sa2_code  AS sa2_res_code,
  d.sa2_name  AS sa2_res_name_std,
  d.state_name,
  d.sa4_name,
  d.gccsa_name,
  d.change_flag,
  d.change_label,

  UPPER(TRIM(
    REPLACE(
      REPLACE(r.mode_raw, 'Car, as driver',    'Car as driver'),
                       'Car, as passenger', 'Car as passenger'
    )
  )) AS mode_raw_norm,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED')
      THEN 'NON_INFORMATIVE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WORKED AT HOME')
      THEN 'WFH'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
     AND REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|DRIVER|PASSENGER')
      THEN 'MIXED_PT_PRIVATE'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL')
      THEN 'PUBLIC_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'WALKED ONLY|BICYCLE')
     AND NOT REGEXP_LIKE(UPPER(r.mode_raw), 'BUS|TRAIN|FERRY|TRAM|LIGHT RAIL|CAR|TRUCK|MOTORBIKE|SCOOTER')
      THEN 'ACTIVE_TRANSPORT'
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'CAR|TRUCK|MOTORBIKE|SCOOTER|TAXI')
      THEN 'PRIVATE_VEHICLE'
    ELSE 'OTHER'
  END AS mode_group,

  CASE
    WHEN REGEXP_LIKE(UPPER(r.mode_raw), 'NOT APPLICABLE|DID NOT GO TO WORK|NOT STATED') THEN 0
    ELSE 1
  END AS is_informative,

  r.commuter_count
FROM V_TRANSPORT_RAW_2021 r
JOIN V_DIM_SA2_2021 d
  ON d.sa2_name_key = r.sa2_res_key;

SELECT * FROM V_TRANSPORT_CLEAN_2021;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_CLEAN_2021;


-- Unified transport fact table (type-aligned)
CREATE OR REPLACE VIEW V_TRANSPORT_CLEAN AS
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  state_name,
  sa4_name,
  gccsa_name,
  CAST(NULL AS VARCHAR2(10))  AS change_flag,
  CAST(NULL AS VARCHAR2(200)) AS change_label,
  mode_raw_norm               AS mode_raw,
  mode_group,
  is_informative,
  commuter_count
FROM V_TRANSPORT_CLEAN_2011

UNION ALL
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  state_name,
  sa4_name,
  gccsa_name,
  CAST(NULL AS VARCHAR2(10))  AS change_flag,
  CAST(NULL AS VARCHAR2(200)) AS change_label,
  mode_raw_norm               AS mode_raw,
  mode_group,
  is_informative,
  commuter_count
FROM V_TRANSPORT_CLEAN_2016

UNION ALL
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  state_name,
  sa4_name,
  gccsa_name,
  TO_CHAR(change_flag)        AS change_flag,
  change_label                AS change_label,
  mode_raw_norm               AS mode_raw,
  mode_group,
  is_informative,
  commuter_count
FROM V_TRANSPORT_CLEAN_2021;

SELECT * FROM V_TRANSPORT_CLEAN;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_CLEAN;
SELECT census_year, COUNT(*) AS row_cnt
FROM V_TRANSPORT_CLEAN
GROUP BY census_year
ORDER BY census_year;


-- NSW-only, informative-only transport facts (main analysis dataset for mode trends)
CREATE OR REPLACE VIEW V_TRANSPORT_CLEAN_NSW AS
SELECT *
FROM V_TRANSPORT_CLEAN
WHERE state_name = 'New South Wales'
  AND is_informative = 1;

SELECT * FROM V_TRANSPORT_CLEAN_NSW;
SELECT COUNT(*) AS row_cnt FROM V_TRANSPORT_CLEAN_NSW;
SELECT census_year, COUNT(*) AS row_cnt
FROM V_TRANSPORT_CLEAN_NSW
GROUP BY census_year
ORDER BY census_year;



/* =====================================================================
   4) RAW FLOW TABLES (SA2 Residence x SA2 Workplace)
   Purpose
   - Extract OD flows between SA2 residence and SA2 workplace
   - Normalise text keys to join to SA2 dimension
   ===================================================================== */

CREATE OR REPLACE VIEW V_FLOW_RAW_2011 AS
SELECT
  2011                        AS census_year,
  SA2RESIDENCE                AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE))   AS sa2_res_key,
  SA2PLACEOFWORK              AS sa2_work_name,
  UPPER(TRIM(SA2PLACEOFWORK)) AS sa2_work_key,
  COUNT                       AS commuter_count
FROM SA2RESIDENTXSA2WORK2011;

SELECT * FROM V_FLOW_RAW_2011;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_RAW_2011;


CREATE OR REPLACE VIEW V_FLOW_RAW_2016 AS
SELECT
  2016                      AS census_year,
  SA2RESIDENCE              AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE)) AS sa2_res_key,
  SA2WORKPLACE              AS sa2_work_name,
  UPPER(TRIM(SA2WORKPLACE)) AS sa2_work_key,
  COUNT                     AS commuter_count
FROM SA2RESIDENTXSA2WORK2016;

SELECT * FROM V_FLOW_RAW_2016;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_RAW_2016;


CREATE OR REPLACE VIEW V_FLOW_RAW_2021 AS
SELECT
  2021                      AS census_year,
  SA2RESIDENCE              AS sa2_res_name,
  UPPER(TRIM(SA2RESIDENCE)) AS sa2_res_key,
  SA2WORKPLACE              AS sa2_work_name,
  UPPER(TRIM(SA2WORKPLACE)) AS sa2_work_key,
  COUNT                     AS commuter_count
FROM SA2RESIDENTXSA2WORK2021;

SELECT * FROM V_FLOW_RAW_2021;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_RAW_2021;


/* =====================================================================
   5) FLOW CLEAN LAYER
   Purpose
   - Join OD flows to SA2 dimension for codes and geography
   - Preserve 2021 change metadata for context (res/work sides)
   ===================================================================== */

CREATE OR REPLACE VIEW V_FLOW_CLEAN_2011 AS
SELECT
  r.census_year,

  dR.sa2_code   AS sa2_res_code,
  dR.sa2_name   AS sa2_res_name_std,
  dR.state_name AS res_state_name,
  dR.sa4_name   AS res_sa4_name,

  dW.sa2_code   AS sa2_work_code,
  dW.sa2_name   AS sa2_work_name_std,
  dW.state_name AS work_state_name,
  dW.sa4_name   AS work_sa4_name,

  r.commuter_count
FROM V_FLOW_RAW_2011 r
JOIN V_DIM_SA2_2011 dR
  ON dR.sa2_name_key = r.sa2_res_key
JOIN V_DIM_SA2_2011 dW
  ON dW.sa2_name_key = r.sa2_work_key;

SELECT * FROM V_FLOW_CLEAN_2011;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_CLEAN_2011;


CREATE OR REPLACE VIEW V_FLOW_CLEAN_2016 AS
SELECT
  r.census_year,

  dR.sa2_code   AS sa2_res_code,
  dR.sa2_name   AS sa2_res_name_std,
  dR.state_name AS res_state_name,
  dR.sa4_name   AS res_sa4_name,

  dW.sa2_code   AS sa2_work_code,
  dW.sa2_name   AS sa2_work_name_std,
  dW.state_name AS work_state_name,
  dW.sa4_name   AS work_sa4_name,

  r.commuter_count
FROM V_FLOW_RAW_2016 r
JOIN V_DIM_SA2_2016 dR
  ON dR.sa2_name_key = r.sa2_res_key
JOIN V_DIM_SA2_2016 dW
  ON dW.sa2_name_key = r.sa2_work_key;

SELECT * FROM V_FLOW_CLEAN_2016;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_CLEAN_2016;


CREATE OR REPLACE VIEW V_FLOW_CLEAN_2021 AS
SELECT
  r.census_year,

  dR.sa2_code     AS sa2_res_code,
  dR.sa2_name     AS sa2_res_name_std,
  dR.state_name   AS res_state_name,
  dR.sa4_name     AS res_sa4_name,
  dR.change_flag  AS res_change_flag,
  dR.change_label AS res_change_label,

  dW.sa2_code     AS sa2_work_code,
  dW.sa2_name     AS sa2_work_name_std,
  dW.state_name   AS work_state_name,
  dW.sa4_name     AS work_sa4_name,
  dW.change_flag  AS work_change_flag,
  dW.change_label AS work_change_label,

  r.commuter_count
FROM V_FLOW_RAW_2021 r
JOIN V_DIM_SA2_2021 dR
  ON dR.sa2_name_key = r.sa2_res_key
JOIN V_DIM_SA2_2021 dW
  ON dW.sa2_name_key = r.sa2_work_key;

SELECT * FROM V_FLOW_CLEAN_2021;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_CLEAN_2021;


-- Unified OD flow fact table (type-aligned)
CREATE OR REPLACE VIEW V_FLOW_CLEAN AS
SELECT
  census_year,

  sa2_res_code,
  sa2_res_name_std,
  res_state_name,
  res_sa4_name,
  CAST(NULL AS VARCHAR2(10))  AS res_change_flag,
  CAST(NULL AS VARCHAR2(200)) AS res_change_label,

  sa2_work_code,
  sa2_work_name_std,
  work_state_name,
  work_sa4_name,
  CAST(NULL AS VARCHAR2(10))  AS work_change_flag,
  CAST(NULL AS VARCHAR2(200)) AS work_change_label,

  commuter_count
FROM V_FLOW_CLEAN_2011

UNION ALL
SELECT
  census_year,

  sa2_res_code,
  sa2_res_name_std,
  res_state_name,
  res_sa4_name,
  CAST(NULL AS VARCHAR2(10))  AS res_change_flag,
  CAST(NULL AS VARCHAR2(200)) AS res_change_label,

  sa2_work_code,
  sa2_work_name_std,
  work_state_name,
  work_sa4_name,
  CAST(NULL AS VARCHAR2(10))  AS work_change_flag,
  CAST(NULL AS VARCHAR2(200)) AS work_change_label,

  commuter_count
FROM V_FLOW_CLEAN_2016

UNION ALL
SELECT
  census_year,

  sa2_res_code,
  sa2_res_name_std,
  res_state_name,
  res_sa4_name,
  TO_CHAR(res_change_flag)    AS res_change_flag,
  res_change_label            AS res_change_label,

  sa2_work_code,
  sa2_work_name_std,
  work_state_name,
  work_sa4_name,
  TO_CHAR(work_change_flag)   AS work_change_flag,
  work_change_label           AS work_change_label,

  commuter_count
FROM V_FLOW_CLEAN_2021;

SELECT * FROM V_FLOW_CLEAN;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_CLEAN;
SELECT census_year, COUNT(*) AS row_cnt
FROM V_FLOW_CLEAN
GROUP BY census_year
ORDER BY census_year;


-- NSW-only flow dataset (used for commuting patterns and CBD dependency)
CREATE OR REPLACE VIEW V_FLOW_CLEAN_NSW AS
SELECT *
FROM V_FLOW_CLEAN
WHERE res_state_name = 'New South Wales';

SELECT * FROM V_FLOW_CLEAN_NSW;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_CLEAN_NSW;


/* =====================================================================
   6) QA CHECKS: UNMATCHED KEYS (2021)
   Purpose
   - Confirm whether any raw SA2 names fail to map to SA2 codes
   - This is primarily relevant in 2021 due to special / non-standard categories
   ===================================================================== */

CREATE OR REPLACE VIEW V_QA_UNMATCHED_TRANSPORT_2021 AS
SELECT DISTINCT r.sa2_res_name, r.sa2_res_key
FROM V_TRANSPORT_RAW_2021 r
LEFT JOIN V_DIM_SA2_2021 d
  ON d.sa2_name_key = r.sa2_res_key
WHERE d.sa2_code IS NULL;

SELECT * FROM V_QA_UNMATCHED_TRANSPORT_2021;
SELECT COUNT(*) AS unmatched_cnt FROM V_QA_UNMATCHED_TRANSPORT_2021;


CREATE OR REPLACE VIEW V_QA_UNMATCHED_FLOW_2021 AS
SELECT DISTINCT r.sa2_res_name, r.sa2_work_name
FROM V_FLOW_RAW_2021 r
LEFT JOIN V_DIM_SA2_2021 dR
  ON dR.sa2_name_key = r.sa2_res_key
LEFT JOIN V_DIM_SA2_2021 dW
  ON dW.sa2_name_key = r.sa2_work_key
WHERE dR.sa2_code IS NULL OR dW.sa2_code IS NULL;

SELECT * FROM V_QA_UNMATCHED_FLOW_2021;
SELECT COUNT(*) AS unmatched_cnt FROM V_QA_UNMATCHED_FLOW_2021;



/* =====================================================================
   7) INVESTIGATE UNMATCHED WORK SA2 VALUES (2021)
   Purpose
   - Identify which work destinations fail to map to SA2 codes and their impact
   ===================================================================== */

SELECT
  sa2_work_name,
  COUNT(*) AS row_cnt,
  SUM(commuter_count) AS commuters
FROM V_FLOW_RAW_2021 r
LEFT JOIN V_DIM_SA2_2021 d
  ON d.sa2_name_key = r.sa2_work_key
WHERE d.sa2_code IS NULL
GROUP BY sa2_work_name
ORDER BY commuters DESC;

/* =====================================================================
   8) ANALYSIS FLOW DATASET (NSW, EXCLUSIONS APPLIED)
   Purpose
   - Create the primary NSW OD dataset used for downstream flow metrics
   - Exclude non-geographic destinations that cannot be mapped reliably
   ===================================================================== */

CREATE OR REPLACE VIEW V_FLOW_ANALYSIS_NSW AS
SELECT *
FROM V_FLOW_CLEAN
WHERE UPPER(res_state_name) = 'NEW SOUTH WALES'
  AND sa2_work_code IS NOT NULL
  AND sa2_work_code <> '197979799';  -- Migratory - Offshore - Shipping (NSW)

SELECT * FROM V_FLOW_ANALYSIS_NSW;
SELECT COUNT(*) AS row_cnt FROM V_FLOW_ANALYSIS_NSW;
SELECT census_year, SUM(commuter_count) AS commuters
FROM V_FLOW_ANALYSIS_NSW
GROUP BY census_year
ORDER BY census_year;



/* =====================================================================
   9) WORKPLACE HUB VALIDATION (2021)
   Purpose
   - Rank NSW SA3 destinations by total worker volume (supports CBD focus)
   ===================================================================== */

CREATE OR REPLACE VIEW V_VAL_WORKPLACE_RANKING_SA3_2021 AS
SELECT
  dW.sa3_name AS workplace_sa3_hub,
  dW.sa4_name AS workplace_sa4_region,
  SUM(f.commuter_count) AS total_workers_at_destination,
  ROUND(
    100 * SUM(f.commuter_count) / SUM(SUM(f.commuter_count)) OVER (),
    2
  ) AS pct_of_nsw_workforce
FROM V_FLOW_ANALYSIS_NSW f
JOIN V_DIM_SA2_2021 dW
  ON dW.sa2_code = f.sa2_work_code
WHERE f.census_year = 2021
GROUP BY dW.sa3_name, dW.sa4_name
ORDER BY total_workers_at_destination DESC;

SELECT * FROM V_VAL_WORKPLACE_RANKING_SA3_2021;



/* =====================================================================
   10) METRICS LAYER (NSW)
   Purpose
   - Mode share by SA2-year
   - WFH rate by SA2-year
   - Mixed-mode rate by SA2-year
   - Self-containment rate by SA2-year
   - CBD dependency by SA2-year (Sydney Inner City SA3)
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_MODE_SHARE_NSW AS
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  mode_group,
  SUM(commuter_count) AS commuters,
  SUM(commuter_count) / SUM(SUM(commuter_count)) OVER (
    PARTITION BY census_year, sa2_res_code
  ) AS mode_share
FROM V_TRANSPORT_CLEAN_NSW
GROUP BY census_year, sa2_res_code, sa2_res_name_std, mode_group;

SELECT * FROM V_METRIC_MODE_SHARE_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_MODE_SHARE_NSW;


CREATE OR REPLACE VIEW V_METRIC_WFH_NSW AS
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  SUM(CASE WHEN mode_group = 'WFH' THEN commuter_count ELSE 0 END) AS wfh_commuters,
  SUM(commuter_count) AS total_commuters,
  SUM(CASE WHEN mode_group = 'WFH' THEN commuter_count ELSE 0 END) / SUM(commuter_count) AS wfh_rate
FROM V_TRANSPORT_CLEAN_NSW
GROUP BY census_year, sa2_res_code, sa2_res_name_std;

SELECT * FROM V_METRIC_WFH_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_WFH_NSW;


CREATE OR REPLACE VIEW V_METRIC_MIXED_MODE_NSW AS
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  SUM(CASE WHEN mode_group = 'MIXED_PT_PRIVATE' THEN commuter_count ELSE 0 END) AS mixed_commuters,
  SUM(commuter_count) AS total_commuters,
  SUM(CASE WHEN mode_group = 'MIXED_PT_PRIVATE' THEN commuter_count ELSE 0 END) / SUM(commuter_count) AS mixed_mode_rate
FROM V_TRANSPORT_CLEAN_NSW
GROUP BY census_year, sa2_res_code, sa2_res_name_std;

SELECT * FROM V_METRIC_MIXED_MODE_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_MIXED_MODE_NSW;


CREATE OR REPLACE VIEW V_METRIC_SELF_CONTAINMENT_NSW AS
SELECT
  census_year,
  sa2_res_code,
  sa2_res_name_std,
  SUM(CASE WHEN sa2_res_code = sa2_work_code THEN commuter_count ELSE 0 END) AS local_workers,
  SUM(commuter_count) AS total_commuters,
  SUM(CASE WHEN sa2_res_code = sa2_work_code THEN commuter_count ELSE 0 END) / SUM(commuter_count) AS self_containment_rate
FROM V_FLOW_ANALYSIS_NSW
GROUP BY census_year, sa2_res_code, sa2_res_name_std;

SELECT * FROM V_METRIC_SELF_CONTAINMENT_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_SELF_CONTAINMENT_NSW;


CREATE OR REPLACE VIEW V_DIM_CBD_SA2 AS
SELECT
  census_year,
  sa2_code,
  sa2_name
FROM V_DIM_SA2
WHERE sa3_name = 'Sydney Inner City';

SELECT * FROM V_DIM_CBD_SA2;
SELECT census_year, COUNT(*) AS row_cnt
FROM V_DIM_CBD_SA2
GROUP BY census_year
ORDER BY census_year;


CREATE OR REPLACE VIEW V_METRIC_CBD_DEPENDENCY_NSW AS
SELECT
  f.census_year,
  f.sa2_res_code,
  f.sa2_res_name_std,
  SUM(CASE WHEN c.sa2_code IS NOT NULL THEN f.commuter_count ELSE 0 END) AS cbd_commuters,
  SUM(f.commuter_count) AS total_commuters,
  SUM(CASE WHEN c.sa2_code IS NOT NULL THEN f.commuter_count ELSE 0 END) / SUM(f.commuter_count) AS cbd_dependency_rate
FROM V_FLOW_ANALYSIS_NSW f
LEFT JOIN V_DIM_CBD_SA2 c
  ON c.census_year = f.census_year
 AND c.sa2_code    = f.sa2_work_code
GROUP BY f.census_year, f.sa2_res_code, f.sa2_res_name_std;

SELECT * FROM V_METRIC_CBD_DEPENDENCY_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_CBD_DEPENDENCY_NSW;



/* =====================================================================
   11) NSW-WIDE MODE TRENDS (Year Level)
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_MODE_TREND_NSW AS
SELECT
  census_year,
  mode_group,
  SUM(commuter_count) AS commuters,
  SUM(commuter_count) / SUM(SUM(commuter_count)) OVER (PARTITION BY census_year) AS mode_share_nsw
FROM V_TRANSPORT_CLEAN_NSW
GROUP BY census_year, mode_group;

SELECT * FROM V_METRIC_MODE_TREND_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_MODE_TREND_NSW;



/* =====================================================================
   12) SA2-YEAR FEATURE TABLE
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_SA2_MODE_FEATURES_NSW AS
WITH pivot_shares AS (
  SELECT
    census_year,
    sa2_res_code,
    sa2_res_name_std,
    MAX(CASE WHEN mode_group = 'PUBLIC_TRANSPORT' THEN mode_share END) AS pt_share,
    MAX(CASE WHEN mode_group = 'PRIVATE_VEHICLE'  THEN mode_share END) AS private_share,
    MAX(CASE WHEN mode_group = 'WFH'              THEN mode_share END) AS wfh_share_from_pivot,
    MAX(CASE WHEN mode_group = 'ACTIVE_TRANSPORT' THEN mode_share END) AS active_share,
    MAX(CASE WHEN mode_group = 'MIXED_PT_PRIVATE' THEN mode_share END) AS mixed_share_from_pivot,
    MAX(CASE WHEN mode_group = 'OTHER'            THEN mode_share END) AS other_share
  FROM V_METRIC_MODE_SHARE_NSW
  GROUP BY census_year, sa2_res_code, sa2_res_name_std
)
SELECT
  p.census_year,
  p.sa2_res_code,
  p.sa2_res_name_std,
  p.pt_share,
  p.private_share,
  w.wfh_rate        AS wfh_share,
  m.mixed_mode_rate AS mixed_share,
  p.active_share,
  p.other_share,
  w.total_commuters AS total_commuters,
  w.wfh_commuters   AS wfh_commuters,
  m.mixed_commuters AS mixed_commuters
FROM pivot_shares p
LEFT JOIN V_METRIC_WFH_NSW w
  ON w.census_year  = p.census_year
 AND w.sa2_res_code = p.sa2_res_code
LEFT JOIN V_METRIC_MIXED_MODE_NSW m
  ON m.census_year  = p.census_year
 AND m.sa2_res_code = p.sa2_res_code;

SELECT * FROM V_METRIC_SA2_MODE_FEATURES_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_SA2_MODE_FEATURES_NSW;



/* =====================================================================
   13) CBD VOLUME + DEPENDENCY (Used for ranking/scoring)
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_CBD_VOLUME_NSW AS
SELECT
  f.census_year,
  f.sa2_res_code,
  f.sa2_res_name_std,
  SUM(CASE WHEN c.sa2_code IS NOT NULL THEN f.commuter_count ELSE 0 END) AS cbd_commuters,
  SUM(f.commuter_count) AS total_commuters_od,
  SUM(CASE WHEN c.sa2_code IS NOT NULL THEN f.commuter_count ELSE 0 END) / SUM(f.commuter_count) AS cbd_dependency_rate
FROM V_FLOW_ANALYSIS_NSW f
LEFT JOIN V_DIM_CBD_SA2 c
  ON c.census_year = f.census_year
 AND c.sa2_code    = f.sa2_work_code
GROUP BY f.census_year, f.sa2_res_code, f.sa2_res_name_std;

SELECT * FROM V_METRIC_CBD_VOLUME_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_CBD_VOLUME_NSW;



/* =====================================================================
   14) CBD GROWTH (2016 -> 2021) BY SA2 ORIGIN
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_CBD_GROWTH_16_21_NSW AS
SELECT
  sa2_res_code,
  sa2_res_name_std,
  MAX(CASE WHEN census_year = 2016 THEN cbd_commuters END) AS cbd_commuters_2016,
  MAX(CASE WHEN census_year = 2021 THEN cbd_commuters END) AS cbd_commuters_2021,
  MAX(CASE WHEN census_year = 2016 THEN cbd_dependency_rate END) AS cbd_rate_2016,
  MAX(CASE WHEN census_year = 2021 THEN cbd_dependency_rate END) AS cbd_rate_2021,
  (MAX(CASE WHEN census_year = 2021 THEN cbd_commuters END)
   - MAX(CASE WHEN census_year = 2016 THEN cbd_commuters END)) AS cbd_commuter_change_16_21,
  (MAX(CASE WHEN census_year = 2021 THEN cbd_dependency_rate END)
   - MAX(CASE WHEN census_year = 2016 THEN cbd_dependency_rate END)) AS cbd_rate_pp_change_16_21,
  CASE
    WHEN MAX(CASE WHEN census_year = 2016 THEN cbd_commuters END) > 0
    THEN (MAX(CASE WHEN census_year = 2021 THEN cbd_commuters END)
          - MAX(CASE WHEN census_year = 2016 THEN cbd_commuters END))
         / MAX(CASE WHEN census_year = 2016 THEN cbd_commuters END)
  END AS cbd_growth_rate_16_21
FROM V_METRIC_CBD_VOLUME_NSW
GROUP BY sa2_res_code, sa2_res_name_std;

SELECT * FROM V_METRIC_CBD_GROWTH_16_21_NSW;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_CBD_GROWTH_16_21_NSW;



/* =====================================================================
   15) BUS-TO-CBD CANDIDATE SHORTLIST (2021, NORMALISED SCORE)
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_BUS_CANDIDATE_2021_NSW_NORMALIZED AS
WITH base AS (
  SELECT
    v.census_year,
    v.sa2_res_code,
    v.sa2_res_name_std,
    v.cbd_commuters,
    v.cbd_dependency_rate,
    f.pt_share,
    f.wfh_share,
    f.mixed_share,
    f.total_commuters
  FROM V_METRIC_CBD_VOLUME_NSW v
  JOIN V_METRIC_SA2_MODE_FEATURES_NSW f
    ON f.census_year  = v.census_year
   AND f.sa2_res_code = v.sa2_res_code
  WHERE v.census_year = 2021
    AND v.cbd_commuters >= 50
    AND f.total_commuters >= 500
    AND f.pt_share IS NOT NULL
    AND f.wfh_share IS NOT NULL
    AND f.mixed_share IS NOT NULL
),
g AS (
  SELECT
    sa2_res_code,
    NVL(cbd_growth_rate_16_21, 0) AS cbd_growth_rate_16_21
  FROM V_METRIC_CBD_GROWTH_16_21_NSW
)
SELECT
  b.*,
  g.cbd_growth_rate_16_21,
  (
    0.40 * (b.cbd_commuters / NULLIF(MAX(b.cbd_commuters) OVER (), 0)) +
    0.20 * (g.cbd_growth_rate_16_21 / NULLIF(MAX(g.cbd_growth_rate_16_21) OVER (), 0)) +
    0.20 * (1 - b.pt_share) +
    0.10 * (1 - b.wfh_share) +
    0.10 * (b.mixed_share / NULLIF(MAX(b.mixed_share) OVER (), 0))
  ) AS bus_candidate_score
FROM base b
LEFT JOIN g
  ON g.sa2_res_code = b.sa2_res_code
ORDER BY bus_candidate_score DESC;

SELECT * FROM V_METRIC_BUS_CANDIDATE_2021_NSW_NORMALIZED;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_BUS_CANDIDATE_2021_NSW_NORMALIZED;



/* =====================================================================
   16) TOP CBD ORIGINS (2021)
   ===================================================================== */

CREATE OR REPLACE VIEW V_METRIC_TOP_CBD_ORIGINS_2021 AS
SELECT
  sa2_res_code,
  sa2_res_name_std,
  cbd_commuters,
  cbd_dependency_rate
FROM V_METRIC_CBD_VOLUME_NSW
WHERE census_year = 2021
ORDER BY cbd_commuters DESC;

SELECT * FROM V_METRIC_TOP_CBD_ORIGINS_2021;
SELECT COUNT(*) AS row_cnt FROM V_METRIC_TOP_CBD_ORIGINS_2021;