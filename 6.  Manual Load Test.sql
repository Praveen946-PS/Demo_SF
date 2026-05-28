-- ═══════════════════════════════════════════════════════════════════════════
-- MANUAL LOAD TEST — Bronze → Silver → Gold (HCP Domain)
-- ═══════════════════════════════════════════════════════════════════════════
-- Run each statement one at a time. Wait for each to finish before next.
--
-- ONE-TIME SETUP (Steps 1–7):
--   STEP 1  : Create Bronze table + INSERT 10 sample rows
--   STEP 2  : Create Silver table (empty shell)
--   STEP 3  : Create Gold table (empty shell)
--   STEP 4  : Create SP_BUILD_SILVER_HCP
--   STEP 5  : Create SP_BUILD_GOLD_HCP
--   STEP 6  : Update PIPELINE_MASTER + insert DQM rules
--   STEP 7  : Verify PIPELINE_MASTER + DQM_RULES
--
-- RUN EVERY TIME (Steps A–F):
--   STEP A  : Reset logs + insert Bronze PASSED entry
--   STEP B  : Run Silver pipeline
--   STEP C  : Run Gold pipeline
--   STEP D  : Verify row counts
--   STEP E  : Check pipeline run logs
--   STEP F  : Check DQM results


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 1 — BRONZE TABLE + SAMPLE DATA
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS (
  CALL_ID              VARCHAR(100)   NOT NULL,
  NPI                  VARCHAR(10)    NOT NULL,  /// what if not null jumps in -- bronze layer failed ??? 
  CALL_DATE            DATE           NOT NULL,
  PRODUCT_CODE         VARCHAR(20),
  TERRITORY_CODE       VARCHAR(50),
  REP_ID               VARCHAR(50),
  CALL_TYPE            VARCHAR(30),
  CALL_OUTCOME         VARCHAR(50),
  SAMPLE_UNITS         NUMBER(5,0),
  NOTES                VARCHAR(2000),
  _AUDIT_RUN_ID        VARCHAR(100),
  _AUDIT_BATCH_ID      VARCHAR(100),
  _AUDIT_LOAD_TS       TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  _AUDIT_SOURCE_SYSTEM VARCHAR(50)    DEFAULT 'VEEVA_CRM',
  _AUDIT_SOURCE_FILE   VARCHAR(500),
  _AUDIT_ROW_HASH      VARCHAR(64),
  _AUDIT_IS_DELETED    BOOLEAN        DEFAULT FALSE,
  _AUDIT_ENV           VARCHAR(20)    DEFAULT 'DEV'
)
DATA_RETENTION_TIME_IN_DAYS = 90
COMMENT = 'Bronze: raw Veeva CRM call activity.';

INSERT INTO NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS (
  CALL_ID, NPI, CALL_DATE, PRODUCT_CODE, TERRITORY_CODE,
  REP_ID, CALL_TYPE, CALL_OUTCOME, SAMPLE_UNITS, NOTES,
  _AUDIT_RUN_ID, _AUDIT_BATCH_ID, _AUDIT_SOURCE_SYSTEM,
  _AUDIT_SOURCE_FILE, _AUDIT_ROW_HASH, _AUDIT_ENV
)
SELECT * FROM (
  SELECT 'CALL-001','1234567890','2026-05-10'::DATE,'RELU_001','NE-001','REP-101',
         'FACE_TO_FACE','COMPLETED',2,'Dr Singh receptive to Relu data',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-10',
         SHA2('CALL-001|1234567890|2026-05-10|RELU_001|NE-001|REP-101',256),'DEV'
  UNION ALL
  SELECT 'CALL-002','2345678901','2026-05-10'::DATE,'RELU_001','SE-002','REP-102',
         'VIRTUAL','COMPLETED',1,'Good MOA discussion',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-10',
         SHA2('CALL-002|2345678901|2026-05-10|RELU_001|SE-002|REP-102',256),'DEV'
  UNION ALL
  SELECT 'CALL-003','3456789012','2026-05-09'::DATE,'ULIXA_001','MW-003','REP-103',
         'FACE_TO_FACE','NO_SEE',0,'Gatekeeper blocked — rebook',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-09',
         SHA2('CALL-003|3456789012|2026-05-09|ULIXA_001|MW-003|REP-103',256),'DEV'
  UNION ALL
  SELECT 'CALL-004','4567890123','2026-05-09'::DATE,'RELU_001','SW-004','REP-104',
         'PHONE','COMPLETED',0,'Follow-up post conference',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-09',
         SHA2('CALL-004|4567890123|2026-05-09|RELU_001|SW-004|REP-104',256),'DEV'
  UNION ALL
  SELECT 'CALL-005','5678901234','2026-05-08'::DATE,'ULIXA_001','NW-005','REP-105',
         'FACE_TO_FACE','COMPLETED',3,'High interest in Ulixa patient support',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-08',
         SHA2('CALL-005|5678901234|2026-05-08|ULIXA_001|NW-005|REP-105',256),'DEV'
  UNION ALL
  SELECT 'CALL-006','6789012345','2026-05-08'::DATE,'RELU_001','NE-001','REP-101',
         'VIRTUAL','LEFT_SAMPLE',1,'Left samples with nurse',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-08',
         SHA2('CALL-006|6789012345|2026-05-08|RELU_001|NE-001|REP-101',256),'DEV'
  UNION ALL
  SELECT 'CALL-007','7890123456','2026-05-07'::DATE,'RELU_001','SE-002','REP-102',
         'FACE_TO_FACE','COMPLETED',2,'Requested clinical papers',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-07',
         SHA2('CALL-007|7890123456|2026-05-07|RELU_001|SE-002|REP-102',256),'DEV'
  UNION ALL
  SELECT 'CALL-008','8901234567','2026-05-07'::DATE,'ULIXA_001','MW-003','REP-103',
         'PHONE','COMPLETED',0,'Confirmed writing script',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-07',
         SHA2('CALL-008|8901234567|2026-05-07|ULIXA_001|MW-003|REP-103',256),'DEV'
  UNION ALL
  SELECT 'CALL-009','9012345678','2026-05-06'::DATE,'RELU_001','SW-004','REP-104',
         'FACE_TO_FACE','COMPLETED',2,'Second call — reinforced dosing',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-06',
         SHA2('CALL-009|9012345678|2026-05-06|RELU_001|SW-004|REP-104',256),'DEV'
  UNION ALL
  SELECT 'CALL-010','1234567890','2026-05-06'::DATE,'ULIXA_001','NE-001','REP-101',
         'VIRTUAL','COMPLETED',1,'Same HCP — second product discussion',
         'TEST-RUN-001','TEST-BATCH-001','VEEVA_CRM','veeva_crm_api/calls/2026-05-06',
         SHA2('CALL-010|1234567890|2026-05-06|ULIXA_001|NE-001|REP-101',256),'DEV'
);

SELECT COUNT(*) AS bronze_rows FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 2 — SILVER TABLE (empty shell — SP fills it)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE NEW_TEST.SILVER.HCP_MASTER (
  UHCP_ID              VARCHAR(100)   NOT NULL,
  NPI                  VARCHAR(10)    NOT NULL,
  FIRST_NAME           VARCHAR(100),
  LAST_NAME            VARCHAR(100),
  FULL_NAME            VARCHAR(200),
  SPECIALTY            VARCHAR(100),
  TERRITORY_CODE       VARCHAR(50),
  REP_ID               VARCHAR(50),
  LAST_CALL_DATE       DATE,
  TOTAL_CALLS_YTD      NUMBER(10,0)   DEFAULT 0,
  _AUDIT_CREATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  _AUDIT_UPDATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  _AUDIT_CREATED_BY    VARCHAR(100)   DEFAULT 'SP_BUILD_SILVER_HCP',
  _AUDIT_RUN_ID        VARCHAR(100),
  _AUDIT_SOURCE_RUN_ID VARCHAR(100),
  _AUDIT_RECORD_HASH   VARCHAR(64),
  _AUDIT_IS_CURRENT    BOOLEAN        DEFAULT TRUE,
  _AUDIT_DQ_STATUS     VARCHAR(20)    DEFAULT 'PASSED',
  _AUDIT_ENV           VARCHAR(20)    DEFAULT 'DEV'
)
COMMENT = 'Silver: HCP master. Populated by SP_BUILD_SILVER_HCP.';


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 3 — GOLD TABLE (empty shell — SP fills it)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE TABLE NEW_TEST.GOLD.DIM_HCP (
  HCP_KEY              VARCHAR(64)    NOT NULL,
  UHCP_ID              VARCHAR(100)   NOT NULL,
  NPI                  VARCHAR(10)    NOT NULL,
  FIRST_NAME           VARCHAR(100),
  LAST_NAME            VARCHAR(100),
  FULL_NAME            VARCHAR(200),
  SPECIALTY            VARCHAR(100),
  TERRITORY_CODE       VARCHAR(50),
  REP_ID               VARCHAR(50),
  LAST_CALL_DATE       DATE,
  TOTAL_CALLS_YTD      NUMBER(10,0)   DEFAULT 0,
  IS_ACTIVE            BOOLEAN        DEFAULT TRUE,
  VALID_FROM           TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  VALID_TO             TIMESTAMP_NTZ,
  _AUDIT_CREATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  _AUDIT_UPDATED_AT    TIMESTAMP_NTZ  DEFAULT CURRENT_TIMESTAMP(),
  _AUDIT_CREATED_BY    VARCHAR(100)   DEFAULT 'SP_BUILD_GOLD_HCP',
  _AUDIT_RUN_ID        VARCHAR(100),
  _AUDIT_SOURCE_RUN_ID VARCHAR(100),
  _AUDIT_RECORD_HASH   VARCHAR(64),
  _AUDIT_IS_CURRENT    BOOLEAN        DEFAULT TRUE,
  _AUDIT_DQ_STATUS     VARCHAR(20)    DEFAULT 'PASSED',
  _AUDIT_ENV           VARCHAR(20)    DEFAULT 'DEV'
)
COMMENT = 'Gold: DIM_HCP dimension. Populated by SP_BUILD_GOLD_HCP.';


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 4 — SP_BUILD_SILVER_HCP (loads Bronze → Silver)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_inserted NUMBER := 0; v_config VARIANT; v_load_type VARCHAR;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_SILVER_HCP', :P_ENV) INTO :v_config;
  v_load_type := COALESCE(v_config:load_type::VARCHAR, 'FULL_REFRESH');

  IF (v_load_type = 'FULL_REFRESH') THEN
    TRUNCATE TABLE NEW_TEST.SILVER.HCP_MASTER;
    INSERT INTO NEW_TEST.SILVER.HCP_MASTER (
      UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
      _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
      _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
    )
    WITH latest_call AS (
      SELECT NPI, TERRITORY_CODE, REP_ID, CALL_DATE,
             _AUDIT_RUN_ID AS bronze_run_id,
             ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY CALL_DATE DESC) AS rn
      FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
      WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
        AND CALL_DATE <= CURRENT_DATE() AND CALL_DATE >= DATE_TRUNC('year', CURRENT_DATE())
    ),
    call_stats AS (
      SELECT NPI, MAX(CALL_DATE) AS last_call_date, COUNT(*) AS total_calls_ytd
      FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
      WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
        AND CALL_DATE >= DATE_TRUNC('year', CURRENT_DATE())
      GROUP BY NPI
    )
    SELECT SHA2(lc.NPI, 256), lc.NPI,
      'TEST_FIRST_' || lc.NPI, 'TEST_LAST_' || lc.NPI,
      UPPER('TEST_FIRST_' || lc.NPI || ' TEST_LAST_' || lc.NPI),
      'NEUROLOGY', lc.TERRITORY_CODE, lc.REP_ID,
      cs.last_call_date, COALESCE(cs.total_calls_ytd, 0),
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'SP_BUILD_SILVER_HCP',
      :P_RUN_ID, lc.bronze_run_id,
      SHA2(lc.NPI || '|' || COALESCE(lc.TERRITORY_CODE,'') || '|' || COALESCE(lc.REP_ID,''), 256),
      TRUE, 'PASSED', :P_ENV
    FROM latest_call lc LEFT JOIN call_stats cs ON lc.NPI = cs.NPI WHERE lc.rn = 1;
  END IF;

  SELECT COUNT(*) INTO v_rows_inserted FROM NEW_TEST.SILVER.HCP_MASTER;
  RETURN 'SP_BUILD_SILVER_HCP: SUCCESS | Load: ' || v_load_type || ' | Rows: ' || v_rows_inserted::VARCHAR;
EXCEPTION
  WHEN OTHER THEN RETURN 'SP_BUILD_SILVER_HCP: FAILED — ' || SQLERRM;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 5 — SP_BUILD_GOLD_HCP (loads Silver → Gold)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_inserted NUMBER := 0; v_silver_rows NUMBER := 0; v_config VARIANT; v_load_type VARCHAR;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_GOLD_HCP', :P_ENV) INTO :v_config;
  v_load_type := COALESCE(v_config:load_type::VARCHAR, 'FULL_REFRESH');

  SELECT COUNT(*) INTO v_silver_rows FROM NEW_TEST.SILVER.HCP_MASTER WHERE _AUDIT_DQ_STATUS = 'PASSED';
  IF (v_silver_rows = 0) THEN
    RETURN 'SP_BUILD_GOLD_HCP: SKIPPED — Silver HCP_MASTER is empty.';
  END IF;

  IF (v_load_type = 'FULL_REFRESH') THEN
    TRUNCATE TABLE NEW_TEST.GOLD.DIM_HCP;
    INSERT INTO NEW_TEST.GOLD.DIM_HCP (
      HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      IS_ACTIVE, VALID_FROM, VALID_TO,
      _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
      _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
      _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
    )
    SELECT SHA2(UHCP_ID, 256), UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      TRUE, CURRENT_TIMESTAMP(), NULL::TIMESTAMP_NTZ,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'SP_BUILD_GOLD_HCP',
      :P_RUN_ID, _AUDIT_RUN_ID,
      SHA2(UHCP_ID || '|' || NPI || '|' || COALESCE(TERRITORY_CODE,''), 256),
      TRUE, 'PASSED', :P_ENV
    FROM NEW_TEST.SILVER.HCP_MASTER WHERE _AUDIT_DQ_STATUS = 'PASSED';
  END IF;

  SELECT COUNT(*) INTO v_rows_inserted FROM NEW_TEST.GOLD.DIM_HCP;
  RETURN 'SP_BUILD_GOLD_HCP: SUCCESS | Load: ' || v_load_type || ' | Rows: ' || v_rows_inserted::VARCHAR;
EXCEPTION
  WHEN OTHER THEN RETURN 'SP_BUILD_GOLD_HCP: FAILED — ' || SQLERRM;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 6 — UPDATE PIPELINE_MASTER + INSERT DQM RULES
-- ═══════════════════════════════════════════════════════════════════════════

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET COMPONENT_NAME = 'NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP',
    COMPONENT_TYPE = 'STORED_PROCEDURE',
    NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'murali.v@pharmsight.com, chirag@pharmsight.com',
    EXPECTED_ROW_COUNT = 5,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = 'DEV';

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET COMPONENT_NAME = 'NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP',
    COMPONENT_TYPE = 'STORED_PROCEDURE',
    NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'murali.v@pharmsight.com, chirag@pharmsight.com',
    EXPECTED_ROW_COUNT = 5,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = 'DEV';

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'murali.v@pharmsight.com, chirag@pharmsight.com',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'VEEVA_CRM_INBOUND' AND ENVIRONMENT = 'DEV';

DELETE FROM NEW_TEST.REFERENCE.DQM_RULES WHERE PIPELINE_CODE IN ('SP_SILVER_HCP', 'SP_GOLD_HCP');

INSERT INTO NEW_TEST.REFERENCE.DQM_RULES (RULE_CODE, RULE_NAME, PIPELINE_CODE, PIPELINE_LAYER, DOMAIN, RULE_TYPE, COLUMN_NAME, SEVERITY, ACTION_ON_FAIL, IS_ACTIVE, RULE_EXPRESSION)
VALUES
  ('SLV_HCP_NPI_NN',        'NPI must not be null',            'SP_SILVER_HCP', 'SILVER', 'HCP', 'NOT_NULL',        'NPI',              'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('SLV_HCP_UHCPID_NN',     'UHCP_ID must not be null',        'SP_SILVER_HCP', 'SILVER', 'HCP', 'NOT_NULL',        'UHCP_ID',          'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('SLV_HCP_FULLNAME_NN',   'FULL_NAME must not be null',      'SP_SILVER_HCP', 'SILVER', 'HCP', 'NOT_NULL',        'FULL_NAME',        'HIGH',     'WARN_AND_CONTINUE', TRUE, NULL),
  ('SLV_HCP_NPI_UQ',        'NPI must be unique',              'SP_SILVER_HCP', 'SILVER', 'HCP', 'UNIQUE',          'NPI',              'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('SLV_HCP_UHCPID_UQ',     'UHCP_ID must be unique',          'SP_SILVER_HCP', 'SILVER', 'HCP', 'UNIQUE',          'UHCP_ID',          'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('SLV_HCP_NPI_REGEX',     'NPI must be 10 digits',           'SP_SILVER_HCP', 'SILVER', 'HCP', 'REGEX',           'NPI',              'HIGH',     'WARN_AND_CONTINUE', TRUE, '^[0-9]{10}$'),
  ('SLV_HCP_SPEC_AV',       'SPECIALTY must be valid',         'SP_SILVER_HCP', 'SILVER', 'HCP', 'ACCEPTED_VALUES', 'SPECIALTY',        'MEDIUM',   'WARN_AND_CONTINUE', TRUE, 'NEUROLOGY,CARDIOLOGY,ONCOLOGY,DERMATOLOGY,INTERNAL_MEDICINE,PSYCHIATRY'),
  ('SLV_HCP_CALLS_RANGE',   'TOTAL_CALLS_YTD must be 0-10000','SP_SILVER_HCP', 'SILVER', 'HCP', 'RANGE',           'TOTAL_CALLS_YTD',  'MEDIUM',   'WARN_AND_CONTINUE', TRUE, '0,10000'),
  ('SLV_HCP_CALLDATE_FRESH','Last call within 30 days',        'SP_SILVER_HCP', 'SILVER', 'HCP', 'FRESHNESS',       'LAST_CALL_DATE',   'HIGH',     'WARN_AND_CONTINUE', TRUE, '720'),
  ('GLD_HCP_NPI_NN',        'NPI must not be null',            'SP_GOLD_HCP',   'GOLD',   'HCP', 'NOT_NULL',        'NPI',              'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('GLD_HCP_HCPKEY_NN',     'HCP_KEY must not be null',        'SP_GOLD_HCP',   'GOLD',   'HCP', 'NOT_NULL',        'HCP_KEY',          'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('GLD_HCP_ISACTIVE_NN',   'IS_ACTIVE must not be null',      'SP_GOLD_HCP',   'GOLD',   'HCP', 'NOT_NULL',        'IS_ACTIVE',        'HIGH',     'WARN_AND_CONTINUE', TRUE, NULL),
  ('GLD_HCP_NPI_UQ',        'NPI must be unique',              'SP_GOLD_HCP',   'GOLD',   'HCP', 'UNIQUE',          'NPI',              'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('GLD_HCP_HCPKEY_UQ',     'HCP_KEY must be unique',          'SP_GOLD_HCP',   'GOLD',   'HCP', 'UNIQUE',          'HCP_KEY',          'CRITICAL', 'FAIL_PIPELINE',     TRUE, NULL),
  ('GLD_HCP_NPI_REF',       'NPI must exist in Silver',        'SP_GOLD_HCP',   'GOLD',   'HCP', 'REFERENTIAL',     'NPI',              'CRITICAL', 'FAIL_PIPELINE',     TRUE, 'NEW_TEST.SILVER.HCP_MASTER.NPI');


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP 7 — VERIFY CONFIG
-- ═══════════════════════════════════════════════════════════════════════════

SELECT PIPELINE_CODE, PIPELINE_LAYER, COMPONENT_NAME, IS_ACTIVE,
       NOTIFY_ON_SUCCESS, NOTIFY_ON_FAILURE, NOTIFICATION_EMAILS, EXPECTED_ROW_COUNT
FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
WHERE PIPELINE_CODE IN ('VEEVA_CRM_INBOUND', 'SP_SILVER_HCP', 'SP_GOLD_HCP')
AND ENVIRONMENT = 'DEV';

SELECT PIPELINE_CODE, RULE_CODE, RULE_TYPE, COLUMN_NAME, SEVERITY, RULE_EXPRESSION
FROM NEW_TEST.REFERENCE.DQM_RULES
WHERE PIPELINE_CODE IN ('SP_SILVER_HCP', 'SP_GOLD_HCP') AND IS_ACTIVE = TRUE
ORDER BY PIPELINE_CODE, RULE_TYPE;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP A — RESET + INSERT BRONZE PASSED LOG ENTRY
-- ═══════════════════════════════════════════════════════════════════════════

DELETE FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG WHERE RUN_DATE = CURRENT_DATE() AND ENVIRONMENT = 'DEV';
DELETE FROM NEW_TEST.AUDIT.DQM_CHECK_LOG WHERE ENVIRONMENT = 'DEV';
TRUNCATE TABLE NEW_TEST.SILVER.HCP_MASTER;
TRUNCATE TABLE NEW_TEST.GOLD.DIM_HCP;

INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
  RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
  SOURCE_CODE, TARGET_TABLE, RUN_STATUS, DQ_CHECK_STATUS,
  RUN_TRIGGER_TYPE, RUN_DATE, RUN_START_TIME, RUN_END_TIME,
  RUN_DURATION_SECS, ROWS_INSERTED, TRIGGERED_BY, NOTIFICATION_SENT
)
SELECT UUID_STRING(), 'MANUAL-' || TO_VARCHAR(CURRENT_DATE(),'YYYYMMDD'),
  'DEV', 'HCP', 'VEEVA_CRM_INBOUND', 'BRONZE',
  'VEEVA_CRM', 'NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS',
  'PASSED', 'PASSED', 'MANUAL', CURRENT_DATE(),
  CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 0, 10, 'MANUAL_TEST', FALSE;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP B — RUN SILVER
-- ═══════════════════════════════════════════════════════════════════════════

CALL NEW_TEST.REFERENCE.RUN_PIPELINE('SP_SILVER_HCP', 'DEV');


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP C — RUN GOLD
-- ═══════════════════════════════════════════════════════════════════════════

CALL NEW_TEST.REFERENCE.RUN_PIPELINE('SP_GOLD_HCP', 'DEV');


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP D — VERIFY ROW COUNTS
-- ═══════════════════════════════════════════════════════════════════════════

SELECT 'BRONZE' AS layer, COUNT(*) AS row_count FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
UNION ALL SELECT 'SILVER', COUNT(*) FROM NEW_TEST.SILVER.HCP_MASTER
UNION ALL SELECT 'GOLD', COUNT(*) FROM NEW_TEST.GOLD.DIM_HCP;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP E — CHECK PIPELINE RUN LOGS
-- ═══════════════════════════════════════════════════════════════════════════

SELECT PIPELINE_CODE, PIPELINE_LAYER, RUN_STATUS, DQ_CHECK_STATUS,
       ROWS_INSERTED, NOTIFICATION_SENT, ERROR_MESSAGE
FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
WHERE RUN_DATE = CURRENT_DATE() AND ENVIRONMENT = 'DEV'
ORDER BY RUN_START_TIME;


-- ═══════════════════════════════════════════════════════════════════════════
-- STEP F — CHECK DQM RESULTS
-- ═══════════════════════════════════════════════════════════════════════════

SELECT PIPELINE_CODE, RULE_CODE, RULE_TYPE, COLUMN_NAME,
       CHECK_STATUS, ROWS_CHECKED, ROWS_FAILED, SEVERITY
FROM NEW_TEST.AUDIT.DQM_CHECK_LOG
WHERE RUN_ID IN (
  SELECT RUN_ID FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE RUN_DATE = CURRENT_DATE() AND ENVIRONMENT = 'DEV'
)
ORDER BY PIPELINE_CODE, RULE_TYPE;




=================================
-- adding new column in bronze table 
ALTER TABLE NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS 
ADD COLUMN _AUDIT_CREATED_AT TIMESTAMP_NTZ;

ALTER TABLE NEW_TEST.BRONZE.STG_VEEVA_CRM_CALLS 
ADD COLUMN _AUDIT_CREATED_AT TIMESTAMP_NTZ;

UPDATE NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
SET _AUDIT_CREATED_AT = _AUDIT_LOAD_TS
WHERE _AUDIT_CREATED_AT IS NULL;

SELECT CALL_ID, _AUDIT_LOAD_TS::VARCHAR, _AUDIT_CREATED_AT::VARCHAR
FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS;

-- Stage state right now
SELECT CALL_ID, NPI, COUNT(*) 
FROM NEW_TEST.BRONZE.STG_VEEVA_CRM_CALLS
GROUP BY CALL_ID, NPI;