-- ═══════════════════════════════════════════════════════════════════════════════
-- COMPLETE CHANGES: ADD PK_COLUMN TO PIPELINE_MASTER + UPDATED SPs
-- ═══════════════════════════════════════════════════════════════════════════════
-- CHANGES SUMMARY:
--   1. ADD PK_COLUMN to PIPELINE_MASTER (keep WATERMARK_TYPE for Bronze)
--   2. UPDATE GET_PIPELINE_CONFIG to return pk_column
--   3. UPDATE SP_BUILD_SILVER_HCP — dynamic PK from config with validation
--   4. UPDATE SP_BUILD_GOLD_HCP — dynamic PK from config with validation
--   5. POPULATE PK_COLUMN values for all Silver/Gold pipelines
-- ═══════════════════════════════════════════════════════════════════════════════

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 1: ADD PK_COLUMN TO PIPELINE_MASTER
-- (Keep WATERMARK_TYPE — Bronze still uses it for TIMESTAMP/DATE types)
-- ─────────────────────────────────────────────────────────────────────────────

ALTER TABLE NEW_TEST.REFERENCE.PIPELINE_MASTER
ADD COLUMN IF NOT EXISTS PK_COLUMN VARCHAR(200)
COMMENT 'Primary key column(s) for MERGE. Comma-separated for composite keys. E.g. NPI or CLAIM_ID,LINE_NUMBER';

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 2: POPULATE PK_COLUMN FOR ALL SILVER/GOLD PIPELINES
-- ─────────────────────────────────────────────────────────────────────────────

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'NPI' WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'NPI' WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'CLAIM_ID,LINE_NUMBER' WHERE PIPELINE_CODE = 'SP_SILVER_CLAIMS' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'PATIENT_ID' WHERE PIPELINE_CODE = 'SP_SILVER_PATIENT' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'EVENT_ID' WHERE PIPELINE_CODE = 'SP_SILVER_COMMERCIAL' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'CALL_ID' WHERE PIPELINE_CODE = 'SP_GOLD_CALLS' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'NPI' WHERE PIPELINE_CODE = 'SP_GOLD_RX' AND ENVIRONMENT = 'DEV';
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER SET PK_COLUMN = 'REP_ID,TERRITORY_CODE' WHERE PIPELINE_CODE = 'SP_GOLD_IC' AND ENVIRONMENT = 'DEV';

-- ─────────────────────────────────────────────────────────────────────────────
-- STEP 3: UPDATE GET_PIPELINE_CONFIG SP (add pk_column to output)
-- ─────────────────────────────────────────────────────────────────────────────

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(
  P_PIPELINE_CODE VARCHAR, P_ENVIRONMENT VARCHAR
) RETURNS VARIANT LANGUAGE SQL EXECUTE AS OWNER AS
$$
DECLARE
  v_config VARIANT;
BEGIN
  SELECT OBJECT_CONSTRUCT(
    'pipeline_id',             PIPELINE_ID,
    'pipeline_code',           PIPELINE_CODE,
    'pipeline_name',           PIPELINE_NAME,
    'domain',                  DOMAIN,
    'pipeline_layer',          PIPELINE_LAYER,
    'pipeline_type',           PIPELINE_TYPE,
    'source_code',             SOURCE_CODE,
    'target_table',            TARGET_TABLE,
    'is_active',               IS_ACTIVE,
    'inactive_reason',         INACTIVE_REASON,
    'schedule_type',           SCHEDULE_TYPE,
    'schedule_cron',           SCHEDULE_CRON,
    'pk_column',               PK_COLUMN,
    'watermark_column',        WATERMARK_COLUMN,
    'watermark_type',          WATERMARK_TYPE,
    'watermark_current',       WATERMARK_CURRENT,
    'watermark_lookback_hrs',  WATERMARK_LOOKBACK_HRS,
    'load_type',               LOAD_TYPE,
    'batch_size',              BATCH_SIZE,
    'max_rows_per_run',        MAX_ROWS_PER_RUN,
    'expected_row_count',      EXPECTED_ROW_COUNT,
    'row_count_tolerance_pct', ROW_COUNT_TOLERANCE_PCT,
    'max_retry_count',         MAX_RETRY_COUNT,
    'retry_backoff_secs',      RETRY_BACKOFF_SECS,
    'notify_on_success',       NOTIFY_ON_SUCCESS,
    'notify_on_failure',       NOTIFY_ON_FAILURE,
    'notify_on_warning',       NOTIFY_ON_WARNING,
    'notification_emails',     NOTIFICATION_EMAILS,
    'slack_channel',           SLACK_CHANNEL,
    'alert_priority',          ALERT_PRIORITY,
    'depends_on_pipelines',    DEPENDS_ON_PIPELINES,
    'dependency_timeout_mins', DEPENDENCY_TIMEOUT_MINS,
    'component_type',          COMPONENT_TYPE,
    'component_name',          COMPONENT_NAME,
    'task_name',               TASK_NAME,
    'owner',                   OWNER
  ) INTO v_config
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;

  RETURN v_config;
END;
$$;


-- STEP 4: SP_BUILD_SILVER_HCP (Dynamic PK from PIPELINE_MASTER)


CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_inserted     NUMBER := 0;
  v_rows_updated      NUMBER := 0;
  v_rows_soft_deleted NUMBER := 0;
  v_config            VARIANT;
  v_load_type         VARCHAR;
  v_domain            VARCHAR;
  v_source_code       VARCHAR;
  v_target_table      VARCHAR;
  v_pk_column         VARCHAR;
  v_watermark_start   TIMESTAMP_NTZ;
  v_watermark_end     TIMESTAMP_NTZ;
  v_dq_status         VARCHAR := 'PASSED';
  v_run_status        VARCHAR;
  v_null_count        NUMBER := 0;
  v_duplicate_count   NUMBER := 0;
  v_query_id          VARCHAR;
  v_run_id            VARCHAR := UUID_STRING();
  v_start_time        TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_last_load_ts      TIMESTAMP_NTZ := '1900-01-01'::TIMESTAMP_NTZ;
  v_expected_rows     NUMBER;
  v_tolerance_pct     NUMBER;
  v_existing_run      NUMBER := 0;
  v_bronze_null_count NUMBER := 0;
  v_bronze_total      NUMBER := 0;
  v_pk_valid          NUMBER := 0;
  v_merge_on_clause   VARCHAR;
  v_merge_sql         VARCHAR;
BEGIN
  -- ═══ STEP 1: LOAD CONFIG ═══
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_SILVER_HCP', :P_ENV) INTO :v_config;
  v_domain          := COALESCE(v_config:domain::VARCHAR,              'HCP');
  v_source_code     := COALESCE(v_config:source_code::VARCHAR,         'VEEVA_CRM');
  v_target_table    := COALESCE(v_config:target_table::VARCHAR,        'NEW_TEST.SILVER.HCP_MASTER');
  v_pk_column       := COALESCE(v_config:pk_column::VARCHAR,           'NPI');
  v_expected_rows   := v_config:expected_row_count::NUMBER;
  v_tolerance_pct   := COALESCE(v_config:row_count_tolerance_pct::NUMBER, 20);
  v_watermark_start := v_config:watermark_current::TIMESTAMP_NTZ;

  -- ═══ STEP 2: VALIDATE PK COLUMN EXISTS IN TARGET TABLE ═══
  SELECT COUNT(*) INTO v_pk_valid
  FROM NEW_TEST.INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'SILVER' AND TABLE_NAME = 'HCP_MASTER'
    AND COLUMN_NAME = UPPER(TRIM(SPLIT_PART(:v_pk_column, ',', 1)));
  IF (v_pk_valid = 0) THEN
    RETURN 'SP_BUILD_SILVER_HCP: FAILED — PK_COLUMN "' || v_pk_column || '" not found in HCP_MASTER. Check PIPELINE_MASTER config.';
  END IF;

  -- ═══ STEP 3: BUILD DYNAMIC MERGE ON CLAUSE ═══
  -- Supports composite PKs: 'NPI' → 'tgt.NPI = src.NPI'
  -- Supports: 'CLAIM_ID,LINE_NUMBER' → 'tgt.CLAIM_ID = src.CLAIM_ID AND tgt.LINE_NUMBER = src.LINE_NUMBER'
  v_merge_on_clause := 'tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 1));
  IF (SPLIT_PART(v_pk_column, ',', 2) != '') THEN
    v_merge_on_clause := v_merge_on_clause || ' AND tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 2)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 2));
  END IF;
  IF (SPLIT_PART(v_pk_column, ',', 3) != '') THEN
    v_merge_on_clause := v_merge_on_clause || ' AND tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 3)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 3));
  END IF;

  -- ═══ STEP 4: IDEMPOTENCY CHECK ═══
  SELECT COUNT(*) INTO v_existing_run
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE BATCH_ID = :P_RUN_ID
    AND PIPELINE_CODE = 'SP_SILVER_HCP'
    AND ENVIRONMENT = :P_ENV
    AND RUN_STATUS IN ('PASSED', 'RUNNING');
  IF (v_existing_run > 0) THEN
    RETURN 'SP_BUILD_SILVER_HCP: SKIPPED — Already run for batch ' || :P_RUN_ID;
  END IF;

  -- ═══ STEP 5: PRE-LOAD DQ GATE (dynamic PK null check) ═══
  LET rs1 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS cnt FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS WHERE _AUDIT_IS_DELETED = FALSE AND ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' IS NOT NULL AND CALL_DATE >= DATE_TRUNC(''year'', CURRENT_DATE())');
  LET cur1 CURSOR FOR rs1;
  OPEN cur1;
  FETCH cur1 INTO v_bronze_total;
  CLOSE cur1;

  LET rs2 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS cnt FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS WHERE _AUDIT_IS_DELETED = FALSE AND ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' IS NULL AND CALL_DATE >= DATE_TRUNC(''year'', CURRENT_DATE())');
  LET cur2 CURSOR FOR rs2;
  OPEN cur2;
  FETCH cur2 INTO v_bronze_null_count;
  CLOSE cur2;

  IF (v_bronze_total = 0) THEN
    RETURN 'SP_BUILD_SILVER_HCP: SKIPPED — No valid Bronze rows for current year.';
  END IF;

  IF (v_bronze_null_count > v_bronze_total * 0.10) THEN
    INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
      RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
      SOURCE_CODE, TARGET_TABLE, RUN_STATUS, DQ_CHECK_STATUS,
      RUN_START_TIME, RUN_END_TIME, ERROR_MESSAGE, TRIGGERED_BY, CREATED_AT
    ) VALUES (
      :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_SILVER_HCP', 'SILVER',
      :v_source_code, :v_target_table, 'FAILED', 'FAILED',
      :v_start_time, CURRENT_TIMESTAMP(),
      'PRE-LOAD DQ FAILED: >10% Bronze ' || v_pk_column || ' nulls (' || v_bronze_null_count || '/' || (v_bronze_total + v_bronze_null_count) || ')',
      'SP_BUILD_SILVER_HCP', CURRENT_TIMESTAMP()
    );
    RETURN 'SP_BUILD_SILVER_HCP: FAILED — Pre-load DQ: >10% null ' || v_pk_column || ' in Bronze.';
  END IF;

  -- ═══ STEP 6: DETERMINE LOAD TYPE ═══
  LET v_silver_count NUMBER := (SELECT COUNT(*) FROM NEW_TEST.SILVER.HCP_MASTER);
  IF (v_silver_count = 0) THEN
    v_load_type := 'FULL_REFRESH';
  ELSE
    v_load_type := 'INCREMENTAL';
  END IF;

  -- ═══ STEP 7: LOG RUN START ═══
  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
    SOURCE_CODE, TARGET_TABLE, RUN_STATUS, RUN_START_TIME, TRIGGERED_BY, CREATED_AT
  ) VALUES (
    :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_SILVER_HCP', 'SILVER',
    :v_source_code, :v_target_table, 'RUNNING', :v_start_time,
    'SP_BUILD_SILVER_HCP', CURRENT_TIMESTAMP()
  );

  -- ═══ STEP 8: EXECUTE LOAD ═══
  IF (v_load_type = 'FULL_REFRESH') THEN
    CREATE OR REPLACE TEMPORARY TABLE NEW_TEST.SILVER.HCP_MASTER_SWAP LIKE NEW_TEST.SILVER.HCP_MASTER;

    INSERT INTO NEW_TEST.SILVER.HCP_MASTER_SWAP (
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
        AND CALL_DATE >= DATE_TRUNC('year', CURRENT_DATE())
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

    v_rows_inserted := SQLROWCOUNT;
    v_query_id      := (SELECT LAST_QUERY_ID());

    IF (v_rows_inserted > 0) THEN
      TRUNCATE TABLE NEW_TEST.SILVER.HCP_MASTER;
      INSERT INTO NEW_TEST.SILVER.HCP_MASTER SELECT * FROM NEW_TEST.SILVER.HCP_MASTER_SWAP;
    END IF;
    DROP TABLE IF EXISTS NEW_TEST.SILVER.HCP_MASTER_SWAP;

  ELSEIF (v_load_type = 'INCREMENTAL') THEN
    SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), '1900-01-01'::TIMESTAMP_NTZ)
    INTO v_last_load_ts
    FROM NEW_TEST.SILVER.HCP_MASTER;

    -- DYNAMIC MERGE using PK_COLUMN from PIPELINE_MASTER
    v_merge_sql := '
      MERGE INTO NEW_TEST.SILVER.HCP_MASTER AS tgt
      USING (
        WITH latest_call AS (
          SELECT NPI, TERRITORY_CODE, REP_ID, CALL_DATE,
                 _AUDIT_RUN_ID AS bronze_run_id,
                 ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY CALL_DATE DESC) AS rn
          FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
          WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
            AND CALL_DATE >= DATE_TRUNC(''year'', CURRENT_DATE())
            AND _AUDIT_LOAD_TS > ''' || v_last_load_ts::VARCHAR || '''::TIMESTAMP_NTZ
        ),
        call_stats AS (
          SELECT NPI, MAX(CALL_DATE) AS last_call_date, COUNT(*) AS total_calls_ytd
          FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
          WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
            AND CALL_DATE >= DATE_TRUNC(''year'', CURRENT_DATE())
          GROUP BY NPI
        )
        SELECT SHA2(lc.NPI, 256) AS UHCP_ID, lc.NPI,
          ''TEST_FIRST_'' || lc.NPI AS FIRST_NAME,
          ''TEST_LAST_'' || lc.NPI AS LAST_NAME,
          UPPER(''TEST_FIRST_'' || lc.NPI || '' TEST_LAST_'' || lc.NPI) AS FULL_NAME,
          ''NEUROLOGY'' AS SPECIALTY, lc.TERRITORY_CODE, lc.REP_ID,
          cs.last_call_date, COALESCE(cs.total_calls_ytd, 0) AS total_calls_ytd,
          lc.bronze_run_id,
          SHA2(lc.NPI || ''|'' || COALESCE(lc.TERRITORY_CODE,'''') || ''|'' || COALESCE(lc.REP_ID,''''), 256) AS record_hash
        FROM latest_call lc LEFT JOIN call_stats cs ON lc.NPI = cs.NPI WHERE lc.rn = 1
      ) AS src
      ON ' || v_merge_on_clause || '
      WHEN MATCHED AND tgt._AUDIT_RECORD_HASH != src.record_hash THEN UPDATE SET
        tgt.TERRITORY_CODE       = src.TERRITORY_CODE,
        tgt.REP_ID               = src.REP_ID,
        tgt.LAST_CALL_DATE       = src.last_call_date,
        tgt.TOTAL_CALLS_YTD      = src.total_calls_ytd,
        tgt._AUDIT_UPDATED_AT    = CURRENT_TIMESTAMP(),
        tgt._AUDIT_RUN_ID        = ''' || :P_RUN_ID || ''',
        tgt._AUDIT_SOURCE_RUN_ID = src.bronze_run_id,
        tgt._AUDIT_RECORD_HASH   = src.record_hash,
        tgt._AUDIT_IS_CURRENT    = TRUE,
        tgt._AUDIT_ENV           = ''' || :P_ENV || '''
      WHEN NOT MATCHED THEN INSERT (
        UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
        TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
        _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
        _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
        _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
      ) VALUES (
        src.UHCP_ID, src.NPI, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME, src.SPECIALTY,
        src.TERRITORY_CODE, src.REP_ID, src.last_call_date, src.total_calls_ytd,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''SP_BUILD_SILVER_HCP'',
        ''' || :P_RUN_ID || ''', src.bronze_run_id, src.record_hash,
        TRUE, ''PASSED'', ''' || :P_ENV || '''
      )';
    EXECUTE IMMEDIATE v_merge_sql;
    v_rows_inserted := SQLROWCOUNT;
    v_query_id      := (SELECT LAST_QUERY_ID());

    -- SOFT-DELETE: mark Silver rows where Bronze source is now deleted (dynamic PK)
    EXECUTE IMMEDIATE '
      UPDATE NEW_TEST.SILVER.HCP_MASTER
      SET _AUDIT_IS_CURRENT  = FALSE,
          _AUDIT_DQ_STATUS   = ''SOFT_DELETED'',
          _AUDIT_UPDATED_AT  = CURRENT_TIMESTAMP(),
          _AUDIT_RUN_ID      = ''' || :P_RUN_ID || '''
      WHERE _AUDIT_IS_CURRENT = TRUE
        AND ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' NOT IN (
          SELECT DISTINCT ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || '
          FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
          WHERE _AUDIT_IS_DELETED = FALSE AND ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' IS NOT NULL
        )';
    v_rows_soft_deleted := SQLROWCOUNT;
  END IF;

  -- ═══ STEP 9: POST-LOAD DQ CHECKS (dynamic PK) ═══
  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end FROM NEW_TEST.SILVER.HCP_MASTER;
  LET rs3 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS cnt FROM NEW_TEST.SILVER.HCP_MASTER WHERE ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' IS NULL AND _AUDIT_IS_CURRENT = TRUE');
  LET cur3 CURSOR FOR rs3;
  OPEN cur3;
  FETCH cur3 INTO v_null_count;
  CLOSE cur3;
  LET rs4 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) - COUNT(DISTINCT ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ') AS cnt FROM NEW_TEST.SILVER.HCP_MASTER WHERE _AUDIT_IS_CURRENT = TRUE');
  LET cur4 CURSOR FOR rs4;
  OPEN cur4;
  FETCH cur4 INTO v_duplicate_count;
  CLOSE cur4;

  v_dq_status := CASE
    WHEN v_null_count > 0 OR v_duplicate_count > 0 THEN 'WARNING'
    ELSE 'PASSED'
  END;

  -- ═══ STEP 10: ROW COUNT VALIDATION ═══
  IF (v_expected_rows IS NOT NULL AND v_rows_inserted > 0) THEN
    IF (ABS(v_rows_inserted - v_expected_rows) > (v_expected_rows * v_tolerance_pct / 100)) THEN
      v_dq_status := 'WARNING';
    END IF;
  END IF;

  v_run_status := CASE
    WHEN v_rows_inserted = 0 AND v_load_type = 'INCREMENTAL' AND v_rows_soft_deleted = 0 THEN 'SKIPPED'
    WHEN v_dq_status = 'FAILED' THEN 'FAILED'
    ELSE 'PASSED'
  END;

  -- ═══ STEP 11: UPDATE RUN LOG ═══
  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET RUN_STATUS         = :v_run_status,
      DQ_CHECK_STATUS    = :v_dq_status,
      ROWS_INSERTED      = :v_rows_inserted,
      ROWS_UPDATED       = :v_rows_soft_deleted,
      ROWS_EXPECTED      = :v_expected_rows,
      WATERMARK_START    = :v_watermark_start,
      WATERMARK_END      = :v_watermark_end,
      SNOWFLAKE_QUERY_ID = :v_query_id,
      RUN_END_TIME       = CURRENT_TIMESTAMP(),
      RUN_DURATION_SECS  = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
  WHERE RUN_ID = :v_run_id;

  -- ═══ STEP 12: PERSIST WATERMARK TO PIPELINE_MASTER ═══
  IF (v_run_status = 'PASSED' AND v_watermark_end IS NOT NULL) THEN
    UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
    SET WATERMARK_CURRENT = :v_watermark_end,
        UPDATED_AT        = CURRENT_TIMESTAMP()
    WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = :P_ENV;
  END IF;

  RETURN 'SP_BUILD_SILVER_HCP: ' || v_run_status
    || ' | Load: ' || v_load_type
    || ' | PK: ' || v_pk_column
    || ' | DQ: ' || v_dq_status
    || ' | Inserted: ' || v_rows_inserted::VARCHAR
    || ' | SoftDeleted: ' || v_rows_soft_deleted::VARCHAR
    || ' | Duration: ' || DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || 's';

EXCEPTION
  WHEN OTHER THEN
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET RUN_STATUS      = 'FAILED',
        ERROR_MESSAGE   = SQLERRM,
        RUN_END_TIME    = CURRENT_TIMESTAMP(),
        RUN_DURATION_SECS = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
    WHERE RUN_ID = :v_run_id;
    RETURN 'SP_BUILD_SILVER_HCP: FAILED — ' || SQLERRM;
END;
$$;



-- STEP 5: SP_BUILD_GOLD_HCP (Dynamic PK from PIPELINE_MASTER)


CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_inserted     NUMBER := 0;
  v_rows_updated      NUMBER := 0;
  v_rows_soft_deleted NUMBER := 0;
  v_silver_rows       NUMBER := 0;
  v_config            VARIANT;
  v_load_type         VARCHAR;
  v_domain            VARCHAR;
  v_source_code       VARCHAR;
  v_target_table      VARCHAR;
  v_pk_column         VARCHAR;
  v_watermark_start   TIMESTAMP_NTZ;
  v_watermark_end     TIMESTAMP_NTZ;
  v_dq_status         VARCHAR := 'PASSED';
  v_run_status        VARCHAR;
  v_null_count        NUMBER := 0;
  v_duplicate_count   NUMBER := 0;
  v_query_id          VARCHAR;
  v_run_id            VARCHAR := UUID_STRING();
  v_start_time        TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_expected_rows     NUMBER;
  v_tolerance_pct     NUMBER;
  v_existing_run      NUMBER := 0;
  v_last_load_ts      TIMESTAMP_NTZ := '1900-01-01'::TIMESTAMP_NTZ;
  v_pk_valid          NUMBER := 0;
  v_merge_on_clause   VARCHAR;
  v_merge_sql         VARCHAR;
BEGIN
  -- ═══ STEP 1: LOAD CONFIG ═══
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_GOLD_HCP', :P_ENV) INTO :v_config;
  v_domain          := COALESCE(v_config:domain::VARCHAR,              'HCP');
  v_source_code     := COALESCE(v_config:source_code::VARCHAR,         'SILVER');
  v_target_table    := COALESCE(v_config:target_table::VARCHAR,        'NEW_TEST.GOLD.DIM_HCP');
  v_pk_column       := COALESCE(v_config:pk_column::VARCHAR,           'NPI');
  v_expected_rows   := v_config:expected_row_count::NUMBER;
  v_tolerance_pct   := COALESCE(v_config:row_count_tolerance_pct::NUMBER, 20);
  v_watermark_start := v_config:watermark_current::TIMESTAMP_NTZ;

  -- ═══ STEP 2: VALIDATE PK COLUMN EXISTS IN TARGET TABLE ═══
  SELECT COUNT(*) INTO v_pk_valid
  FROM NEW_TEST.INFORMATION_SCHEMA.COLUMNS
  WHERE TABLE_SCHEMA = 'GOLD' AND TABLE_NAME = 'DIM_HCP'
    AND COLUMN_NAME = UPPER(TRIM(SPLIT_PART(:v_pk_column, ',', 1)));
  IF (v_pk_valid = 0) THEN
    RETURN 'SP_BUILD_GOLD_HCP: FAILED — PK_COLUMN "' || v_pk_column || '" not found in DIM_HCP. Check PIPELINE_MASTER config.';
  END IF;

  -- ═══ STEP 3: BUILD DYNAMIC MERGE ON CLAUSE ═══
  v_merge_on_clause := 'tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 1));
  IF (SPLIT_PART(v_pk_column, ',', 2) != '') THEN
    v_merge_on_clause := v_merge_on_clause || ' AND tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 2)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 2));
  END IF;
  IF (SPLIT_PART(v_pk_column, ',', 3) != '') THEN
    v_merge_on_clause := v_merge_on_clause || ' AND tgt.' || TRIM(SPLIT_PART(v_pk_column, ',', 3)) || ' = src.' || TRIM(SPLIT_PART(v_pk_column, ',', 3));
  END IF;
  v_merge_on_clause := v_merge_on_clause || ' AND tgt._AUDIT_IS_CURRENT = TRUE';

  -- ═══ STEP 4: IDEMPOTENCY CHECK ═══
  SELECT COUNT(*) INTO v_existing_run
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE BATCH_ID = :P_RUN_ID
    AND PIPELINE_CODE = 'SP_GOLD_HCP'
    AND ENVIRONMENT = :P_ENV
    AND RUN_STATUS IN ('PASSED', 'RUNNING');
  IF (v_existing_run > 0) THEN
    RETURN 'SP_BUILD_GOLD_HCP: SKIPPED — Already run for batch ' || :P_RUN_ID;
  END IF;

  -- ═══ STEP 5: PRE-LOAD DQ GATE ═══
  SELECT COUNT(*) INTO v_silver_rows
  FROM NEW_TEST.SILVER.HCP_MASTER
  WHERE _AUDIT_DQ_STATUS = 'PASSED' AND _AUDIT_IS_CURRENT = TRUE;

  IF (v_silver_rows = 0) THEN
    INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
      RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
      SOURCE_CODE, TARGET_TABLE, RUN_STATUS, DQ_CHECK_STATUS,
      RUN_START_TIME, RUN_END_TIME, ERROR_MESSAGE, TRIGGERED_BY, CREATED_AT
    ) VALUES (
      :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_GOLD_HCP', 'GOLD',
      :v_source_code, :v_target_table, 'SKIPPED', 'PASSED',
      :v_start_time, CURRENT_TIMESTAMP(),
      'Silver HCP_MASTER has no active PASSED rows.',
      'SP_BUILD_GOLD_HCP', CURRENT_TIMESTAMP()
    );
    RETURN 'SP_BUILD_GOLD_HCP: SKIPPED — Silver HCP_MASTER is empty or all inactive.';
  END IF;

  -- ═══ STEP 6: DETERMINE LOAD TYPE ═══
  LET v_gold_count NUMBER := (SELECT COUNT(*) FROM NEW_TEST.GOLD.DIM_HCP);
  IF (v_gold_count = 0) THEN
    v_load_type := 'FULL_REFRESH';
  ELSE
    v_load_type := 'INCREMENTAL';
  END IF;

  -- ═══ STEP 7: LOG RUN START ═══
  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
    SOURCE_CODE, TARGET_TABLE, RUN_STATUS, RUN_START_TIME, TRIGGERED_BY, CREATED_AT
  ) VALUES (
    :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_GOLD_HCP', 'GOLD',
    :v_source_code, :v_target_table, 'RUNNING', :v_start_time,
    'SP_BUILD_GOLD_HCP', CURRENT_TIMESTAMP()
  );

  -- ═══ STEP 8: EXECUTE LOAD ═══
  IF (v_load_type = 'FULL_REFRESH') THEN
    CREATE OR REPLACE TEMPORARY TABLE NEW_TEST.GOLD.DIM_HCP_SWAP LIKE NEW_TEST.GOLD.DIM_HCP;

    INSERT INTO NEW_TEST.GOLD.DIM_HCP_SWAP (
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
    FROM NEW_TEST.SILVER.HCP_MASTER
    WHERE _AUDIT_DQ_STATUS = 'PASSED' AND _AUDIT_IS_CURRENT = TRUE;

    v_rows_inserted := SQLROWCOUNT;
    v_query_id      := (SELECT LAST_QUERY_ID());

    IF (v_rows_inserted > 0) THEN
      TRUNCATE TABLE NEW_TEST.GOLD.DIM_HCP;
      INSERT INTO NEW_TEST.GOLD.DIM_HCP SELECT * FROM NEW_TEST.GOLD.DIM_HCP_SWAP;
    END IF;
    DROP TABLE IF EXISTS NEW_TEST.GOLD.DIM_HCP_SWAP;

  ELSEIF (v_load_type = 'INCREMENTAL') THEN
    SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), '1900-01-01'::TIMESTAMP_NTZ)
    INTO v_last_load_ts
    FROM NEW_TEST.GOLD.DIM_HCP;

    -- DYNAMIC MERGE using PK_COLUMN from PIPELINE_MASTER
    v_merge_sql := '
      MERGE INTO NEW_TEST.GOLD.DIM_HCP AS tgt
      USING (
        SELECT SHA2(UHCP_ID, 256) AS HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
          TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
          _AUDIT_RUN_ID AS source_run_id,
          SHA2(UHCP_ID || ''|'' || NPI || ''|'' || COALESCE(TERRITORY_CODE,''''), 256) AS record_hash
        FROM NEW_TEST.SILVER.HCP_MASTER
        WHERE _AUDIT_DQ_STATUS = ''PASSED'' AND _AUDIT_IS_CURRENT = TRUE
          AND _AUDIT_UPDATED_AT > ''' || v_last_load_ts::VARCHAR || '''::TIMESTAMP_NTZ
      ) AS src
      ON ' || v_merge_on_clause || '
      WHEN MATCHED AND tgt._AUDIT_RECORD_HASH != src.record_hash THEN UPDATE SET
        tgt.FIRST_NAME           = src.FIRST_NAME,
        tgt.LAST_NAME            = src.LAST_NAME,
        tgt.FULL_NAME            = src.FULL_NAME,
        tgt.SPECIALTY            = src.SPECIALTY,
        tgt.TERRITORY_CODE       = src.TERRITORY_CODE,
        tgt.REP_ID               = src.REP_ID,
        tgt.LAST_CALL_DATE       = src.LAST_CALL_DATE,
        tgt.TOTAL_CALLS_YTD      = src.TOTAL_CALLS_YTD,
        tgt._AUDIT_UPDATED_AT    = CURRENT_TIMESTAMP(),
        tgt._AUDIT_RUN_ID        = ''' || :P_RUN_ID || ''',
        tgt._AUDIT_SOURCE_RUN_ID = src.source_run_id,
        tgt._AUDIT_RECORD_HASH   = src.record_hash,
        tgt._AUDIT_ENV           = ''' || :P_ENV || '''
      WHEN NOT MATCHED THEN INSERT (
        HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
        TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
        IS_ACTIVE, VALID_FROM, VALID_TO,
        _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
        _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
        _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
      ) VALUES (
        src.HCP_KEY, src.UHCP_ID, src.NPI, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME, src.SPECIALTY,
        src.TERRITORY_CODE, src.REP_ID, src.LAST_CALL_DATE, src.TOTAL_CALLS_YTD,
        TRUE, CURRENT_TIMESTAMP(), NULL::TIMESTAMP_NTZ,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''SP_BUILD_GOLD_HCP'',
        ''' || :P_RUN_ID || ''', src.source_run_id, src.record_hash,
        TRUE, ''PASSED'', ''' || :P_ENV || '''
      )';
    EXECUTE IMMEDIATE v_merge_sql;
    v_rows_inserted := SQLROWCOUNT;
    v_query_id      := (SELECT LAST_QUERY_ID());

    -- SOFT-DELETE: deactivate Gold rows where Silver is no longer current (dynamic PK)
    EXECUTE IMMEDIATE '
      UPDATE NEW_TEST.GOLD.DIM_HCP
      SET IS_ACTIVE          = FALSE,
          _AUDIT_IS_CURRENT  = FALSE,
          VALID_TO           = CURRENT_TIMESTAMP(),
          _AUDIT_UPDATED_AT  = CURRENT_TIMESTAMP(),
          _AUDIT_RUN_ID      = ''' || :P_RUN_ID || '''
      WHERE _AUDIT_IS_CURRENT = TRUE
        AND ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' NOT IN (
          SELECT DISTINCT ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || '
          FROM NEW_TEST.SILVER.HCP_MASTER
          WHERE _AUDIT_IS_CURRENT = TRUE AND _AUDIT_DQ_STATUS = ''PASSED''
        )';
    v_rows_soft_deleted := SQLROWCOUNT;
  END IF;

  -- ═══ STEP 9: POST-LOAD DQ CHECKS (dynamic PK) ═══
  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end FROM NEW_TEST.GOLD.DIM_HCP;
  LET rs5 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) AS cnt FROM NEW_TEST.GOLD.DIM_HCP WHERE ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ' IS NULL AND _AUDIT_IS_CURRENT = TRUE');
  LET cur5 CURSOR FOR rs5;
  OPEN cur5;
  FETCH cur5 INTO v_null_count;
  CLOSE cur5;
  LET rs6 RESULTSET := (EXECUTE IMMEDIATE 'SELECT COUNT(*) - COUNT(DISTINCT ' || TRIM(SPLIT_PART(v_pk_column, ',', 1)) || ') AS cnt FROM NEW_TEST.GOLD.DIM_HCP WHERE _AUDIT_IS_CURRENT = TRUE');
  LET cur6 CURSOR FOR rs6;
  OPEN cur6;
  FETCH cur6 INTO v_duplicate_count;
  CLOSE cur6;

  v_dq_status := CASE
    WHEN v_null_count > 0 OR v_duplicate_count > 0 THEN 'WARNING'
    ELSE 'PASSED'
  END;

  -- ═══ STEP 10: ROW COUNT VALIDATION ═══
  IF (v_expected_rows IS NOT NULL AND v_rows_inserted > 0) THEN
    IF (ABS(v_rows_inserted - v_expected_rows) > (v_expected_rows * v_tolerance_pct / 100)) THEN
      v_dq_status := 'WARNING';
    END IF;
  END IF;

  v_run_status := CASE
    WHEN v_rows_inserted = 0 AND v_load_type = 'INCREMENTAL' AND v_rows_soft_deleted = 0 THEN 'SKIPPED'
    WHEN v_dq_status = 'FAILED' THEN 'FAILED'
    ELSE 'PASSED'
  END;

  -- ═══ STEP 11: UPDATE RUN LOG ═══
  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET RUN_STATUS         = :v_run_status,
      DQ_CHECK_STATUS    = :v_dq_status,
      ROWS_INSERTED      = :v_rows_inserted,
      ROWS_UPDATED       = :v_rows_soft_deleted,
      ROWS_EXPECTED      = :v_expected_rows,
      WATERMARK_START    = :v_watermark_start,
      WATERMARK_END      = :v_watermark_end,
      SNOWFLAKE_QUERY_ID = :v_query_id,
      RUN_END_TIME       = CURRENT_TIMESTAMP(),
      RUN_DURATION_SECS  = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
  WHERE RUN_ID = :v_run_id;

  -- ═══ STEP 12: PERSIST WATERMARK TO PIPELINE_MASTER ═══
  IF (v_run_status = 'PASSED' AND v_watermark_end IS NOT NULL) THEN
    UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
    SET WATERMARK_CURRENT = :v_watermark_end,
        UPDATED_AT        = CURRENT_TIMESTAMP()
    WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = :P_ENV;
  END IF;

  RETURN 'SP_BUILD_GOLD_HCP: ' || v_run_status
    || ' | Load: ' || v_load_type
    || ' | PK: ' || v_pk_column
    || ' | DQ: ' || v_dq_status
    || ' | Inserted: ' || v_rows_inserted::VARCHAR
    || ' | SoftDeleted: ' || v_rows_soft_deleted::VARCHAR
    || ' | Duration: ' || DATEDIFF('second', v_start_time, CURRENT_TIMESTAMP()) || 's';

EXCEPTION
  WHEN OTHER THEN
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET RUN_STATUS      = 'FAILED',
        ERROR_MESSAGE   = SQLERRM,
        RUN_END_TIME    = CURRENT_TIMESTAMP(),
        RUN_DURATION_SECS = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
    WHERE RUN_ID = :v_run_id;
    RETURN 'SP_BUILD_GOLD_HCP: FAILED — ' || SQLERRM;
END;
$$;


-- ─────────────────────────────────────────────────────────────────────────────
-- VERIFICATION: Check updated PIPELINE_MASTER
-- ─────────────────────────────────────────────────────────────────────────────

SELECT PIPELINE_CODE, PIPELINE_LAYER, PK_COLUMN, WATERMARK_COLUMN, WATERMARK_TYPE, LOAD_TYPE
FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
WHERE ENVIRONMENT = 'DEV'
ORDER BY PIPELINE_LAYER, PIPELINE_CODE;

-------------------------------------------------------


----------------------------

//270526

-- Silver SP with retry attempt support
-- Accepts P_RETRY_ATTEMPT (default 0) to track retries in audit log

-- Drop existing Silver SP (the 2-param version)
DROP PROCEDURE IF EXISTS NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP(VARCHAR, VARCHAR);

-- Drop existing Gold SP (the 2-param version)
DROP PROCEDURE IF EXISTS NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP(VARCHAR, VARCHAR);

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP(
  P_RUN_ID        VARCHAR,
  P_ENV           VARCHAR,
  P_RETRY_ATTEMPT NUMBER DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_batch_id        VARCHAR := :P_RUN_ID;
  v_run_id          VARCHAR;
  v_config          VARIANT;
  v_load_type       VARCHAR;
  v_pk_column       VARCHAR;
  v_pk_first        VARCHAR;
  v_merge_on_clause VARCHAR;
  v_rows_read       NUMBER := 0;
  v_rows_inserted   NUMBER := 0;
  v_rows_updated    NUMBER := 0;
  v_rows_rejected   NUMBER := 0;
  v_merge_sql       VARCHAR;
  v_last_load_ts    TIMESTAMP_NTZ;
  v_watermark_end   TIMESTAMP_NTZ;
  v_query_id        VARCHAR;
  v_dq_result       VARIANT;
  v_dq_status       VARCHAR;
  v_tests_passed    NUMBER := 0;
  v_tests_failed    NUMBER := 0;
  v_tests_warned    NUMBER := 0;
  v_is_retry        BOOLEAN;
  v_retry_reason    VARCHAR;
BEGIN

  -- Set retry flags
  v_is_retry := (:P_RETRY_ATTEMPT > 0);
  v_retry_reason := CASE WHEN v_is_retry 
    THEN ''Retry attempt '' || :P_RETRY_ATTEMPT::VARCHAR 
    ELSE NULL 
  END;

  -- Step 1: Open audit row with retry attempt number
  CALL NEW_TEST.AUDIT.LOG_PIPELINE_START(
    ''SP_SILVER_HCP'', :P_ENV, :v_batch_id, :P_RETRY_ATTEMPT, :v_is_retry, :v_retry_reason
  ) INTO :v_run_id;

  -- Step 2: Read pipeline config
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(''SP_SILVER_HCP'', :P_ENV) INTO :v_config;
  v_load_type := COALESCE(v_config:load_type::VARCHAR, ''FULL_REFRESH'');
  v_pk_column := COALESCE(v_config:pk_column::VARCHAR, ''NPI'');
  v_pk_first  := TRIM(SPLIT_PART(v_pk_column, '','', 1));

  -- Step 3: Build dynamic MERGE ON clause
  v_merge_on_clause := ''tgt.'' || v_pk_first || '' = src.'' || v_pk_first;
  IF (SPLIT_PART(v_pk_column, '','', 2) != '''') THEN
    v_merge_on_clause := v_merge_on_clause ||
      '' AND tgt.'' || TRIM(SPLIT_PART(v_pk_column, '','', 2)) ||
      '' = src.''  || TRIM(SPLIT_PART(v_pk_column, '','', 2));
  END IF;
  IF (SPLIT_PART(v_pk_column, '','', 3) != '''') THEN
    v_merge_on_clause := v_merge_on_clause ||
      '' AND tgt.'' || TRIM(SPLIT_PART(v_pk_column, '','', 3)) ||
      '' = src.''  || TRIM(SPLIT_PART(v_pk_column, '','', 3));
  END IF;

  -- Step 4: ROWS_READ from Bronze
  SELECT COUNT(*) INTO v_rows_read
  FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
  WHERE _AUDIT_IS_DELETED = FALSE;

  -- Step 5: Transform Bronze to Silver
  BEGIN TRANSACTION;

  IF (v_load_type = ''FULL_REFRESH'') THEN
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
             ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY _AUDIT_LOAD_TS DESC, CALL_DATE DESC) AS rn
      FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
      WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
    ),
    call_stats AS (
      SELECT NPI, MAX(CALL_DATE) AS last_call_date, COUNT(*) AS total_calls_ytd
      FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
      WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
      GROUP BY NPI
    )
    SELECT
      SHA2(lc.NPI, 256), lc.NPI,
      ''TEST_FIRST_'' || lc.NPI, ''TEST_LAST_'' || lc.NPI,
      UPPER(''TEST_FIRST_'' || lc.NPI || '' TEST_LAST_'' || lc.NPI),
      ''NEUROLOGY'', lc.TERRITORY_CODE, lc.REP_ID,
      cs.last_call_date, COALESCE(cs.total_calls_ytd, 0),
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''SP_BUILD_SILVER_HCP'',
      :v_run_id, lc.bronze_run_id,
      SHA2(lc.NPI || ''|'' || COALESCE(lc.TERRITORY_CODE,'''') || ''|'' || COALESCE(lc.REP_ID,''''), 256),
      TRUE, ''PASSED'', :P_ENV
    FROM latest_call lc
    LEFT JOIN call_stats cs ON lc.NPI = cs.NPI
    WHERE lc.rn = 1;

    v_rows_inserted := SQLROWCOUNT;
    v_rows_updated  := 0;
    v_query_id      := (SELECT LAST_QUERY_ID());

  ELSEIF (v_load_type = ''INCREMENTAL'') THEN
    SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), ''1900-01-01''::TIMESTAMP_NTZ)
    INTO v_last_load_ts
    FROM NEW_TEST.SILVER.HCP_MASTER;

    v_merge_sql := ''
      MERGE INTO NEW_TEST.SILVER.HCP_MASTER AS tgt
      USING (
        WITH latest_call AS (
          SELECT NPI, TERRITORY_CODE, REP_ID, CALL_DATE,
                 _AUDIT_RUN_ID AS bronze_run_id,
                 ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY _AUDIT_LOAD_TS DESC, CALL_DATE DESC) AS rn
          FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
          WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
            AND _AUDIT_LOAD_TS > '''''' || v_last_load_ts::VARCHAR || ''''''::TIMESTAMP_NTZ
        ),
        call_stats AS (
          SELECT NPI, MAX(CALL_DATE) AS last_call_date, COUNT(*) AS total_calls_ytd
          FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
          WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
          GROUP BY NPI
        )
        SELECT
          SHA2(lc.NPI, 256) AS UHCP_ID, lc.NPI,
          ''''TEST_FIRST_'''' || lc.NPI AS FIRST_NAME,
          ''''TEST_LAST_''''  || lc.NPI AS LAST_NAME,
          UPPER(''''TEST_FIRST_'''' || lc.NPI || '''' TEST_LAST_'''' || lc.NPI) AS FULL_NAME,
          ''''NEUROLOGY'''' AS SPECIALTY,
          lc.TERRITORY_CODE, lc.REP_ID,
          cs.last_call_date,
          COALESCE(cs.total_calls_ytd, 0) AS total_calls_ytd,
          lc.bronze_run_id,
          SHA2(lc.NPI || ''''|'''' || COALESCE(lc.TERRITORY_CODE,'''''''') || ''''|'''' || COALESCE(lc.REP_ID,''''''''), 256) AS record_hash
        FROM latest_call lc
        LEFT JOIN call_stats cs ON lc.NPI = cs.NPI
        WHERE lc.rn = 1
      ) AS src
      ON '' || v_merge_on_clause || ''
      WHEN MATCHED AND tgt._AUDIT_RECORD_HASH != src.record_hash THEN UPDATE SET
        tgt.TERRITORY_CODE       = src.TERRITORY_CODE,
        tgt.REP_ID               = src.REP_ID,
        tgt.LAST_CALL_DATE       = src.last_call_date,
        tgt.TOTAL_CALLS_YTD      = src.total_calls_ytd,
        tgt._AUDIT_UPDATED_AT    = CURRENT_TIMESTAMP(),
        tgt._AUDIT_RUN_ID        = '''''' || :v_run_id || '''''',
        tgt._AUDIT_SOURCE_RUN_ID = src.bronze_run_id,
        tgt._AUDIT_RECORD_HASH   = src.record_hash,
        tgt._AUDIT_ENV           = '''''' || :P_ENV || ''''''
      WHEN NOT MATCHED THEN INSERT (
        UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
        TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
        _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
        _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
        _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
      ) VALUES (
        src.UHCP_ID, src.NPI, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME, src.SPECIALTY,
        src.TERRITORY_CODE, src.REP_ID, src.last_call_date, src.total_calls_ytd,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''''SP_BUILD_SILVER_HCP'''',
        '''''' || :v_run_id || '''''', src.bronze_run_id, src.record_hash,
        TRUE, ''''PASSED'''', '''''' || :P_ENV || ''''''
      )'';

    EXECUTE IMMEDIATE v_merge_sql;
    v_query_id := (SELECT LAST_QUERY_ID());

    SELECT "number of rows inserted", "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(:v_query_id));
  END IF;

  COMMIT;

  -- Step 6: Run DQ checks
  CALL NEW_TEST.REFERENCE.RUN_DQM_CHECKS(''SP_SILVER_HCP'', :v_run_id, :P_ENV) INTO :v_dq_result;
  v_tests_passed := COALESCE(v_dq_result:passed_checks::NUMBER, 0);
  v_tests_failed := COALESCE(v_dq_result:failed_checks::NUMBER, 0);
  v_tests_warned := COALESCE(v_dq_result:warned_checks::NUMBER, 0);
  v_dq_status    := COALESCE(v_dq_result:dq_status::VARCHAR, ''PASSED'');

  -- Step 7: Compute watermark
  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end
  FROM NEW_TEST.SILVER.HCP_MASTER;

  -- Step 8: Close audit + send email
  CALL NEW_TEST.AUDIT.LOG_PIPELINE_SUCCESS(
    :v_run_id, ''SP_SILVER_HCP'', :P_ENV,
    :v_rows_read, :v_rows_inserted, :v_rows_updated,
    :v_rows_rejected, 0,
    :v_tests_passed, :v_tests_failed, :v_tests_warned,
    :v_dq_status, :v_watermark_end, :v_query_id
  );

  RETURN ''SP_BUILD_SILVER_HCP: SUCCESS | Load: '' || v_load_type
      || '' | Attempt: '' || :P_RETRY_ATTEMPT::VARCHAR
      || '' | Read: '' || v_rows_read::VARCHAR
      || '' | Inserted: '' || v_rows_inserted::VARCHAR
      || '' | Updated: '' || v_rows_updated::VARCHAR
      || '' | DQ: '' || v_dq_status;

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    IF (v_run_id IS NOT NULL) THEN
      CALL NEW_TEST.AUDIT.LOG_PIPELINE_FAILURE(
        :v_run_id, ''SP_SILVER_HCP'', :P_ENV,
        SQLSTATE, SQLERRM, SQLERRM
      );
    END IF;
    RAISE;  -- re-raise so EXECUTE_WITH_RETRY catches and retries
END;
';

-------------------------
CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP(
  P_RUN_ID        VARCHAR,
  P_ENV           VARCHAR,
  P_RETRY_ATTEMPT NUMBER DEFAULT 0
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_batch_id        VARCHAR := :P_RUN_ID;
  v_run_id          VARCHAR;
  v_config          VARIANT;
  v_load_type       VARCHAR;
  v_pk_column       VARCHAR;
  v_pk_first        VARCHAR;
  v_merge_on_clause VARCHAR;
  v_rows_read       NUMBER := 0;
  v_rows_inserted   NUMBER := 0;
  v_rows_updated    NUMBER := 0;
  v_rows_rejected   NUMBER := 0;
  v_merge_sql       VARCHAR;
  v_last_load_ts    TIMESTAMP_NTZ;
  v_watermark_end   TIMESTAMP_NTZ;
  v_query_id        VARCHAR;
  v_dq_result       VARIANT;
  v_dq_status       VARCHAR;
  v_tests_passed    NUMBER := 0;
  v_tests_failed    NUMBER := 0;
  v_tests_warned    NUMBER := 0;
  v_is_retry        BOOLEAN;
  v_retry_reason    VARCHAR;
BEGIN

  v_is_retry := (:P_RETRY_ATTEMPT > 0);
  v_retry_reason := CASE WHEN v_is_retry 
    THEN ''Retry attempt '' || :P_RETRY_ATTEMPT::VARCHAR 
    ELSE NULL 
  END;

  CALL NEW_TEST.AUDIT.LOG_PIPELINE_START(
    ''SP_GOLD_HCP'', :P_ENV, :v_batch_id, :P_RETRY_ATTEMPT, :v_is_retry, :v_retry_reason
  ) INTO :v_run_id;

  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(''SP_GOLD_HCP'', :P_ENV) INTO :v_config;
  v_load_type := COALESCE(v_config:load_type::VARCHAR, ''FULL_REFRESH'');
  v_pk_column := COALESCE(v_config:pk_column::VARCHAR, ''NPI'');
  v_pk_first  := TRIM(SPLIT_PART(v_pk_column, '','', 1));

  v_merge_on_clause := ''tgt.'' || v_pk_first || '' = src.'' || v_pk_first;
  IF (SPLIT_PART(v_pk_column, '','', 2) != '''') THEN
    v_merge_on_clause := v_merge_on_clause ||
      '' AND tgt.'' || TRIM(SPLIT_PART(v_pk_column, '','', 2)) ||
      '' = src.''  || TRIM(SPLIT_PART(v_pk_column, '','', 2));
  END IF;

  SELECT COUNT(*) INTO v_rows_read
  FROM NEW_TEST.SILVER.HCP_MASTER
  WHERE _AUDIT_IS_CURRENT = TRUE;

  BEGIN TRANSACTION;

  IF (v_load_type = ''FULL_REFRESH'') THEN
    TRUNCATE TABLE NEW_TEST.GOLD.DIM_HCP;

    INSERT INTO NEW_TEST.GOLD.DIM_HCP (
      HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      IS_ACTIVE, VALID_FROM, VALID_TO,
      _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
      _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
      _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
    )
    SELECT
      SHA2(NPI, 256), UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      TRUE, CURRENT_TIMESTAMP(), NULL,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''SP_BUILD_GOLD_HCP'',
      :v_run_id, _AUDIT_RUN_ID,
      SHA2(NPI || ''|'' || COALESCE(TERRITORY_CODE,'''') || ''|'' || COALESCE(REP_ID,''''), 256),
      TRUE, ''PASSED'', :P_ENV
    FROM NEW_TEST.SILVER.HCP_MASTER
    WHERE _AUDIT_IS_CURRENT = TRUE;

    v_rows_inserted := SQLROWCOUNT;
    v_rows_updated  := 0;
    v_query_id      := (SELECT LAST_QUERY_ID());

  ELSEIF (v_load_type = ''INCREMENTAL'') THEN
    SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), ''1900-01-01''::TIMESTAMP_NTZ)
    INTO v_last_load_ts
    FROM NEW_TEST.GOLD.DIM_HCP;

    v_merge_sql := ''
      MERGE INTO NEW_TEST.GOLD.DIM_HCP AS tgt
      USING (
        SELECT
          SHA2(NPI, 256) AS HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
          TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
          TRUE AS IS_ACTIVE, _AUDIT_RUN_ID AS silver_run_id,
          SHA2(NPI || ''''|'''' || COALESCE(TERRITORY_CODE,'''''''') || ''''|'''' || COALESCE(REP_ID,''''''''), 256) AS record_hash
        FROM NEW_TEST.SILVER.HCP_MASTER
        WHERE _AUDIT_IS_CURRENT = TRUE
          AND _AUDIT_UPDATED_AT > '''''' || v_last_load_ts::VARCHAR || ''''''::TIMESTAMP_NTZ
      ) AS src
      ON '' || v_merge_on_clause || ''
      WHEN MATCHED AND tgt._AUDIT_RECORD_HASH != src.record_hash THEN UPDATE SET
        tgt.UHCP_ID = src.UHCP_ID, tgt.FIRST_NAME = src.FIRST_NAME,
        tgt.LAST_NAME = src.LAST_NAME, tgt.FULL_NAME = src.FULL_NAME,
        tgt.SPECIALTY = src.SPECIALTY, tgt.TERRITORY_CODE = src.TERRITORY_CODE,
        tgt.REP_ID = src.REP_ID, tgt.LAST_CALL_DATE = src.LAST_CALL_DATE,
        tgt.TOTAL_CALLS_YTD = src.TOTAL_CALLS_YTD, tgt.IS_ACTIVE = src.IS_ACTIVE,
        tgt._AUDIT_UPDATED_AT = CURRENT_TIMESTAMP(),
        tgt._AUDIT_RUN_ID = '''''' || :v_run_id || '''''',
        tgt._AUDIT_SOURCE_RUN_ID = src.silver_run_id,
        tgt._AUDIT_RECORD_HASH = src.record_hash,
        tgt._AUDIT_ENV = '''''' || :P_ENV || ''''''
      WHEN NOT MATCHED THEN INSERT (
        HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
        TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
        IS_ACTIVE, VALID_FROM, VALID_TO,
        _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
        _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
        _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
      ) VALUES (
        src.HCP_KEY, src.UHCP_ID, src.NPI, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME, src.SPECIALTY,
        src.TERRITORY_CODE, src.REP_ID, src.LAST_CALL_DATE, src.TOTAL_CALLS_YTD,
        src.IS_ACTIVE, CURRENT_TIMESTAMP(), NULL,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), ''''SP_BUILD_GOLD_HCP'''',
        '''''' || :v_run_id || '''''', src.silver_run_id, src.record_hash,
        TRUE, ''''PASSED'''', '''''' || :P_ENV || ''''''
      )'';

    EXECUTE IMMEDIATE v_merge_sql;
    v_query_id := (SELECT LAST_QUERY_ID());

    SELECT "number of rows inserted", "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(:v_query_id));
  END IF;

  COMMIT;

  CALL NEW_TEST.REFERENCE.RUN_DQM_CHECKS(''SP_GOLD_HCP'', :v_run_id, :P_ENV) INTO :v_dq_result;
  v_tests_passed := COALESCE(v_dq_result:passed_checks::NUMBER, 0);
  v_tests_failed := COALESCE(v_dq_result:failed_checks::NUMBER, 0);
  v_tests_warned := COALESCE(v_dq_result:warned_checks::NUMBER, 0);
  v_dq_status    := COALESCE(v_dq_result:dq_status::VARCHAR, ''PASSED'');

  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end FROM NEW_TEST.GOLD.DIM_HCP;

  CALL NEW_TEST.AUDIT.LOG_PIPELINE_SUCCESS(
    :v_run_id, ''SP_GOLD_HCP'', :P_ENV,
    :v_rows_read, :v_rows_inserted, :v_rows_updated,
    :v_rows_rejected, 0,
    :v_tests_passed, :v_tests_failed, :v_tests_warned,
    :v_dq_status, :v_watermark_end, :v_query_id
  );

  RETURN ''SP_BUILD_GOLD_HCP: SUCCESS | Attempt: '' || :P_RETRY_ATTEMPT::VARCHAR
      || '' | Inserted: '' || v_rows_inserted::VARCHAR
      || '' | DQ: '' || v_dq_status;

EXCEPTION
  WHEN OTHER THEN
    ROLLBACK;
    IF (v_run_id IS NOT NULL) THEN
      CALL NEW_TEST.AUDIT.LOG_PIPELINE_FAILURE(
        :v_run_id, ''SP_GOLD_HCP'', :P_ENV,
        SQLSTATE, SQLERRM, SQLERRM
      );
    END IF;
    RAISE;  -- re-raise for retry
END;
';