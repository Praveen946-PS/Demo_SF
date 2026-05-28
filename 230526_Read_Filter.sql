-- ============================================================
-- STEP 1: Add new columns to PIPELINE_MASTER
-- ============================================================
ALTER TABLE NEW_TEST.REFERENCE.PIPELINE_MASTER
ADD COLUMN READ_FILTER VARCHAR(1000);

ALTER TABLE NEW_TEST.REFERENCE.PIPELINE_MASTER
ADD COLUMN SOURCE_TABLE VARCHAR(500);

select * from NEW_TEST.REFERENCE.PIPELINE_MASTER


-- ============================================================
-- STEP 2: Populate SOURCE_TABLE and READ_FILTER
-- ============================================================
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET SOURCE_TABLE = 'NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS',
    READ_FILTER  = '_AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL'
WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = 'DEV';

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET SOURCE_TABLE = 'NEW_TEST.SILVER.HCP_MASTER',
    READ_FILTER  = '_AUDIT_DQ_STATUS = ''PASSED'''
WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = 'DEV';


-- ============================================================
-- STEP 3: Update GET_PIPELINE_CONFIG to return new columns
-- ============================================================
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
    'source_table',            SOURCE_TABLE,
    'target_table',            TARGET_TABLE,
    'read_filter',             READ_FILTER,
    'is_active',               IS_ACTIVE,
    'inactive_reason',         INACTIVE_REASON,
    'schedule_type',           SCHEDULE_TYPE,
    'schedule_cron',           SCHEDULE_CRON,
    'watermark_column',        WATERMARK_COLUMN,
    'watermark_type',          WATERMARK_TYPE,
    'watermark_current',       WATERMARK_CURRENT,
    'watermark_lookback_hrs',  WATERMARK_LOOKBACK_HRS,
    'load_type',               LOAD_TYPE,
    'batch_size',              BATCH_SIZE,
    'expected_row_count',      EXPECTED_ROW_COUNT,
    'row_count_tolerance_pct', ROW_COUNT_TOLERANCE_PCT,
    'max_retry_count',         MAX_RETRY_COUNT,
    'retry_backoff_secs',      RETRY_BACKOFF_SECS,
    'retry_backoff_type',      RETRY_BACKOFF_TYPE,
    'current_retry_count',     CURRENT_RETRY_COUNT,
    'notify_on_failure',       NOTIFY_ON_FAILURE,
    'notify_on_success',       NOTIFY_ON_SUCCESS,
    'notification_emails',     NOTIFICATION_EMAILS,
    'slack_channel',           SLACK_CHANNEL,
    'alert_priority',          ALERT_PRIORITY,
    'depends_on_pipelines',    DEPENDS_ON_PIPELINES,
    'dependency_timeout_mins', DEPENDENCY_TIMEOUT_MINS
  ) INTO v_config
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;
  RETURN v_config;
END;
$$;


-- ============================================================
-- STEP 4: Update SILVER SP with READ_FILTER, SOURCE_TABLE, 
--         ROWS_READ, ROWS_UPDATED, RUN_DURATION_SECS, RUN_TRIGGER_TYPE,
--         DQ counters (TESTS_PASSED/FAILED/WARNED), ROWS_REJECTED
-- ============================================================
CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_read       NUMBER := 0;
  v_rows_inserted   NUMBER := 0;
  v_rows_updated    NUMBER := 0;
  v_rows_rejected   NUMBER := 0;
  v_bronze_total    NUMBER := 0;
  v_tests_passed    NUMBER := 0;
  v_tests_failed    NUMBER := 0;
  v_tests_warned    NUMBER := 0;
  v_config          VARIANT;
  v_load_type       VARCHAR;
  v_domain          VARCHAR;
  v_source_code     VARCHAR;
  v_source_table    VARCHAR;
  v_target_table    VARCHAR;
  v_read_filter     VARCHAR;
  v_watermark_start VARCHAR;
  v_watermark_end   TIMESTAMP_NTZ;
  v_dq_status       VARCHAR;
  v_run_status      VARCHAR;
  v_null_count      NUMBER := 0;
  v_duplicate_count NUMBER := 0;
  v_query_id        VARCHAR;
  v_run_id          VARCHAR := UUID_STRING();
  v_start_time      TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_end_time        TIMESTAMP_NTZ;
  v_duration_secs   NUMBER := 0;
  v_trigger_type    VARCHAR := 'SCHEDULED';
  v_last_load_ts    TIMESTAMP_NTZ := '1900-01-01'::TIMESTAMP_NTZ;
  v_sql             VARCHAR;
  v_rs              RESULTSET;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_SILVER_HCP', :P_ENV) INTO :v_config;
  v_load_type       := COALESCE(v_config:load_type::VARCHAR,        'FULL_REFRESH');
  v_domain          := COALESCE(v_config:domain::VARCHAR,           'HCP');
  v_source_code     := COALESCE(v_config:source_code::VARCHAR,      'VEEVA_CRM');
  v_source_table    := COALESCE(v_config:source_table::VARCHAR,     'NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS');
  v_target_table    := COALESCE(v_config:target_table::VARCHAR,     'NEW_TEST.SILVER.HCP_MASTER');
  v_read_filter     := COALESCE(v_config:read_filter::VARCHAR,      '1=1');
  v_watermark_start := COALESCE(v_config:watermark_current::VARCHAR, NULL);
 
  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
    SOURCE_CODE, TARGET_TABLE, RUN_STATUS, RUN_TRIGGER_TYPE,
    RUN_START_TIME, TRIGGERED_BY, CREATED_AT
  ) VALUES (
    :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_SILVER_HCP', 'SILVER',
    :v_source_code, :v_target_table, 'RUNNING', :v_trigger_type,
    :v_start_time, 'SP_BUILD_SILVER_HCP', CURRENT_TIMESTAMP()
  );
 
  -- Count TOTAL records in source (no filter)
  v_sql := 'SELECT COUNT(*) FROM ' || v_source_table;
  v_rs := (EXECUTE IMMEDIATE :v_sql);
  LET c0 CURSOR FOR v_rs;
  OPEN c0;
  FETCH c0 INTO v_bronze_total;
  CLOSE c0;
 
  -- Count ROWS_READ from source using dynamic READ_FILTER
  v_sql := 'SELECT COUNT(*) FROM ' || v_source_table || ' WHERE ' || v_read_filter;
  v_rs := (EXECUTE IMMEDIATE :v_sql);
  LET c1 CURSOR FOR v_rs;
  OPEN c1;
  FETCH c1 INTO v_rows_read;
  CLOSE c1;
 
  -- ROWS_REJECTED = total in source - rows passing filter
  v_rows_rejected := v_bronze_total - v_rows_read;
 
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
             ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY _AUDIT_LOAD_TS DESC, CALL_DATE DESC) AS rn
      FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
      WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
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
    v_rows_updated  := 0;
    v_query_id      := (SELECT LAST_QUERY_ID());
 
  ELSEIF (v_load_type = 'INCREMENTAL') THEN
    SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), '1900-01-01'::TIMESTAMP_NTZ)
    INTO v_last_load_ts
    FROM NEW_TEST.SILVER.HCP_MASTER;
 
    MERGE INTO NEW_TEST.SILVER.HCP_MASTER AS tgt
    USING (
      WITH latest_call AS (
        SELECT NPI, TERRITORY_CODE, REP_ID, CALL_DATE,
               _AUDIT_RUN_ID AS bronze_run_id,
               ROW_NUMBER() OVER (PARTITION BY NPI ORDER BY _AUDIT_LOAD_TS DESC, CALL_DATE DESC) AS rn
        FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
        WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
          AND _AUDIT_LOAD_TS > :v_last_load_ts
      ),
      call_stats AS (
        SELECT NPI, MAX(CALL_DATE) AS last_call_date, COUNT(*) AS total_calls_ytd
        FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS
        WHERE _AUDIT_IS_DELETED = FALSE AND NPI IS NOT NULL
          AND CALL_DATE >= DATE_TRUNC('year', CURRENT_DATE())
        GROUP BY NPI
      )
      SELECT SHA2(lc.NPI, 256) AS UHCP_ID, lc.NPI,
        'TEST_FIRST_' || lc.NPI AS FIRST_NAME,
        'TEST_LAST_' || lc.NPI AS LAST_NAME,
        UPPER('TEST_FIRST_' || lc.NPI || ' TEST_LAST_' || lc.NPI) AS FULL_NAME,
        'NEUROLOGY' AS SPECIALTY, lc.TERRITORY_CODE, lc.REP_ID,
        cs.last_call_date, COALESCE(cs.total_calls_ytd, 0) AS total_calls_ytd,
        lc.bronze_run_id,
        SHA2(lc.NPI || '|' || COALESCE(lc.TERRITORY_CODE,'') || '|' || COALESCE(lc.REP_ID,''), 256) AS record_hash
      FROM latest_call lc LEFT JOIN call_stats cs ON lc.NPI = cs.NPI WHERE lc.rn = 1
    ) AS src
    ON tgt.NPI = src.NPI
    WHEN MATCHED AND tgt._AUDIT_RECORD_HASH != src.record_hash THEN UPDATE SET
      tgt.TERRITORY_CODE       = src.TERRITORY_CODE,
      tgt.REP_ID               = src.REP_ID,
      tgt.LAST_CALL_DATE       = src.last_call_date,
      tgt.TOTAL_CALLS_YTD      = src.total_calls_ytd,
      tgt._AUDIT_UPDATED_AT    = CURRENT_TIMESTAMP(),
      tgt._AUDIT_RUN_ID        = :P_RUN_ID,
      tgt._AUDIT_SOURCE_RUN_ID = src.bronze_run_id,
      tgt._AUDIT_RECORD_HASH   = src.record_hash,
      tgt._AUDIT_ENV           = :P_ENV
    WHEN NOT MATCHED THEN INSERT (
      UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
      TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
      _AUDIT_CREATED_AT, _AUDIT_UPDATED_AT, _AUDIT_CREATED_BY,
      _AUDIT_RUN_ID, _AUDIT_SOURCE_RUN_ID, _AUDIT_RECORD_HASH,
      _AUDIT_IS_CURRENT, _AUDIT_DQ_STATUS, _AUDIT_ENV
    ) VALUES (
      src.UHCP_ID, src.NPI, src.FIRST_NAME, src.LAST_NAME, src.FULL_NAME, src.SPECIALTY,
      src.TERRITORY_CODE, src.REP_ID, src.last_call_date, src.total_calls_ytd,
      CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), 'SP_BUILD_SILVER_HCP',
      :P_RUN_ID, src.bronze_run_id, src.record_hash,
      TRUE, 'PASSED', :P_ENV
    );
    v_query_id := (SELECT LAST_QUERY_ID());
 
    SELECT "number of rows inserted", "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(:v_query_id));
  END IF;
 
  -- Target DQ checks
  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end FROM NEW_TEST.SILVER.HCP_MASTER;
  SELECT COUNT(*) INTO v_null_count FROM NEW_TEST.SILVER.HCP_MASTER WHERE NPI IS NULL;
  SELECT COUNT(*) - COUNT(DISTINCT NPI) INTO v_duplicate_count FROM NEW_TEST.SILVER.HCP_MASTER;
 
  IF (v_null_count = 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_failed := v_tests_failed + 1; END IF;
 
  IF (v_duplicate_count = 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_failed := v_tests_failed + 1; END IF;
 
  IF (v_rows_inserted + v_rows_updated > 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_warned := v_tests_warned + 1; END IF;
 
  v_dq_status := CASE
    WHEN v_tests_failed > 0 THEN 'FAILED'
    WHEN v_tests_warned > 0 THEN 'WARNING'
    ELSE 'PASSED'
  END;
 
  v_run_status := CASE
    WHEN v_rows_inserted = 0 AND v_rows_updated = 0 AND v_load_type = 'INCREMENTAL' THEN 'SKIPPED'
    WHEN v_dq_status = 'FAILED' THEN 'FAILED'
    ELSE 'PASSED'
  END;
 
  v_end_time := CURRENT_TIMESTAMP();
  v_duration_secs := DATEDIFF('second', v_start_time, v_end_time);
 
  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET RUN_STATUS         = :v_run_status,
      DQ_CHECK_STATUS    = :v_dq_status,
      ROWS_READ          = :v_rows_read,
      ROWS_INSERTED      = :v_rows_inserted,
      ROWS_UPDATED       = :v_rows_updated,
      ROWS_REJECTED      = :v_rows_rejected,
      TESTS_PASSED       = :v_tests_passed,
      TESTS_FAILED       = :v_tests_failed,
      TESTS_WARNED       = :v_tests_warned,
      WATERMARK_START    = :v_watermark_start,
      WATERMARK_END      = :v_watermark_end,
      SNOWFLAKE_QUERY_ID = :v_query_id,
      RUN_END_TIME       = :v_end_time,
      RUN_DURATION_SECS  = :v_duration_secs
  WHERE RUN_ID = :v_run_id;
 
  IF (v_load_type = 'FULL_REFRESH' AND v_run_status = 'PASSED') THEN
    UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
    SET LOAD_TYPE  = 'INCREMENTAL',
        UPDATED_AT = CURRENT_TIMESTAMP(),
        UPDATED_BY = 'SP_BUILD_SILVER_HCP'
    WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = :P_ENV;
  END IF;
 
  RETURN 'SP_BUILD_SILVER_HCP: SUCCESS | Load: ' || v_load_type || ' | Status: ' || v_run_status || ' | Read: ' || v_rows_read::VARCHAR || ' | Rejected: ' || v_rows_rejected::VARCHAR || ' | Inserted: ' || v_rows_inserted::VARCHAR || ' | Updated: ' || v_rows_updated::VARCHAR;
 
EXCEPTION
  WHEN OTHER THEN
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET RUN_STATUS    = 'FAILED',
        ERROR_MESSAGE = SQLERRM,
        RUN_END_TIME  = CURRENT_TIMESTAMP(),
        RUN_DURATION_SECS = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
    WHERE RUN_ID = :v_run_id;
    RETURN 'SP_BUILD_SILVER_HCP: FAILED - ' || SQLERRM;
END;
$$;
 
 
-- ============================================================
-- GOLD SP — ROWS_REJECTED = Silver records that failed READ_FILTER
-- ============================================================
CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP(
  P_RUN_ID VARCHAR, P_ENV VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_rows_read       NUMBER := 0;
  v_rows_inserted   NUMBER := 0;
  v_rows_updated    NUMBER := 0;
  v_rows_rejected   NUMBER := 0;
  v_silver_total    NUMBER := 0;
  v_tests_passed    NUMBER := 0;
  v_tests_failed    NUMBER := 0;
  v_tests_warned    NUMBER := 0;
  v_silver_rows     NUMBER := 0;
  v_config          VARIANT;
  v_load_type       VARCHAR;
  v_pk_column       VARCHAR;
  v_merge_sql       VARCHAR;
  v_domain          VARCHAR;
  v_source_code     VARCHAR;
  v_source_table    VARCHAR;
  v_target_table    VARCHAR;
  v_read_filter     VARCHAR;
  v_watermark_start VARCHAR;
  v_watermark_end   TIMESTAMP_NTZ;
  v_dq_status       VARCHAR;
  v_run_status      VARCHAR;
  v_null_count      NUMBER := 0;
  v_duplicate_count NUMBER := 0;
  v_query_id        VARCHAR;
  v_run_id          VARCHAR := UUID_STRING();
  v_start_time      TIMESTAMP_NTZ := CURRENT_TIMESTAMP();
  v_end_time        TIMESTAMP_NTZ;
  v_duration_secs   NUMBER := 0;
  v_trigger_type    VARCHAR := 'SCHEDULED';
  v_sql             VARCHAR;
  v_rs              RESULTSET;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG('SP_GOLD_HCP', :P_ENV) INTO :v_config;
  v_load_type       := COALESCE(v_config:load_type::VARCHAR,         'FULL_REFRESH');
  v_pk_column       := 'NPI';
  v_domain          := COALESCE(v_config:domain::VARCHAR,            'HCP');
  v_source_code     := COALESCE(v_config:source_code::VARCHAR,       'SILVER');
  v_source_table    := COALESCE(v_config:source_table::VARCHAR,      'NEW_TEST.SILVER.HCP_MASTER');
  v_target_table    := COALESCE(v_config:target_table::VARCHAR,      'NEW_TEST.GOLD.DIM_HCP');
  v_read_filter     := COALESCE(v_config:read_filter::VARCHAR,       '1=1');
  v_watermark_start := COALESCE(v_config:watermark_current::VARCHAR,  NULL);
 
  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
    SOURCE_CODE, TARGET_TABLE, RUN_STATUS, RUN_TRIGGER_TYPE,
    RUN_START_TIME, TRIGGERED_BY, CREATED_AT
  ) VALUES (
    :v_run_id, :P_RUN_ID, :P_ENV, :v_domain, 'SP_GOLD_HCP', 'GOLD',
    :v_source_code, :v_target_table, 'RUNNING', :v_trigger_type,
    :v_start_time, 'SP_BUILD_GOLD_HCP', CURRENT_TIMESTAMP()
  );
 
  -- Count TOTAL records in source (no filter)
  v_sql := 'SELECT COUNT(*) FROM ' || v_source_table;
  v_rs := (EXECUTE IMMEDIATE :v_sql);
  LET c0 CURSOR FOR v_rs;
  OPEN c0;
  FETCH c0 INTO v_silver_total;
  CLOSE c0;
 
  -- Count ROWS_READ from source using dynamic READ_FILTER
  v_sql := 'SELECT COUNT(*) FROM ' || v_source_table || ' WHERE ' || v_read_filter;
  v_rs := (EXECUTE IMMEDIATE :v_sql);
  LET c2 CURSOR FOR v_rs;
  OPEN c2;
  FETCH c2 INTO v_rows_read;
  CLOSE c2;
 
  -- ROWS_REJECTED = total - passing filter
  v_rows_rejected := v_silver_total - v_rows_read;
  v_silver_rows := v_rows_read;
 
  IF (v_silver_rows = 0) THEN
    v_end_time := CURRENT_TIMESTAMP();
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET RUN_STATUS = 'SKIPPED', DQ_CHECK_STATUS = 'PASSED',
        ROWS_READ = 0, ROWS_INSERTED = 0, ROWS_UPDATED = 0,
        ROWS_REJECTED = :v_rows_rejected,
        RUN_END_TIME = :v_end_time,
        RUN_DURATION_SECS = DATEDIFF('second', :v_start_time, :v_end_time)
    WHERE RUN_ID = :v_run_id;
    RETURN 'SP_BUILD_GOLD_HCP: SKIPPED - Silver source is empty.';
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
    v_rows_inserted := SQLROWCOUNT;
    v_rows_updated  := 0;
    v_query_id      := (SELECT LAST_QUERY_ID());
 
  ELSEIF (v_load_type = 'INCREMENTAL') THEN
    v_merge_sql := '
      MERGE INTO NEW_TEST.GOLD.DIM_HCP AS tgt
      USING (
        SELECT SHA2(UHCP_ID, 256) AS HCP_KEY, UHCP_ID, NPI, FIRST_NAME, LAST_NAME, FULL_NAME, SPECIALTY,
          TERRITORY_CODE, REP_ID, LAST_CALL_DATE, TOTAL_CALLS_YTD,
          _AUDIT_RUN_ID AS source_run_id,
          SHA2(UHCP_ID || ''|'' || NPI || ''|'' || COALESCE(TERRITORY_CODE,''''), 256) AS record_hash
        FROM NEW_TEST.SILVER.HCP_MASTER
        WHERE _AUDIT_DQ_STATUS = ''PASSED''
          AND _AUDIT_UPDATED_AT > (SELECT COALESCE(MAX(_AUDIT_UPDATED_AT), ''1900-01-01''::TIMESTAMP_NTZ) FROM NEW_TEST.GOLD.DIM_HCP)
      ) AS src
      ON tgt.' || v_pk_column || ' = src.' || v_pk_column || ' AND tgt._AUDIT_IS_CURRENT = TRUE
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
    v_query_id := (SELECT LAST_QUERY_ID());
 
    SELECT "number of rows inserted", "number of rows updated"
    INTO :v_rows_inserted, :v_rows_updated
    FROM TABLE(RESULT_SCAN(:v_query_id));
  END IF;
 
  SELECT MAX(_AUDIT_UPDATED_AT) INTO v_watermark_end FROM NEW_TEST.GOLD.DIM_HCP;
  SELECT COUNT(*) INTO v_null_count FROM NEW_TEST.GOLD.DIM_HCP WHERE NPI IS NULL;
  SELECT COUNT(*) - COUNT(DISTINCT NPI) INTO v_duplicate_count FROM NEW_TEST.GOLD.DIM_HCP;
 
  IF (v_null_count = 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_failed := v_tests_failed + 1; END IF;
 
  IF (v_duplicate_count = 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_failed := v_tests_failed + 1; END IF;
 
  IF (v_rows_inserted + v_rows_updated > 0) THEN v_tests_passed := v_tests_passed + 1;
  ELSE v_tests_warned := v_tests_warned + 1; END IF;
 
  v_dq_status := CASE
    WHEN v_tests_failed > 0 THEN 'FAILED'
    WHEN v_tests_warned > 0 THEN 'WARNING'
    ELSE 'PASSED'
  END;
 
  v_run_status := CASE
    WHEN v_rows_inserted = 0 AND v_rows_updated = 0 AND v_load_type = 'INCREMENTAL' THEN 'SKIPPED'
    WHEN v_dq_status = 'FAILED' THEN 'FAILED'
    ELSE 'PASSED'
  END;
 
  v_end_time := CURRENT_TIMESTAMP();
  v_duration_secs := DATEDIFF('second', v_start_time, v_end_time);
 
  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET RUN_STATUS         = :v_run_status,
      DQ_CHECK_STATUS    = :v_dq_status,
      ROWS_READ          = :v_rows_read,
      ROWS_INSERTED      = :v_rows_inserted,
      ROWS_UPDATED       = :v_rows_updated,
      ROWS_REJECTED      = :v_rows_rejected,
      TESTS_PASSED       = :v_tests_passed,
      TESTS_FAILED       = :v_tests_failed,
      TESTS_WARNED       = :v_tests_warned,
      WATERMARK_START    = :v_watermark_start,
      WATERMARK_END      = :v_watermark_end,
      SNOWFLAKE_QUERY_ID = :v_query_id,
      RUN_END_TIME       = :v_end_time,
      RUN_DURATION_SECS  = :v_duration_secs
  WHERE RUN_ID = :v_run_id;
 
  IF (v_load_type = 'FULL_REFRESH' AND v_run_status = 'PASSED') THEN
    UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
    SET LOAD_TYPE  = 'INCREMENTAL',
        UPDATED_AT = CURRENT_TIMESTAMP(),
        UPDATED_BY = 'SP_BUILD_GOLD_HCP'
    WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = :P_ENV;
  END IF;
 
  RETURN 'SP_BUILD_GOLD_HCP: SUCCESS | Load: ' || v_load_type || ' | Status: ' || v_run_status || ' | Read: ' || v_rows_read::VARCHAR || ' | Rejected: ' || v_rows_rejected::VARCHAR || ' | Inserted: ' || v_rows_inserted::VARCHAR || ' | Updated: ' || v_rows_updated::VARCHAR;
 
EXCEPTION
  WHEN OTHER THEN
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET RUN_STATUS    = 'FAILED',
        ERROR_MESSAGE = SQLERRM,
        RUN_END_TIME  = CURRENT_TIMESTAMP(),
        RUN_DURATION_SECS = DATEDIFF('second', :v_start_time, CURRENT_TIMESTAMP())
    WHERE RUN_ID = :v_run_id;
    RETURN 'SP_BUILD_GOLD_HCP: FAILED - ' || SQLERRM;
END;
$$;