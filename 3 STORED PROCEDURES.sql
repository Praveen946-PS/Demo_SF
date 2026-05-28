-- Read pipeline configuration from PIPELINE_MASTER
-- Called by MuleSoft before every pipeline run

CREATE OR REPLACE PROCEDURE
  NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(
  P_PIPELINE_CODE VARCHAR,
  P_ENVIRONMENT   VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
AS
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
---

-- Activate or deactivate any pipeline dynamically
-- No code changes needed — update control table only

CREATE OR REPLACE PROCEDURE
  NEW_TEST.REFERENCE.SET_PIPELINE_STATUS(
  P_PIPELINE_CODE VARCHAR,
  P_ENVIRONMENT   VARCHAR,
  P_IS_ACTIVE     BOOLEAN,
  P_REASON        VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
  SET
    IS_ACTIVE       = :P_IS_ACTIVE,
    INACTIVE_REASON = CASE
      WHEN :P_IS_ACTIVE THEN NULL
      ELSE :P_REASON
    END,
    DEACTIVATED_BY  = CASE
      WHEN :P_IS_ACTIVE THEN NULL
      ELSE CURRENT_USER()
    END,
    DEACTIVATED_AT  = CASE
      WHEN :P_IS_ACTIVE THEN NULL
      ELSE CURRENT_TIMESTAMP()
    END,
    UPDATED_AT      = CURRENT_TIMESTAMP(),
    UPDATED_BY      = CURRENT_USER()
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;

  RETURN :P_PIPELINE_CODE || ' → active=' ||
         :P_IS_ACTIVE::VARCHAR ||
         CASE WHEN NOT :P_IS_ACTIVE
              THEN ' | reason: ' || :P_REASON
              ELSE ''
         END;
END;
$$;

-- Usage:
-- Deactivate IQVIA when vendor is late:
CALL NEW_TEST.REFERENCE.SET_PIPELINE_STATUS(
  'IQVIA_CLAIMS_INBOUND', 'DEV', FALSE,
  'Vendor file delayed — resuming next Sunday'
);

-- Reactivate:
CALL NEW_TEST.REFERENCE.SET_PIPELINE_STATUS(
  'IQVIA_CLAIMS_INBOUND', 'DEV', TRUE, NULL
);


-- Update watermark in PIPELINE_MASTER after success
-- Also resets retry count

CREATE OR REPLACE PROCEDURE
  NEW_TEST.REFERENCE.UPDATE_WATERMARK(
  P_PIPELINE_CODE    VARCHAR,
  P_ENVIRONMENT      VARCHAR,
  P_WATERMARK_VALUE  TIMESTAMP_NTZ
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
BEGIN
  UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
  SET
    WATERMARK_CURRENT     = :P_WATERMARK_VALUE,
    CURRENT_RETRY_COUNT   = 0,
    UPDATED_AT            = CURRENT_TIMESTAMP(),
    UPDATED_BY            = 'PIPELINE_SP'
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;

  RETURN 'Watermark updated to: ' ||
         :P_WATERMARK_VALUE::VARCHAR;
END;
$$;

-- Creates the run log record at pipeline start
-- Returns RUN_ID used for all subsequent logging

CREATE OR REPLACE PROCEDURE  NEW_TEST.AUDIT.LOG_PIPELINE_START(
  P_PIPELINE_CODE   VARCHAR,
  P_ENVIRONMENT     VARCHAR,
  P_BATCH_ID        VARCHAR,
  P_RETRY_ATTEMPT   NUMBER,
  P_IS_RETRY        BOOLEAN,
  P_RETRY_REASON    VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_run_id VARCHAR := UUID_STRING(); v_config VARIANT;
  v_domain VARCHAR; v_layer VARCHAR; v_source VARCHAR; v_target VARCHAR;
  v_watermark_start TIMESTAMP_NTZ; v_expected_rows NUMBER; v_task_name VARCHAR;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(:P_PIPELINE_CODE, :P_ENVIRONMENT) INTO v_config;
  v_domain := v_config:domain::VARCHAR;
  v_layer := v_config:pipeline_layer::VARCHAR;
  v_source := v_config:source_code::VARCHAR;
  v_target := v_config:target_table::VARCHAR;
  v_watermark_start := v_config:watermark_current::TIMESTAMP_NTZ;
  v_expected_rows := v_config:expected_row_count::NUMBER;

  BEGIN
    v_task_name := SYSTEM$TASK_RUNTIME_INFO('CURRENT_TASK_NAME');
  EXCEPTION
    WHEN OTHER THEN
      v_task_name := :P_PIPELINE_CODE;
  END;

  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE, PIPELINE_LAYER,
    SOURCE_CODE, TARGET_TABLE, RUN_STATUS, RUN_TRIGGER_TYPE,
    RUN_DATE, RUN_START_TIME, ROWS_EXPECTED, WATERMARK_START,
    RETRY_ATTEMPT, IS_RETRY, RETRY_REASON, SNOWFLAKE_TASK_NAME,
    TRIGGERED_BY, CREATED_AT
  ) VALUES (
    :v_run_id, :P_BATCH_ID, :P_ENVIRONMENT, :v_domain, :P_PIPELINE_CODE, :v_layer,
    :v_source, :v_target, 'RUNNING', 'SCHEDULED',
    CURRENT_DATE(), CURRENT_TIMESTAMP(), :v_expected_rows, :v_watermark_start,
    :P_RETRY_ATTEMPT, :P_IS_RETRY, :P_RETRY_REASON, :v_task_name,
    CURRENT_USER(), CURRENT_TIMESTAMP()
  );
  RETURN v_run_id;
END;
$$;

//Closes out the audit row (UPDATE to PASSED + fill counts), updates PIPELINE_MASTER watermark, sends notification if configured, marks notification sent.


CREATE OR REPLACE PROCEDURE  NEW_TEST.AUDIT.LOG_PIPELINE_SUCCESS(
  P_RUN_ID           VARCHAR,
  P_PIPELINE_CODE    VARCHAR,
  P_ENVIRONMENT      VARCHAR,
  P_ROWS_READ        NUMBER,
  P_ROWS_INSERTED    NUMBER,
  P_ROWS_UPDATED     NUMBER,
  P_ROWS_REJECTED    NUMBER,
  P_ROWS_QUARANTINED NUMBER,
  P_TESTS_PASSED     NUMBER,
  P_TESTS_FAILED     NUMBER,
  P_TESTS_WARNED     NUMBER,
  P_DQ_STATUS        VARCHAR,
  P_WATERMARK_END    TIMESTAMP_NTZ,
  P_QUERY_ID         VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_duration        NUMBER;
  v_notify_success  BOOLEAN;
  v_emails          VARCHAR;
  v_slack           VARCHAR;
BEGIN
  -- Calculate duration
  SELECT DATEDIFF('second', RUN_START_TIME, CURRENT_TIMESTAMP())
  INTO v_duration
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE RUN_ID = :P_RUN_ID;

  -- Update run log
  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET
    RUN_STATUS         = 'PASSED',
    DQ_CHECK_STATUS    = :P_DQ_STATUS,
    RUN_END_TIME       = CURRENT_TIMESTAMP(),
    RUN_DURATION_SECS  = :v_duration,
    ROWS_READ          = :P_ROWS_READ,
    ROWS_INSERTED      = :P_ROWS_INSERTED,
    ROWS_UPDATED       = :P_ROWS_UPDATED,
    ROWS_REJECTED      = :P_ROWS_REJECTED,
    ROWS_QUARANTINED   = :P_ROWS_QUARANTINED,
    TESTS_PASSED       = :P_TESTS_PASSED,
    TESTS_FAILED       = :P_TESTS_FAILED,
    TESTS_WARNED       = :P_TESTS_WARNED,
    WATERMARK_END      = :P_WATERMARK_END,
    SNOWFLAKE_QUERY_ID = :P_QUERY_ID
  WHERE RUN_ID = :P_RUN_ID;

  -- Update watermark in PIPELINE_MASTER
  IF (:P_WATERMARK_END IS NOT NULL) THEN
    CALL NEW_TEST.REFERENCE.UPDATE_WATERMARK(
      :P_PIPELINE_CODE, :P_ENVIRONMENT, :P_WATERMARK_END
    );
  END IF;

  -- Check notification config
  SELECT NOTIFY_ON_SUCCESS, NOTIFICATION_EMAILS, SLACK_CHANNEL
  INTO v_notify_success, v_emails, v_slack
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;

  IF (:v_notify_success) THEN
    CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(
      :P_RUN_ID, :P_PIPELINE_CODE, :P_ENVIRONMENT,
      'SUCCESS', :v_emails, :v_slack
    );
    UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
    SET NOTIFICATION_SENT = TRUE,
        NOTIFICATION_AT   = CURRENT_TIMESTAMP()
    WHERE RUN_ID = :P_RUN_ID;
  END IF;

  RETURN 'SUCCESS logged: ' || :P_RUN_ID;
END;
$$;

CREATE OR REPLACE PROCEDURE  NEW_TEST.AUDIT.LOG_PIPELINE_FAILURE(
  P_RUN_ID        VARCHAR,
  P_PIPELINE_CODE VARCHAR,
  P_ENVIRONMENT   VARCHAR,
  P_ERROR_CODE    VARCHAR,
  P_ERROR_MSG     VARCHAR,
  P_ERROR_STACK   VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_duration  NUMBER;
  v_emails    VARCHAR;
  v_slack     VARCHAR;
BEGIN
  SELECT DATEDIFF('second', RUN_START_TIME, CURRENT_TIMESTAMP())
  INTO v_duration
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE RUN_ID = :P_RUN_ID;

  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET
    RUN_STATUS        = 'FAILED',
    RUN_END_TIME      = CURRENT_TIMESTAMP(),
    RUN_DURATION_SECS = :v_duration,
    ERROR_CODE        = :P_ERROR_CODE,
    ERROR_MESSAGE     = :P_ERROR_MSG,
    ERROR_STACK_TRACE = :P_ERROR_STACK
  WHERE RUN_ID = :P_RUN_ID;

  -- Increment retry count
  UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
  SET
    CURRENT_RETRY_COUNT = CURRENT_RETRY_COUNT + 1,
    LAST_RETRY_AT       = CURRENT_TIMESTAMP(),
    UPDATED_AT          = CURRENT_TIMESTAMP()
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;

  -- Always notify on failure
  SELECT NOTIFICATION_EMAILS, SLACK_CHANNEL
  INTO v_emails, v_slack
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE
  AND   ENVIRONMENT   = :P_ENVIRONMENT;

  CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(
    :P_RUN_ID, :P_PIPELINE_CODE, :P_ENVIRONMENT,
    'FAILURE', :v_emails, :v_slack
  );

  UPDATE NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  SET NOTIFICATION_SENT = TRUE,
      NOTIFICATION_AT   = CURRENT_TIMESTAMP()
  WHERE RUN_ID = :P_RUN_ID;

  RETURN 'FAILURE logged: ' || :P_RUN_ID;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- EXECUTE_WITH_RETRY — Wraps any pipeline with retry logic
-- Reads config from PIPELINE_MASTER: MAX_RETRY_COUNT, RETRY_BACKOFF_SECS
-- Supports FIXED / LINEAR / EXPONENTIAL backoff
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.EXECUTE_WITH_RETRY(
  P_PIPELINE_CODE VARCHAR, P_ENVIRONMENT VARCHAR, P_SQL_TO_EXECUTE VARCHAR
) RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE
  v_max_retry NUMBER; v_current_retry NUMBER; v_backoff_secs NUMBER;
  v_backoff_type VARCHAR; v_is_active BOOLEAN;
  v_batch_id VARCHAR := UUID_STRING(); v_run_id VARCHAR;
  v_wait_secs NUMBER; v_success BOOLEAN := FALSE;
  v_last_error VARCHAR; v_attempt NUMBER := 0;
BEGIN
  SELECT IS_ACTIVE, MAX_RETRY_COUNT, CURRENT_RETRY_COUNT, RETRY_BACKOFF_SECS, RETRY_BACKOFF_TYPE
  INTO v_is_active, v_max_retry, v_current_retry, v_backoff_secs, v_backoff_type
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;

  IF (NOT v_is_active) THEN RETURN 'SKIPPED: ' || :P_PIPELINE_CODE || ' is inactive'; END IF;
  IF (:v_current_retry >= :v_max_retry) THEN RETURN 'ABORTED: Max retries exceeded for ' || :P_PIPELINE_CODE; END IF;

  LOOP
    v_attempt := v_attempt + 1;
    IF (:v_attempt > :v_max_retry + 1) THEN BREAK; END IF;

    CALL NEW_TEST.AUDIT.LOG_PIPELINE_START(
      :P_PIPELINE_CODE, :P_ENVIRONMENT, :v_batch_id, :v_attempt - 1,
      :v_attempt > 1,
      CASE WHEN :v_attempt > 1 THEN 'Retry ' || (:v_attempt - 1) || ' of ' || :v_max_retry ELSE NULL END
    ) INTO :v_run_id;

    BEGIN
      -- Execute the pipeline SQL
      EXECUTE IMMEDIATE :P_SQL_TO_EXECUTE;
      v_success := TRUE;
      UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
      SET CURRENT_RETRY_COUNT = 0, UPDATED_AT = CURRENT_TIMESTAMP()
      WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;
      CALL NEW_TEST.AUDIT.LOG_PIPELINE_SUCCESS(
        :v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT,
        NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'PASSED',
        CURRENT_TIMESTAMP(), LAST_QUERY_ID());
      BREAK;
    EXCEPTION
      WHEN OTHER THEN
        v_last_error := SQLERRM;
        CALL NEW_TEST.AUDIT.LOG_PIPELINE_FAILURE(
          :v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT,
          SQLSTATE, :v_last_error,
          'Attempt ' || :v_attempt || ' of ' || (:v_max_retry + 1));
        IF (:v_attempt <= :v_max_retry) THEN
          v_wait_secs := CASE :v_backoff_type
            WHEN 'FIXED' THEN :v_backoff_secs
            WHEN 'LINEAR' THEN :v_backoff_secs * :v_attempt
            WHEN 'EXPONENTIAL' THEN :v_backoff_secs * POW(2, :v_attempt - 1)
            ELSE :v_backoff_secs END;
          CALL SYSTEM$WAIT(:v_wait_secs, 'SECONDS');
        END IF;
    END;
  END LOOP;

  IF (NOT v_success) THEN
    RETURN 'FAILED after ' || :v_max_retry || ' retries: ' || :P_PIPELINE_CODE || ' — ' || :v_last_error;
  END IF;
  RETURN 'SUCCESS: ' || :P_PIPELINE_CODE;
EXCEPTION
  WHEN OTHER THEN
    RETURN 'EXCEPTION in EXECUTE_WITH_RETRY: ' || :P_PIPELINE_CODE || ' — ' || SQLERRM;
END;
$$;

-- ═══════════════════════════════════════════════════════════════════════════
-- RUN_DQM_CHECKS — Execute all DQ rules for a pipeline
-- Reads rules from DQM_RULES config table + VOLUME from PIPELINE_MASTER
-- Writes results to DQM_CHECK_LOG
-- ═══════════════════════════════════════════════════════════════════════════
-- Supported RULE_TYPEs:
--   NOT_NULL        — column has no NULL values
--   UNIQUE          — column has no duplicates
--   ACCEPTED_VALUES — column only contains allowed values
--                     RULE_EXPRESSION: 'ACTIVE,INACTIVE,PENDING'
--   REGEX           — column matches a pattern
--                     RULE_EXPRESSION: '^[0-9]{10}$'
--   RANGE           — value within min/max bounds
--                     RULE_EXPRESSION: '0,10000'
--   REFERENTIAL     — FK exists in parent table
--                     RULE_EXPRESSION: 'DB.SCHEMA.TABLE.COLUMN'
--   FRESHNESS       — data not stale (max hours)
--                     RULE_EXPRESSION: '720'
--   CUSTOM_SQL      — free-form SQL returning (rows_checked, rows_failed)
--                     RULE_EXPRESSION: full SQL query
--   VOLUME          — row count within tolerance (from PIPELINE_MASTER)
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.RUN_DQM_CHECKS(
  P_PIPELINE_CODE VARCHAR, P_RUN_ID VARCHAR, P_ENVIRONMENT VARCHAR
) RETURNS VARIANT LANGUAGE SQL AS
$$
DECLARE
  v_target_table VARCHAR; v_total_checks NUMBER := 0; v_passed_checks NUMBER := 0;
  v_failed_checks NUMBER := 0; v_critical_failures NUMBER := 0;
  v_rows_checked NUMBER; v_rows_failed NUMBER;
  v_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP(); v_sql VARCHAR; v_rs RESULTSET;
  v_rule_code VARCHAR; v_rule_name VARCHAR; v_col_name VARCHAR;
  v_severity VARCHAR; v_layer VARCHAR; v_domain VARCHAR; v_action VARCHAR; v_expression VARCHAR;
  v_rule_type VARCHAR;
BEGIN
  SELECT TARGET_TABLE INTO v_target_table FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;

  DECLARE
    rs_rules RESULTSET DEFAULT (
      SELECT RULE_CODE, RULE_NAME, COLUMN_NAME, SEVERITY, PIPELINE_LAYER, DOMAIN, ACTION_ON_FAIL, RULE_EXPRESSION, RULE_TYPE
      FROM NEW_TEST.REFERENCE.DQM_RULES WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND IS_ACTIVE = TRUE ORDER BY RULE_TYPE, SEVERITY DESC);
    cr_rules CURSOR FOR rs_rules;
  BEGIN
    FOR rec IN cr_rules DO
      v_rule_code := rec.RULE_CODE; v_rule_name := rec.RULE_NAME; v_col_name := rec.COLUMN_NAME;
      v_severity := rec.SEVERITY; v_layer := rec.PIPELINE_LAYER; v_domain := rec.DOMAIN;
      v_action := rec.ACTION_ON_FAIL; v_expression := rec.RULE_EXPRESSION; v_rule_type := rec.RULE_TYPE;
      v_total_checks := v_total_checks + 1;
      LET start_ms := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000;
      v_rows_checked := 0; v_rows_failed := 0;

      IF (:v_rule_type = 'NOT_NULL') THEN
        v_sql := 'SELECT COUNT(*), SUM(CASE WHEN ' || :v_col_name || ' IS NULL THEN 1 ELSE 0 END) FROM ' || :v_target_table;
      ELSEIF (:v_rule_type = 'UNIQUE') THEN
        v_sql := 'SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT ' || :v_col_name || ') FROM ' || :v_target_table;
      ELSEIF (:v_rule_type = 'ACCEPTED_VALUES') THEN
        v_sql := 'SELECT COUNT(*), SUM(CASE WHEN ' || :v_col_name || ' NOT IN (''' || REPLACE(:v_expression, ',', ''',''') || ''') THEN 1 ELSE 0 END) FROM ' || :v_target_table || ' WHERE ' || :v_col_name || ' IS NOT NULL';
      ELSEIF (:v_rule_type = 'REGEX') THEN
        v_sql := 'SELECT COUNT(*), SUM(CASE WHEN NOT REGEXP_LIKE(' || :v_col_name || ', ''' || :v_expression || ''') THEN 1 ELSE 0 END) FROM ' || :v_target_table || ' WHERE ' || :v_col_name || ' IS NOT NULL';
      ELSEIF (:v_rule_type = 'RANGE') THEN
        v_sql := 'SELECT COUNT(*), SUM(CASE WHEN ' || :v_col_name || ' < ' || SPLIT_PART(:v_expression, ',', 1) || ' OR ' || :v_col_name || ' > ' || SPLIT_PART(:v_expression, ',', 2) || ' THEN 1 ELSE 0 END) FROM ' || :v_target_table || ' WHERE ' || :v_col_name || ' IS NOT NULL';
      ELSEIF (:v_rule_type = 'REFERENTIAL') THEN
        v_sql := 'SELECT COUNT(*), SUM(CASE WHEN ' || :v_col_name || ' NOT IN (SELECT ' || SPLIT_PART(:v_expression, '.', 4) || ' FROM ' || SPLIT_PART(:v_expression, '.', 1) || '.' || SPLIT_PART(:v_expression, '.', 2) || '.' || SPLIT_PART(:v_expression, '.', 3) || ') THEN 1 ELSE 0 END) FROM ' || :v_target_table || ' WHERE ' || :v_col_name || ' IS NOT NULL';
      ELSEIF (:v_rule_type = 'FRESHNESS') THEN
        v_sql := 'SELECT 1, CASE WHEN DATEDIFF(''hour'', MAX(' || :v_col_name || '), CURRENT_TIMESTAMP()) > ' || :v_expression || ' THEN 1 ELSE 0 END FROM ' || :v_target_table;
      ELSEIF (:v_rule_type = 'CUSTOM_SQL') THEN
        v_sql := :v_expression;
      END IF;

      v_rs := (EXECUTE IMMEDIATE :v_sql);
      LET cr CURSOR FOR v_rs; OPEN cr; FETCH cr INTO v_rows_checked, v_rows_failed; CLOSE cr;
      LET exec_ms := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000 - start_ms;
      LET v_fail_pct NUMBER := CASE WHEN :v_rows_checked > 0 THEN ROUND(:v_rows_failed / :v_rows_checked * 100, 4) ELSE 0 END;

      IF (:v_rows_failed = 0) THEN
        v_passed_checks := v_passed_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_EXPRESSION, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, ACTION_TAKEN, EXECUTION_MS)
        VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, :v_layer, :v_target_table, :v_domain, :v_rule_code, :v_rule_name, :v_rule_type, :v_col_name, :v_expression, :v_severity, 'PASSED', :v_rows_checked, :v_rows_checked, 0, 0.0, 'NONE', :exec_ms);
      ELSE
        v_failed_checks := v_failed_checks + 1;
        IF (:v_severity = 'CRITICAL') THEN v_critical_failures := v_critical_failures + 1; END IF;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_EXPRESSION, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_ON_FAIL, ACTION_TAKEN, EXECUTION_MS)
        VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, :v_layer, :v_target_table, :v_domain, :v_rule_code, :v_rule_name, :v_rule_type, :v_col_name, :v_expression, :v_severity, 'FAILED', :v_rows_checked, :v_rows_checked - :v_rows_failed, :v_rows_failed, :v_fail_pct, :v_rows_failed || ' failures in ' || :v_col_name || ' (' || :v_rule_type || ')', :v_action, :v_action, :exec_ms);
      END IF;
    END FOR;
  END;

  -- ═══ VOLUME check (from PIPELINE_MASTER.EXPECTED_ROW_COUNT) ═══
  DECLARE v_expected NUMBER; v_actual NUMBER; v_tolerance NUMBER; v_variance NUMBER; v_var_rounded NUMBER; v_var_display VARCHAR; v_tol_display VARCHAR;
  BEGIN
    SELECT EXPECTED_ROW_COUNT, ROW_COUNT_TOLERANCE_PCT INTO v_expected, v_tolerance FROM NEW_TEST.REFERENCE.PIPELINE_MASTER WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;
    IF (v_expected IS NOT NULL AND v_expected > 0) THEN
      v_sql := 'SELECT COUNT(*) FROM ' || v_target_table;
      v_rs := (EXECUTE IMMEDIATE :v_sql);
      LET cv CURSOR FOR v_rs; OPEN cv; FETCH cv INTO v_actual; CLOSE cv;
      v_variance := ABS(v_actual - v_expected) / v_expected * 100;
      v_var_rounded := ROUND(v_variance, 2);
      v_var_display := 'Actual: ' || v_actual::VARCHAR || ' | Expected: ' || v_expected::VARCHAR || ' | Variance: ' || v_var_rounded::VARCHAR || '%';
      v_tol_display := v_var_display || ' | Tolerance: ' || v_tolerance::VARCHAR || '%';
      v_total_checks := v_total_checks + 1;
      IF (v_variance <= v_tolerance) THEN
        v_passed_checks := v_passed_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_TAKEN) VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, NULL, :v_target_table, NULL, 'VOLUME_CHECK', 'Row count within tolerance', 'VOLUME', 'HIGH', 'PASSED', :v_actual, :v_actual, 0, 0.0, :v_var_display, 'NONE');
      ELSE
        v_failed_checks := v_failed_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_ON_FAIL, ACTION_TAKEN) VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, NULL, :v_target_table, NULL, 'VOLUME_CHECK', 'Row count outside tolerance', 'VOLUME', 'HIGH', 'WARNING', :v_actual, 0, 0, :v_var_rounded, :v_tol_display, 'WARN_AND_CONTINUE', 'WARN_AND_CONTINUE');
      END IF;
    END IF;
  END;

  RETURN OBJECT_CONSTRUCT('pipeline_code', :P_PIPELINE_CODE, 'total_checks', v_total_checks, 'passed_checks', v_passed_checks, 'failed_checks', v_failed_checks, 'critical_failures', v_critical_failures, 'dq_status', CASE WHEN v_critical_failures > 0 THEN 'FAILED' WHEN v_failed_checks > 0 THEN 'WARNING' ELSE 'PASSED' END, 'execution_secs', DATEDIFF('second', v_start_ts, CURRENT_TIMESTAMP()));
END;
$$;
