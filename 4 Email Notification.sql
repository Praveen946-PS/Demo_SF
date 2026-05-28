-- Create email notification integration
CREATE OR REPLACE NOTIFICATION INTEGRATION CDW_EMAIL_INTEGRATION
  TYPE    = EMAIL
  ENABLED = TRUE;

-- Grant to your working role
GRANT USAGE ON INTEGRATION CDW_EMAIL_INTEGRATION  TO ROLE SYSADMIN;

-- Email and Slack notifications
-- Called on success, failure, and warning

-- ═══════════════════════════════════════════════════
-- SEND_NOTIFICATION — complete drop-in replacement
-- Same signature as attached SP
-- Same parameters — nothing else changes
-- ═══════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE  NEW_TEST.REFERENCE.SEND_NOTIFICATION(
  P_RUN_ID        VARCHAR,
  P_PIPELINE_CODE VARCHAR,
  P_ENVIRONMENT   VARCHAR,
  P_STATUS        VARCHAR,
  P_EMAILS        VARCHAR,
  P_SLACK_CHANNEL VARCHAR    -- kept for compatibility
                             -- Slack handled by Notebook separately
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_pipeline      VARCHAR;
  v_domain        VARCHAR;
  v_layer         VARCHAR;
  v_priority      VARCHAR;
  v_rows_inserted NUMBER;
  v_rows_expected NUMBER;
  v_duration      NUMBER;
  v_error_code    VARCHAR;
  v_error_msg     VARCHAR;
  v_tests_passed  NUMBER;
  v_tests_failed  NUMBER;
  v_retry         NUMBER;
  v_max_retry     NUMBER;
  v_run_date      VARCHAR;
  v_subject       VARCHAR;
  v_body          VARCHAR;
  v_status_label  VARCHAR;
  v_status_icon   VARCHAR;
BEGIN

  -- ─── STEP 1: Read run details from PIPELINE_RUN_LOG ───────
  -- Same query as the attached Python SP
  SELECT
    log.PIPELINE_CODE,
    log.DOMAIN,
    log.PIPELINE_LAYER,
    log.RUN_DATE::VARCHAR,
    COALESCE(log.ROWS_INSERTED,     0),
    COALESCE(log.ROWS_EXPECTED,     0),
    COALESCE(log.RUN_DURATION_SECS, 0),
    COALESCE(log.ERROR_CODE,        ''),
    COALESCE(log.ERROR_MESSAGE,     'None'),
    COALESCE(log.TESTS_PASSED,      0),
    COALESCE(log.TESTS_FAILED,      0),
    COALESCE(log.RETRY_ATTEMPT,     0),
    COALESCE(pm.ALERT_PRIORITY,     'P1'),
    COALESCE(pm.MAX_RETRY_COUNT,    3)
  INTO
    v_pipeline,
    v_domain,
    v_layer,
    v_run_date,
    v_rows_inserted,
    v_rows_expected,
    v_duration,
    v_error_code,
    v_error_msg,
    v_tests_passed,
    v_tests_failed,
    v_retry,
    v_priority,
    v_max_retry
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG log
  LEFT JOIN NEW_TEST.REFERENCE.PIPELINE_MASTER pm
    ON  log.PIPELINE_CODE = pm.PIPELINE_CODE
    AND log.ENVIRONMENT   = pm.ENVIRONMENT
  WHERE log.RUN_ID = :P_RUN_ID;

  -- If no record found — use parameters directly
  IF (v_pipeline IS NULL) THEN
    v_pipeline := :P_PIPELINE_CODE;
    v_domain   := 'UNKNOWN';
    v_layer    := 'UNKNOWN';
    v_priority := 'P1';
    v_run_date := CURRENT_DATE()::VARCHAR;
  END IF;

  -- ─── STEP 2: Map status to label and icon ─────────────────
  -- Same mapping as attached Python SP status_map dict
  v_status_label := CASE :P_STATUS
    WHEN 'SUCCESS'             THEN 'Pipeline Succeeded'
    WHEN 'FAILURE'             THEN 'Pipeline Failed'
    WHEN 'WARNING'             THEN 'Pipeline Warning'
    WHEN 'MAX_RETRY_EXHAUSTED' THEN 'Max Retry Exhausted'
    ELSE :P_STATUS
  END;

  v_status_icon := CASE :P_STATUS
    WHEN 'SUCCESS'             THEN '[SUCCESS]'
    WHEN 'FAILURE'             THEN '[FAILED]'
    WHEN 'WARNING'             THEN '[WARNING]'
    WHEN 'MAX_RETRY_EXHAUSTED' THEN '[EXHAUSTED]'
    ELSE '[INFO]'
  END;

  -- ─── STEP 3: Build subject line ───────────────────────────
  -- Same content as attached SP subject variable
  v_subject :=
    '[' || v_priority || '] Praxis CDW — '
    || v_pipeline
    || ' ' || v_status_label
    || ' | ' || v_domain
    || ' | ' || v_layer
    || ' | ' || :P_ENVIRONMENT;

  -- ─── STEP 4: Build email body ──────────────────────────────
  -- Same fields as the HTML table in attached SP
  -- Plain text format — same information
  v_body :=
    v_status_icon || ' PRAXIS CDW PIPELINE NOTIFICATION'    || CHR(10) ||
    REPEAT('=', 50)                                         || CHR(10) ||
    'Status:        ' || v_status_label                     || CHR(10) ||
    'Pipeline:      ' || v_pipeline                         || CHR(10) ||
    'Domain:        ' || v_domain                           || CHR(10) ||
    'Layer:         ' || v_layer                            || CHR(10) ||
    'Environment:   ' || :P_ENVIRONMENT                     || CHR(10) ||
    'Run Date:      ' || v_run_date                         || CHR(10) ||
    REPEAT('-', 50)                                         || CHR(10) ||
    'Rows Inserted: ' || v_rows_inserted::VARCHAR           || CHR(10) ||
    'Rows Expected: ' || v_rows_expected::VARCHAR           || CHR(10) ||
    'Duration:      ' || v_duration::VARCHAR || ' seconds'  || CHR(10) ||
    'Tests Passed:  ' || v_tests_passed::VARCHAR            || CHR(10) ||
    'Tests Failed:  ' || v_tests_failed::VARCHAR            || CHR(10) ||
    'Retry Attempt: ' || v_retry::VARCHAR
                      || ' of ' || v_max_retry::VARCHAR     || CHR(10) ||
    REPEAT('-', 50)                                         || CHR(10) ||

    -- Error section — same as error_row in attached SP
    CASE WHEN :P_STATUS IN ('FAILURE', 'MAX_RETRY_EXHAUSTED')
      THEN
        'Error Code:    ' || v_error_code                   || CHR(10) ||
        'Error Message: ' || v_error_msg                    || CHR(10) ||
        REPEAT('-', 50)                                     || CHR(10)
      ELSE ''
    END ||

    'Run ID:        ' || :P_RUN_ID                          || CHR(10) ||
    REPEAT('=', 50)                                         || CHR(10) ||
    'Praxis CDF Automated Pipeline Monitor'                 || CHR(10) ||
    'Timestamp: ' || CURRENT_TIMESTAMP()::VARCHAR;

  -- ─── STEP 5: Send email ────────────────────────────────────
  -- Replaces smtplib.SMTP_SSL block in attached SP
  -- SYSTEM$SEND_EMAIL uses Snowflake native email service
  -- No Gmail account, no SMTP password, no network rules needed
  IF (:P_EMAILS IS NOT NULL AND LENGTH(TRIM(:P_EMAILS)) > 0) THEN
    CALL SYSTEM$SEND_EMAIL(
      'CDW_EMAIL_INTEGRATION',   -- notification integration
      :P_EMAILS,                 -- same comma-separated emails
      :v_subject,                -- same subject
      :v_body                    -- same content — plain text
    );
  END IF;

  -- ─── P_SLACK_CHANNEL is accepted but not sent here ─────────
  -- Slack is handled by Snowflake Notebook on schedule
  -- Notebook reads PIPELINE_RUN_LOG directly
  -- No webhook secret needed in this SP

  RETURN 'Notification sent: '
      || v_status_label
      || ' for ' || v_pipeline
      || ' — Email: '
      || CASE WHEN :P_EMAILS IS NOT NULL THEN 'sent' ELSE 'skipped' END;

EXCEPTION
  WHEN OTHER THEN
    -- Never let notification failure break the pipeline
    RETURN 'Notification failed (non-blocking): ' || SQLERRM;
END;
$$;


-- Master procedure — ties all parts together
-- Called by Snowflake Tasks or MuleSoft

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.RUN_PIPELINE(P_PIPELINE_CODE VARCHAR, P_ENVIRONMENT VARCHAR)
RETURNS VARCHAR LANGUAGE SQL AS
$$
DECLARE 
  v_config VARIANT; v_is_active BOOLEAN; v_run_id VARCHAR; v_batch_id VARCHAR := UUID_STRING();
  v_dqm_result VARIANT; v_dq_status VARCHAR; v_error_msg VARCHAR; v_depends_on VARCHAR;
  v_pending_deps NUMBER; v_component VARCHAR; v_rows_inserted NUMBER; v_target_table VARCHAR;
  v_notify_success BOOLEAN; v_notify_failure BOOLEAN; v_notify_warning BOOLEAN;
  v_emails VARCHAR; v_slack_channel VARCHAR;
BEGIN
  CALL NEW_TEST.REFERENCE.GET_PIPELINE_CONFIG(:P_PIPELINE_CODE, :P_ENVIRONMENT) INTO :v_config;
  v_is_active := v_config:is_active::BOOLEAN;
  v_depends_on := v_config:depends_on_pipelines::VARCHAR;
  v_target_table := v_config:target_table::VARCHAR;
  v_notify_success := COALESCE(v_config:notify_on_success::BOOLEAN, FALSE);
  v_notify_failure := COALESCE(v_config:notify_on_failure::BOOLEAN, TRUE);
  v_notify_warning := COALESCE(v_config:notify_on_warning::BOOLEAN, TRUE);
  v_emails := v_config:notification_emails::VARCHAR;
  v_slack_channel := v_config:slack_channel::VARCHAR;

  -- STEP 2: Active check
  IF (NOT v_is_active) THEN
    RETURN 'SKIPPED: ' || :P_PIPELINE_CODE || ' inactive';
  END IF;

  -- STEP 3: Dependency check
  IF (v_depends_on IS NOT NULL) THEN
    SELECT COUNT(*) INTO :v_pending_deps
    FROM TABLE(SPLIT_TO_TABLE(:v_depends_on, ',')) AS deps
    WHERE NOT EXISTS (
      SELECT 1 FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG r
      WHERE r.PIPELINE_CODE = TRIM(deps.VALUE) AND r.RUN_DATE = CURRENT_DATE()
        AND r.RUN_STATUS = 'PASSED' AND r.ENVIRONMENT = :P_ENVIRONMENT
    );
    IF (v_pending_deps > 0) THEN
      RETURN 'WAITING: Dependencies not complete for ' || :P_PIPELINE_CODE;
    END IF;
  END IF;

  -- STEP 4: Log start
  CALL NEW_TEST.AUDIT.LOG_PIPELINE_START(:P_PIPELINE_CODE, :P_ENVIRONMENT, :v_batch_id, 0, FALSE, NULL) INTO :v_run_id;

  -- STEP 5: Execute with retry
  SELECT COALESCE(COMPONENT_NAME, 'NEW_TEST.REFERENCE.INGEST_' || REPLACE(:P_PIPELINE_CODE, '-', '_'))
  INTO v_component FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;

  BEGIN
    CALL NEW_TEST.REFERENCE.EXECUTE_WITH_RETRY(:P_PIPELINE_CODE, :P_ENVIRONMENT,
      'CALL ' || :v_component || '(''' || :v_run_id || ''', ''' || :P_ENVIRONMENT || ''')');
  EXCEPTION
    WHEN OTHER THEN
      v_error_msg := SQLERRM;
      IF (v_notify_failure) THEN
        CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(:v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT, 'FAILURE', :v_emails, :v_slack_channel);
      END IF;
      RETURN 'FAILED: ' || :P_PIPELINE_CODE || ' — ' || :v_error_msg;
  END;

  -- STEP 6: Run DQM checks (wrapped — errors don't roll back data)
  BEGIN
    CALL NEW_TEST.REFERENCE.RUN_DQM_CHECKS(:P_PIPELINE_CODE, :v_run_id, :P_ENVIRONMENT) INTO :v_dqm_result;
    v_dq_status := v_dqm_result:dq_status::VARCHAR;
  EXCEPTION
    WHEN OTHER THEN
      v_dq_status := 'SKIPPED';
  END;

  -- STEP 7: Capture row count (wrapped — errors don't roll back data)
  IF (v_target_table IS NOT NULL) THEN
    BEGIN
      EXECUTE IMMEDIATE 'SELECT COUNT(*) AS CNT FROM ' || :v_target_table;
      v_rows_inserted := (SELECT "CNT" FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
    EXCEPTION
      WHEN OTHER THEN
        v_rows_inserted := NULL;
    END;
  END IF;

  CALL NEW_TEST.AUDIT.LOG_PIPELINE_SUCCESS(:v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT,
    :v_rows_inserted, :v_rows_inserted, NULL, NULL, NULL, NULL, NULL, 0, :v_dq_status,
    CURRENT_TIMESTAMP(), LAST_QUERY_ID());

  -- STEP 8: Critical DQ failure?
  IF (:v_dq_status = 'FAILED') THEN
    CALL NEW_TEST.AUDIT.LOG_PIPELINE_FAILURE(:v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT,
      'DQM_CRITICAL', 'Critical DQ checks failed', 'Critical DQ violations detected');
    IF (v_notify_failure) THEN
      CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(:v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT, 'FAILURE', :v_emails, :v_slack_channel);
    END IF;
    RETURN 'DQM CRITICAL: ' || :P_PIPELINE_CODE;
  END IF;

  -- FINAL: Notify on success
  IF (v_notify_success) THEN
    CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(:v_run_id, :P_PIPELINE_CODE, :P_ENVIRONMENT, 'SUCCESS', :v_emails, :v_slack_channel);
  END IF;

  RETURN 'SUCCESS: ' || :P_PIPELINE_CODE || ' | DQ: ' || :v_dq_status;
END;
$$;


-- Current allowed list
DESC NOTIFICATION INTEGRATION CDW_EMAIL_INTEGRATION;

-- Current PIPELINE_MASTER emails
SELECT PIPELINE_CODE, NOTIFICATION_EMAILS
FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
WHERE PIPELINE_CODE IN ('VEEVA_CRM_INBOUND', 'SP_SILVER_HCP', 'SP_GOLD_HCP');

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET COMPONENT_NAME = 'NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP',
    COMPONENT_TYPE = 'STORED_PROCEDURE',
    NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'praveen@pharmsight.com',
    EXPECTED_ROW_COUNT = 5,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'SP_SILVER_HCP' AND ENVIRONMENT = 'DEV';
 
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET COMPONENT_NAME = 'NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP',
    COMPONENT_TYPE = 'STORED_PROCEDURE',
    NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'praveen@pharmsight.com',
    EXPECTED_ROW_COUNT = 5,
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'SP_GOLD_HCP' AND ENVIRONMENT = 'DEV';
 
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET NOTIFY_ON_SUCCESS = TRUE, NOTIFY_ON_FAILURE = TRUE,
    NOTIFICATION_EMAILS = 'praveen@pharmsight.com',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE PIPELINE_CODE = 'VEEVA_CRM_INBOUND' AND ENVIRONMENT = 'DEV';
---------------------
UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET NOTIFICATION_EMAILS = 'praveen.931410@gmail.com'
WHERE PIPELINE_CODE IN ('VEEVA_CRM_INBOUND', 'SP_SILVER_HCP', 'SP_GOLD_HCP')
AND ENVIRONMENT = 'DEV';
-------------------------------------------
//250526 1335 

ALTER INTEGRATION CDW_EMAIL_INTEGRATION
SET ALLOWED_RECIPIENTS = ('praveen.931410@gmail.com', 'praveen@pharmisight.com');

ALTER INTEGRATION CDW_EMAIL_INTEGRATION
SET ALLOWED_RECIPIENTS = ('praveen.931410@gmail.com', 'praveen@pharmisight.com'),
    ALLOWED_RECIPIENT_EMAIL_DOMAINS = ('gmail.com', 'pharmisight.com');

    SHOW USERS;

    CREATE USER PRAVEEN_PHARMISIGHT
  EMAIL = 'praveen@pharmisight.com'
  MUST_CHANGE_PASSWORD = FALSE
  DISPLAY_NAME = 'Praveen Pharmisight';

  ALTER INTEGRATION CDW_EMAIL_INTEGRATION
SET ALLOWED_RECIPIENTS = ('praveen.931410@gmail.com', 'praveen@pharmisight.com');

ALTER USER PRAVEEN_PHARMISIGHT
SET EMAIL = 'praveen@pharmisight.com';

DESC USER PRAVEEN_PHARMISIGHT;

-- Revert email allowlist
ALTER INTEGRATION CDW_EMAIL_INTEGRATION
SET ALLOWED_RECIPIENTS = ('praveen.931410@gmail.com');

UPDATE NEW_TEST.REFERENCE.PIPELINE_MASTER
SET NOTIFICATION_EMAILS = 'praveen.931410@gmail.com',
    UPDATED_AT = CURRENT_TIMESTAMP()
WHERE ENVIRONMENT = 'DEV';

DROP USER PRAVEEN_PHARMISIGHT;