CREATE OR REPLACE PROCEDURE "SEND_NOTIFICATION"("P_RUN_ID" VARCHAR, "P_PIPELINE_CODE" VARCHAR, "P_ENVIRONMENT" VARCHAR, "P_STATUS" VARCHAR, "P_EMAILS" VARCHAR, "P_SLACK_CHANNEL" VARCHAR)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_pipeline      VARCHAR;
  v_domain        VARCHAR;
  v_layer         VARCHAR;
  v_priority      VARCHAR;
  v_rows_inserted NUMBER;
  v_rows_updated  NUMBER;
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

  SELECT
    log.PIPELINE_CODE,
    log.DOMAIN,
    log.PIPELINE_LAYER,
    log.RUN_DATE::VARCHAR,
    COALESCE(log.ROWS_INSERTED,     0),
    COALESCE(log.ROWS_UPDATED,      0),
    COALESCE(log.ROWS_EXPECTED,     0),
    COALESCE(log.RUN_DURATION_SECS, 0),
    COALESCE(log.ERROR_CODE,        ''''),
    COALESCE(log.ERROR_MESSAGE,     ''None''),
    COALESCE(log.TESTS_PASSED,      0),
    COALESCE(log.TESTS_FAILED,      0),
    COALESCE(log.RETRY_ATTEMPT,     0),
    COALESCE(pm.ALERT_PRIORITY,     ''P1''),
    COALESCE(pm.MAX_RETRY_COUNT,    3)
  INTO
    v_pipeline,
    v_domain,
    v_layer,
    v_run_date,
    v_rows_inserted,
    v_rows_updated,
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

  IF (v_pipeline IS NULL) THEN
    v_pipeline := :P_PIPELINE_CODE;
    v_domain   := ''UNKNOWN'';
    v_layer    := ''UNKNOWN'';
    v_priority := ''P1'';
    v_run_date := CURRENT_DATE()::VARCHAR;
  END IF;

  v_status_label := CASE :P_STATUS
    WHEN ''SUCCESS''             THEN ''Pipeline Succeeded''
    WHEN ''FAILURE''             THEN ''Pipeline Failed''
    WHEN ''WARNING''             THEN ''Pipeline Warning''
    WHEN ''MAX_RETRY_EXHAUSTED'' THEN ''Max Retry Exhausted''
    ELSE :P_STATUS
  END;

  v_status_icon := CASE :P_STATUS
    WHEN ''SUCCESS''             THEN ''[SUCCESS]''
    WHEN ''FAILURE''             THEN ''[FAILED]''
    WHEN ''WARNING''             THEN ''[WARNING]''
    WHEN ''MAX_RETRY_EXHAUSTED'' THEN ''[EXHAUSTED]''
    ELSE ''[INFO]''
  END;

  v_subject :=
    ''['' || v_priority || ''] Praxis CDW — ''
    || v_pipeline
    || '' '' || v_status_label
    || '' | '' || v_domain
    || '' | '' || v_layer
    || '' | '' || :P_ENVIRONMENT;

  v_body :=
    v_status_icon || '' PRAXIS CDW PIPELINE NOTIFICATION''    || CHR(10) ||
    REPEAT(''='', 50)                                         || CHR(10) ||
    ''Status:        '' || v_status_label                     || CHR(10) ||
    ''Pipeline:      '' || v_pipeline                         || CHR(10) ||
    ''Domain:        '' || v_domain                           || CHR(10) ||
    ''Layer:         '' || v_layer                            || CHR(10) ||
    ''Environment:   '' || :P_ENVIRONMENT                     || CHR(10) ||
    ''Run Date:      '' || v_run_date                         || CHR(10) ||
    REPEAT(''-'', 50)                                         || CHR(10) ||
    ''Rows Inserted: '' || v_rows_inserted::VARCHAR           || CHR(10) ||
    ''Rows Updated:  '' || v_rows_updated::VARCHAR            || CHR(10) ||
    ''Rows Expected: '' || v_rows_expected::VARCHAR           || CHR(10) ||
    ''Duration:      '' || v_duration::VARCHAR || '' seconds''  || CHR(10) ||
    ''Tests Passed:  '' || v_tests_passed::VARCHAR            || CHR(10) ||
    ''Tests Failed:  '' || v_tests_failed::VARCHAR            || CHR(10) ||
    ''Retry Attempt: '' || v_retry::VARCHAR
                      || '' of '' || v_max_retry::VARCHAR     || CHR(10) ||
    REPEAT(''-'', 50)                                         || CHR(10) ||

    CASE WHEN :P_STATUS IN (''FAILURE'', ''MAX_RETRY_EXHAUSTED'')
      THEN
        ''Error Code:    '' || v_error_code                   || CHR(10) ||
        ''Error Message: '' || v_error_msg                    || CHR(10) ||
        REPEAT(''-'', 50)                                     || CHR(10)
      ELSE ''''
    END ||

    ''Run ID:        '' || :P_RUN_ID                          || CHR(10) ||
    REPEAT(''='', 50)                                         || CHR(10) ||
    ''Praxis CDF Automated Pipeline Monitor''                 || CHR(10) ||
    ''Timestamp: '' || CURRENT_TIMESTAMP()::VARCHAR;

  IF (:P_EMAILS IS NOT NULL AND LENGTH(TRIM(:P_EMAILS)) > 0) THEN
    CALL SYSTEM$SEND_EMAIL(
      ''CDW_EMAIL_INTEGRATION'',
      :P_EMAILS,
      :v_subject,
      :v_body
    );
  END IF;

  RETURN ''Notification sent: ''
      || v_status_label
      || '' for '' || v_pipeline
      || '' — Email: ''
      || CASE WHEN :P_EMAILS IS NOT NULL THEN ''sent'' ELSE ''skipped'' END;

EXCEPTION
  WHEN OTHER THEN
    RETURN ''Notification failed (non-blocking): '' || SQLERRM;
END;
';

---------------------------------
-- Email notification SP
-- Sends Snowflake email via CDW_EMAIL_INTEGRATION
-- Reads run details from PIPELINE_RUN_LOG
-- Shows failed/warned rule codes and names when DQ checks fail



CREATE OR REPLACE PROCEDURE "SEND_NOTIFICATION"(
  "P_RUN_ID" VARCHAR, 
  "P_PIPELINE_CODE" VARCHAR, 
  "P_ENVIRONMENT" VARCHAR, 
  "P_STATUS" VARCHAR, 
  "P_EMAILS" VARCHAR, 
  "P_SLACK_CHANNEL" VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  -- Pipeline metadata
  v_pipeline      VARCHAR;
  v_domain        VARCHAR;
  v_layer         VARCHAR;
  v_priority      VARCHAR;
  v_run_date      VARCHAR;

  -- Row counts
  v_rows_inserted NUMBER;
  v_rows_updated  NUMBER;
  v_rows_expected NUMBER;

  -- Run metrics
  v_duration      NUMBER;
  v_error_code    VARCHAR;
  v_error_msg     VARCHAR;

  -- DQ test counts
  v_tests_passed  NUMBER;
  v_tests_failed  NUMBER;
  v_tests_warned  NUMBER;

  -- Failed/warned rules (formatted as multi-line strings)
  v_failed_rules  VARCHAR;
  v_warned_rules  VARCHAR;

  -- Retry tracking
  v_retry         NUMBER;
  v_max_retry     NUMBER;

  -- Email output
  v_subject       VARCHAR;
  v_body          VARCHAR;
  v_status_label  VARCHAR;
  v_status_icon   VARCHAR;
BEGIN

  --  Read run details from audit log
  SELECT
    log.PIPELINE_CODE,
    log.DOMAIN,
    log.PIPELINE_LAYER,
    log.RUN_DATE::VARCHAR,
    COALESCE(log.ROWS_INSERTED,     0),
    COALESCE(log.ROWS_UPDATED,      0),
    COALESCE(log.ROWS_EXPECTED,     0),
    COALESCE(log.RUN_DURATION_SECS, 0),
    COALESCE(log.ERROR_CODE,        ''''),
    COALESCE(log.ERROR_MESSAGE,     ''None''),
    COALESCE(log.TESTS_PASSED,      0),
    COALESCE(log.TESTS_FAILED,      0),
    COALESCE(log.TESTS_WARNED,      0),
    COALESCE(log.RETRY_ATTEMPT,     0),
    COALESCE(pm.ALERT_PRIORITY,     ''P1''),
    COALESCE(pm.MAX_RETRY_COUNT,    3)
  INTO
    v_pipeline, v_domain, v_layer, v_run_date,
    v_rows_inserted, v_rows_updated, v_rows_expected, v_duration,
    v_error_code, v_error_msg,
    v_tests_passed, v_tests_failed, v_tests_warned,
    v_retry, v_priority, v_max_retry
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG log
  LEFT JOIN NEW_TEST.REFERENCE.PIPELINE_MASTER pm
    ON log.PIPELINE_CODE = pm.PIPELINE_CODE
    AND log.ENVIRONMENT = pm.ENVIRONMENT
  WHERE log.RUN_ID = :P_RUN_ID;

  -- Fallback if audit row not found
  IF (v_pipeline IS NULL) THEN
    v_pipeline := :P_PIPELINE_CODE;
    v_domain   := ''UNKNOWN'';
    v_layer    := ''UNKNOWN'';
    v_priority := ''P1'';
    v_run_date := CURRENT_DATE()::VARCHAR;
  END IF;

--  Get failed rules from DQM_CHECK_LOG
SELECT LISTAGG(RULE_CODE || '' - '' || RULE_NAME, '', '')
       WITHIN GROUP (ORDER BY RULE_CODE)
INTO v_failed_rules
FROM NEW_TEST.AUDIT.DQM_CHECK_LOG
WHERE RUN_ID = :P_RUN_ID
AND CHECK_STATUS = ''FAILED'';

-- Get warned rules from DQM_CHECK_LOG
SELECT LISTAGG(RULE_CODE || '' - '' || RULE_NAME, '', '')
       WITHIN GROUP (ORDER BY RULE_CODE)
INTO v_warned_rules
FROM NEW_TEST.AUDIT.DQM_CHECK_LOG
WHERE RUN_ID = :P_RUN_ID
AND CHECK_STATUS = ''WARNING'';

  --  Map status to label and icon
  v_status_label := CASE :P_STATUS
    WHEN ''SUCCESS''             THEN ''Pipeline Succeeded''
    WHEN ''FAILURE''             THEN ''Pipeline Failed''
    WHEN ''WARNING''             THEN ''Pipeline Warning''
    WHEN ''MAX_RETRY_EXHAUSTED'' THEN ''Max Retry Exhausted''
    ELSE :P_STATUS
  END;

  v_status_icon := CASE :P_STATUS
    WHEN ''SUCCESS''             THEN ''[SUCCESS]''
    WHEN ''FAILURE''             THEN ''[FAILED]''
    WHEN ''WARNING''             THEN ''[WARNING]''
    WHEN ''MAX_RETRY_EXHAUSTED'' THEN ''[EXHAUSTED]''
    ELSE ''[INFO]''
  END;

  --  Build subject line
  v_subject :=
    ''['' || v_priority || ''] Praxis CDW - ''
    || v_pipeline
    || '' '' || v_status_label
    || '' | '' || v_domain
    || '' | '' || v_layer
    || '' | '' || :P_ENVIRONMENT;

  --  Build email body
  v_body :=
    v_status_icon || '' PRAXIS CDW PIPELINE NOTIFICATION''    || CHR(10) ||
    REPEAT(''='', 50)                                         || CHR(10) ||
    ''Status:        '' || v_status_label                     || CHR(10) ||
    ''Pipeline:      '' || v_pipeline                         || CHR(10) ||
    ''Domain:        '' || v_domain                           || CHR(10) ||
    ''Layer:         '' || v_layer                            || CHR(10) ||
    ''Environment:   '' || :P_ENVIRONMENT                     || CHR(10) ||
    ''Run Date:      '' || v_run_date                         || CHR(10) ||
    REPEAT(''-'', 50)                                         || CHR(10) ||
    ''Rows Inserted: '' || v_rows_inserted::VARCHAR           || CHR(10) ||
    ''Rows Updated:  '' || v_rows_updated::VARCHAR            || CHR(10) ||
    ''Rows Expected: '' || v_rows_expected::VARCHAR           || CHR(10) ||
    ''Duration:      '' || v_duration::VARCHAR || '' seconds''  || CHR(10) ||
    ''Tests Passed:  '' || v_tests_passed::VARCHAR            || CHR(10) ||
    ''Tests Failed:  '' || v_tests_failed::VARCHAR            || CHR(10) ||
    ''Tests Warned:  '' || v_tests_warned::VARCHAR            || CHR(10) ||
    ''Retry Attempt: '' || v_retry::VARCHAR
                      || '' of '' || v_max_retry::VARCHAR     || CHR(10) ||
    REPEAT(''-'', 50)                                         || CHR(10) ||

    -- Show failed rules only when DQ has failures
    CASE WHEN v_failed_rules IS NOT NULL THEN
      ''Failed Rules:  '' || v_failed_rules || CHR(10) ||
      REPEAT(''-'', 50)                     || CHR(10)
    ELSE ''''
    END ||

    -- Show warned rules only when DQ has warnings
    CASE WHEN v_warned_rules IS NOT NULL THEN
      ''Warned Rules:  '' || v_warned_rules || CHR(10) ||
      REPEAT(''-'', 50)                     || CHR(10)
    ELSE ''''
    END ||

    -- Show error details only when pipeline failed
    CASE WHEN :P_STATUS IN (''FAILURE'', ''MAX_RETRY_EXHAUSTED'')
      THEN
        ''Error Code:    '' || v_error_code  || CHR(10) ||
        ''Error Message: '' || v_error_msg   || CHR(10) ||
        REPEAT(''-'', 50)                    || CHR(10)
      ELSE ''''
    END ||

    ''Run ID:        '' || :P_RUN_ID                          || CHR(10) ||
    REPEAT(''='', 50)                                         || CHR(10) ||
    ''Praxis CDF Automated Pipeline Monitor''                 || CHR(10) ||
    ''Timestamp: '' || CURRENT_TIMESTAMP()::VARCHAR;

  --  Send email via Snowflake native integration
  IF (:P_EMAILS IS NOT NULL AND LENGTH(TRIM(:P_EMAILS)) > 0) THEN
    CALL SYSTEM$SEND_EMAIL(
      ''CDW_EMAIL_INTEGRATION'',
      :P_EMAILS,
      :v_subject,
      :v_body
    );
  END IF;

  RETURN ''Notification sent: '' || v_status_label
      || '' for '' || v_pipeline
      || '' - Email: ''
      || CASE WHEN :P_EMAILS IS NOT NULL THEN ''sent'' ELSE ''skipped'' END;

EXCEPTION
  WHEN OTHER THEN
    -- Never let notification failure break the pipeline
    RETURN ''Notification failed: '' || SQLERRM;
END;
';

SELECT GET_DDL('PROCEDURE', 
  'NEW_TEST.REFERENCE.RUN_DQM_CHECKS(VARCHAR,VARCHAR,VARCHAR)');