-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 1 — RUN_DOMAIN stored procedure
-- ─────────────────────────────────────────────────────────────────────────
-- Called by each domain Task (DOMAIN_HCP, DOMAIN_CLAIMS, etc.)
-- Reads BATCH_ID from PIPELINE_RUN_LOG (written by MASTER_PIPELINE_TRIGGER)
-- so all domain pipelines share one BATCH_ID per nightly run.
-- Iterates PIPELINE_MASTER: Silver first, then Gold, within domain.
-- Calls RUN_PIPELINE for each active pipeline in sequence.
-- Domain isolation: one domain failing does NOT stop other domain Tasks.
-- ═══════════════════════════════════════════════════════════════════════════
CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.RUN_DOMAIN(
  P_DOMAIN      VARCHAR,
  P_ENVIRONMENT VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  c_pipelines CURSOR FOR
    SELECT PIPELINE_CODE
    FROM   NEW_TEST.REFERENCE.PIPELINE_MASTER
    WHERE  DOMAIN         = :P_DOMAIN
    AND    ENVIRONMENT    = :P_ENVIRONMENT
    AND    IS_ACTIVE      = TRUE
    AND    PIPELINE_LAYER IN ('SILVER', 'GOLD')
    ORDER BY
      CASE PIPELINE_LAYER WHEN 'SILVER' THEN 1 WHEN 'GOLD' THEN 2 END,
      PIPELINE_CODE;

  v_pipeline_code VARCHAR;
  v_result        VARCHAR;
  v_failed        NUMBER  := 0;
  v_passed        NUMBER  := 0;
  v_summary       VARCHAR := '';
  v_batch_id      VARCHAR;
BEGIN

  -- Read BATCH_ID from MASTER_PIPELINE_TRIGGER log row
  -- Snowflake Tasks cannot pass variables to child Tasks directly
  -- PIPELINE_RUN_LOG is the handoff point for the shared BATCH_ID
  SELECT BATCH_ID INTO v_batch_id
  FROM   NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE  PIPELINE_CODE = 'MASTER_PIPELINE_TRIGGER'
  AND    ENVIRONMENT   = :P_ENVIRONMENT
  AND    RUN_DATE      = CURRENT_DATE()
  ORDER  BY RUN_START_TIME DESC
  LIMIT  1;

  -- Fallback if root row not found
  IF (v_batch_id IS NULL) THEN
    v_batch_id := UUID_STRING();
  END IF;

  -- Iterate Silver then Gold pipelines for this domain
  FOR rec IN c_pipelines DO
    v_pipeline_code := rec.PIPELINE_CODE;

    CALL NEW_TEST.REFERENCE.RUN_PIPELINE(
      :v_pipeline_code,
      :P_ENVIRONMENT
    ) INTO v_result;

    IF (v_result LIKE 'FAILED%' OR v_result LIKE 'DQM CRITICAL%') THEN
      v_failed  := v_failed + 1;
      v_summary := v_summary
                || CHR(10) || '  FAILED  : ' || v_pipeline_code
                || ' — ' || v_result;
    ELSE
      v_passed  := v_passed + 1;
      v_summary := v_summary
                || CHR(10) || '  PASSED  : ' || v_pipeline_code;
    END IF;

  END FOR;

  IF (v_failed > 0) THEN
    RETURN 'DOMAIN ' || :P_DOMAIN || ' COMPLETED WITH FAILURES — '
        || v_passed::VARCHAR || ' passed, '
        || v_failed::VARCHAR || ' failed' || v_summary;
  ELSE
    RETURN 'DOMAIN ' || :P_DOMAIN || ' PASSED — '
        || v_passed::VARCHAR || ' pipelines completed' || v_summary;
  END IF;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'DOMAIN ' || :P_DOMAIN || ' EXCEPTION — ' || SQLERRM;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 2 — PIPELINE_COMPLETION stored procedure
-- ─────────────────────────────────────────────────────────────────────────
-- Called by PIPELINE_COMPLETION Task after all 4 domain Tasks finish.
-- Reads today's Gold PIPELINE_RUN_LOG rows to determine overall status.
-- Writes DOMAIN='ALL', PIPELINE_LAYER='GOLD' summary row.
-- MuleSoft polls this row every 2 minutes to detect completion.
-- PASSED → MuleSoft fires outbound feeds (IC, Veeva segments, Power BI).
-- ═══════════════════════════════════════════════════════════════════════════

CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.PIPELINE_COMPLETION(
  P_ENVIRONMENT VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
AS
$$
DECLARE
  v_total_pipelines     NUMBER;
  v_passed_pipelines    NUMBER;
  v_failed_pipelines    NUMBER;
  v_warning_pipelines   NUMBER;
  v_final_status        VARCHAR;
  v_run_id              VARCHAR;
  v_batch_id            VARCHAR;
  v_notification_emails VARCHAR;
BEGIN

  SELECT
    COUNT(*),
    SUM(CASE WHEN RUN_STATUS = 'PASSED'  THEN 1 ELSE 0 END),
    SUM(CASE WHEN RUN_STATUS = 'FAILED'  THEN 1 ELSE 0 END),
    SUM(CASE WHEN RUN_STATUS = 'WARNING' THEN 1 ELSE 0 END),
    MAX(BATCH_ID)
  INTO
    v_total_pipelines, v_passed_pipelines,
    v_failed_pipelines, v_warning_pipelines,
    v_batch_id
  FROM NEW_TEST.AUDIT.PIPELINE_RUN_LOG
  WHERE PIPELINE_LAYER = 'GOLD'
  AND   ENVIRONMENT    = :P_ENVIRONMENT
  AND   RUN_DATE       = CURRENT_DATE();

  v_final_status := CASE
    WHEN v_failed_pipelines  > 0 THEN 'FAILED'
    WHEN v_warning_pipelines > 0 THEN 'WARNING'
    ELSE 'PASSED'
  END;

  -- Pre-generate RUN_ID — RETURNING INTO not supported in Snowflake SQL SPs
  v_run_id := UUID_STRING();

  -- Write MuleSoft polling row
  INSERT INTO NEW_TEST.AUDIT.PIPELINE_RUN_LOG (
    RUN_ID, BATCH_ID, ENVIRONMENT, DOMAIN, PIPELINE_CODE,
    PIPELINE_LAYER, SOURCE_CODE, TARGET_TABLE,
    RUN_STATUS, DQ_CHECK_STATUS, RUN_TRIGGER_TYPE,
    RUN_DATE, RUN_START_TIME, RUN_END_TIME,
    RUN_DURATION_SECS, ROWS_INSERTED,
    TRIGGERED_BY, NOTIFICATION_SENT
  )
  VALUES (
    :v_run_id, :v_batch_id, :P_ENVIRONMENT,
    'ALL', 'PIPELINE_COMPLETION', 'GOLD',
    'SNOWFLAKE_TASK', 'PIPELINE_RUN_LOG',
    :v_final_status, :v_final_status, 'SCHEDULED',
    CURRENT_DATE(), CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(),
    0, v_total_pipelines,
    'SNOWFLAKE_TASK', FALSE
  );

  SELECT LISTAGG(DISTINCT NOTIFICATION_EMAILS, ',')
  INTO   v_notification_emails
  FROM   NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE  ENVIRONMENT       = :P_ENVIRONMENT
  AND    PIPELINE_LAYER    IN ('GOLD', 'OUTBOUND')
  AND    NOTIFY_ON_SUCCESS = TRUE;

  IF (:v_final_status = 'PASSED' AND v_notification_emails IS NOT NULL) THEN
    CALL NEW_TEST.REFERENCE.SEND_NOTIFICATION(
      :v_run_id, 'PIPELINE_COMPLETION', :P_ENVIRONMENT,
      'SUCCESS', :v_notification_emails, '#cdf-alerts'
    );
  END IF;

  RETURN 'PIPELINE_COMPLETION: ' || :v_final_status
      || ' | Total Gold pipelines: ' || v_total_pipelines::VARCHAR
      || ' | Passed: '               || v_passed_pipelines::VARCHAR
      || ' | Failed: '               || v_failed_pipelines::VARCHAR
      || ' | Warnings: '             || v_warning_pipelines::VARCHAR;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'PIPELINE_COMPLETION EXCEPTION — ' || SQLERRM;
END;
$$;


-- ═══════════════════════════════════════════════════════════════════════════
-- BLOCK 3 — CREATE ALL 6 TASKS
-- ─────────────────────────────────────────────────────────────────────────
-- ALL Tasks are created in SUSPENDED state automatically by Snowflake.
-- Do NOT run EXECUTE TASK yet — child Tasks must be created AND resumed first.
-- Warehouse: COMPUTE_WH — change to your actual warehouse name.
-- ═══════════════════════════════════════════════════════════════════════════

-- Task 1: MASTER_PIPELINE_TRIGGER (ROOT)
-- No AFTER clause — triggered on-demand by MuleSoft or manually.
-- Calls LOG_PIPELINE_START which generates BATCH_ID and writes RUNNING row.
-- Return value from LOG_PIPELINE_START is intentionally ignored by the Task.
-- Child Tasks read BATCH_ID back from PIPELINE_RUN_LOG.

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'Root task. Triggered by MuleSoft or manually via EXECUTE TASK. Calls LOG_PIPELINE_START — generates BATCH_ID, writes RUNNING row. Child Tasks fire automatically after this Task SUCCEEDS.'
AS
CALL NEW_TEST.AUDIT.LOG_PIPELINE_START(
  'MASTER_PIPELINE_TRIGGER',
  'DEV',
  UUID_STRING(),
  0,
  FALSE,
  NULL
);


-- Task 2: DOMAIN_HCP
-- Fires after MASTER_PIPELINE_TRIGGER succeeds — parallel with other domains.
-- Runs Silver then Gold pipelines for HCP domain:
--   DBT_SILVER_HCP → SP_BUILD_SILVER_HCP
--   DBT_GOLD_HCP   → SP_BUILD_GOLD_HCP

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.DOMAIN_HCP
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'HCP domain Silver then Gold. Fires after MASTER_PIPELINE_TRIGGER.'
  AFTER     NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER
AS
CALL NEW_TEST.REFERENCE.RUN_DOMAIN('HCP', 'DEV');


-- Task 3: DOMAIN_CLAIMS
-- Fires after MASTER_PIPELINE_TRIGGER — parallel with other domains.

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.DOMAIN_CLAIMS
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'Claims domain Silver then Gold. Fires after MASTER_PIPELINE_TRIGGER.'
  AFTER     NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER
AS
CALL NEW_TEST.REFERENCE.RUN_DOMAIN('CLAIMS', 'DEV');


-- Task 4: DOMAIN_PATIENT
-- Fires after MASTER_PIPELINE_TRIGGER — parallel with other domains.

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.DOMAIN_PATIENT
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'Patient domain Silver. Fires after MASTER_PIPELINE_TRIGGER.'
  AFTER     NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER
AS
CALL NEW_TEST.REFERENCE.RUN_DOMAIN('PATIENT', 'DEV');


-- Task 5: DOMAIN_COMMERCIAL
-- Fires after MASTER_PIPELINE_TRIGGER — parallel with other domains.

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.DOMAIN_COMMERCIAL
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'Commercial domain Silver. Fires after MASTER_PIPELINE_TRIGGER.'
  AFTER     NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER
AS
CALL NEW_TEST.REFERENCE.RUN_DOMAIN('COMMERCIAL', 'DEV');


-- Task 6: PIPELINE_COMPLETION (LEAF)
-- Fires only after ALL FOUR domain Tasks complete (pass or fail).
-- Writes DOMAIN='ALL' summary row — MuleSoft polls for this.

CREATE OR REPLACE TASK NEW_TEST.REFERENCE.PIPELINE_COMPLETION
  WAREHOUSE = COMPUTE_WH
  COMMENT   = 'Leaf task. Fires after all 4 domains complete.
               Writes DOMAIN=ALL PASSED row. MuleSoft polls this to fire outbound feeds.'
  AFTER     NEW_TEST.REFERENCE.DOMAIN_HCP,
            NEW_TEST.REFERENCE.DOMAIN_CLAIMS,
            NEW_TEST.REFERENCE.DOMAIN_PATIENT,
            NEW_TEST.REFERENCE.DOMAIN_COMMERCIAL
AS
CALL NEW_TEST.REFERENCE.PIPELINE_COMPLETION('DEV');

