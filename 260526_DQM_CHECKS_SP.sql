CREATE OR REPLACE PROCEDURE "RUN_DQM_CHECKS"(
  "P_PIPELINE_CODE" VARCHAR, 
  "P_RUN_ID" VARCHAR, 
  "P_ENVIRONMENT" VARCHAR
)
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS '
DECLARE
  v_target_table VARCHAR; 
  v_total_checks NUMBER := 0; 
  v_passed_checks NUMBER := 0;
  v_failed_checks NUMBER := 0; 
  v_warned_checks NUMBER := 0;
  v_critical_failures NUMBER := 0;
  v_rows_checked NUMBER; 
  v_rows_failed NUMBER;
  v_start_ts TIMESTAMP_NTZ := CURRENT_TIMESTAMP(); 
  v_sql VARCHAR; 
  v_rs RESULTSET;
  v_rule_code VARCHAR; 
  v_rule_name VARCHAR; 
  v_col_name VARCHAR;
  v_severity VARCHAR; 
  v_layer VARCHAR; 
  v_domain VARCHAR; 
  v_action VARCHAR; 
  v_expression VARCHAR;
  v_rule_type VARCHAR;
BEGIN
  SELECT TARGET_TABLE INTO v_target_table 
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;

  DECLARE
    rs_rules RESULTSET DEFAULT (
      SELECT RULE_CODE, RULE_NAME, COLUMN_NAME, SEVERITY, PIPELINE_LAYER, DOMAIN, ACTION_ON_FAIL, RULE_EXPRESSION, RULE_TYPE
      FROM NEW_TEST.REFERENCE.DQM_RULES 
      WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND IS_ACTIVE = TRUE 
      ORDER BY RULE_TYPE, SEVERITY DESC);
    cr_rules CURSOR FOR rs_rules;
  BEGIN
    FOR rec IN cr_rules DO
      v_rule_code := rec.RULE_CODE; 
      v_rule_name := rec.RULE_NAME; 
      v_col_name := rec.COLUMN_NAME;
      v_severity := rec.SEVERITY; 
      v_layer := rec.PIPELINE_LAYER; 
      v_domain := rec.DOMAIN;
      v_action := rec.ACTION_ON_FAIL; 
      v_expression := rec.RULE_EXPRESSION; 
      v_rule_type := rec.RULE_TYPE;
      v_total_checks := v_total_checks + 1;
      LET start_ms := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000;
      v_rows_checked := 0; 
      v_rows_failed := 0;

      IF (:v_rule_type = ''NOT_NULL'') THEN
        v_sql := ''SELECT COUNT(*), SUM(CASE WHEN '' || :v_col_name || '' IS NULL THEN 1 ELSE 0 END) FROM '' || :v_target_table;
      ELSEIF (:v_rule_type = ''UNIQUE'') THEN
        v_sql := ''SELECT COUNT(*), COUNT(*) - COUNT(DISTINCT '' || :v_col_name || '') FROM '' || :v_target_table;
      ELSEIF (:v_rule_type = ''ACCEPTED_VALUES'') THEN
        v_sql := ''SELECT COUNT(*), SUM(CASE WHEN '' || :v_col_name || '' NOT IN ('''''' || REPLACE(:v_expression, '','', '''''','''''') || '''''') THEN 1 ELSE 0 END) FROM '' || :v_target_table || '' WHERE '' || :v_col_name || '' IS NOT NULL'';
      ELSEIF (:v_rule_type = ''REGEX'') THEN
        v_sql := ''SELECT COUNT(*), SUM(CASE WHEN NOT REGEXP_LIKE('' || :v_col_name || '', '''''' || :v_expression || '''''') THEN 1 ELSE 0 END) FROM '' || :v_target_table || '' WHERE '' || :v_col_name || '' IS NOT NULL'';
      ELSEIF (:v_rule_type = ''RANGE'') THEN
        v_sql := ''SELECT COUNT(*), SUM(CASE WHEN '' || :v_col_name || '' < '' || SPLIT_PART(:v_expression, '','', 1) || '' OR '' || :v_col_name || '' > '' || SPLIT_PART(:v_expression, '','', 2) || '' THEN 1 ELSE 0 END) FROM '' || :v_target_table || '' WHERE '' || :v_col_name || '' IS NOT NULL'';
      ELSEIF (:v_rule_type = ''REFERENTIAL'') THEN
        v_sql := ''SELECT COUNT(*), SUM(CASE WHEN '' || :v_col_name || '' NOT IN (SELECT '' || SPLIT_PART(:v_expression, ''.'', 4) || '' FROM '' || SPLIT_PART(:v_expression, ''.'', 1) || ''.'' || SPLIT_PART(:v_expression, ''.'', 2) || ''.'' || SPLIT_PART(:v_expression, ''.'', 3) || '') THEN 1 ELSE 0 END) FROM '' || :v_target_table || '' WHERE '' || :v_col_name || '' IS NOT NULL'';
      ELSEIF (:v_rule_type = ''FRESHNESS'') THEN
        v_sql := ''SELECT 1, CASE WHEN DATEDIFF(''''hour'''', MAX('' || :v_col_name || ''), CURRENT_TIMESTAMP()) > '' || :v_expression || '' THEN 1 ELSE 0 END FROM '' || :v_target_table;
      ELSEIF (:v_rule_type = ''CUSTOM_SQL'') THEN
        v_sql := :v_expression;
      END IF;

      v_rs := (EXECUTE IMMEDIATE :v_sql);
      LET cr CURSOR FOR v_rs; 
      OPEN cr; 
      FETCH cr INTO v_rows_checked, v_rows_failed; 
      CLOSE cr;
      LET exec_ms := EXTRACT(EPOCH FROM CURRENT_TIMESTAMP()) * 1000 - start_ms;
      LET v_fail_pct NUMBER := CASE WHEN :v_rows_checked > 0 THEN ROUND(:v_rows_failed / :v_rows_checked * 100, 4) ELSE 0 END;

      IF (:v_rows_failed = 0) THEN
        -- Rule passed
        v_passed_checks := v_passed_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_EXPRESSION, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, ACTION_TAKEN, EXECUTION_MS)
        VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, :v_layer, :v_target_table, :v_domain, :v_rule_code, :v_rule_name, :v_rule_type, :v_col_name, :v_expression, :v_severity, ''PASSED'', :v_rows_checked, :v_rows_checked, 0, 0.0, ''NONE'', :exec_ms);
      ELSE
        -- Rule failed: route to FAILED or WARNING based on severity
        IF (:v_severity = ''CRITICAL'') THEN
          -- Critical severity -> FAILED bucket
          v_failed_checks := v_failed_checks + 1;
          v_critical_failures := v_critical_failures + 1;
          INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_EXPRESSION, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_ON_FAIL, ACTION_TAKEN, EXECUTION_MS)
          VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, :v_layer, :v_target_table, :v_domain, :v_rule_code, :v_rule_name, :v_rule_type, :v_col_name, :v_expression, :v_severity, ''FAILED'', :v_rows_checked, :v_rows_checked - :v_rows_failed, :v_rows_failed, :v_fail_pct, :v_rows_failed || '' failures in '' || :v_col_name || '' ('' || :v_rule_type || '')'', :v_action, :v_action, :exec_ms);
        ELSE
          -- Non-critical severity -> WARNING bucket
          v_warned_checks := v_warned_checks + 1;
          INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, COLUMN_NAME, RULE_EXPRESSION, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_ON_FAIL, ACTION_TAKEN, EXECUTION_MS)
          VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, :v_layer, :v_target_table, :v_domain, :v_rule_code, :v_rule_name, :v_rule_type, :v_col_name, :v_expression, :v_severity, ''WARNING'', :v_rows_checked, :v_rows_checked - :v_rows_failed, :v_rows_failed, :v_fail_pct, :v_rows_failed || '' failures in '' || :v_col_name || '' ('' || :v_rule_type || '')'', :v_action, :v_action, :exec_ms);
        END IF;
      END IF;
    END FOR;
  END;

  -- Volume check
  DECLARE 
    v_expected NUMBER; 
    v_actual NUMBER; 
    v_tolerance NUMBER; 
    v_variance NUMBER; 
    v_var_rounded NUMBER; 
    v_var_display VARCHAR; 
    v_tol_display VARCHAR;
  BEGIN
    SELECT EXPECTED_ROW_COUNT, ROW_COUNT_TOLERANCE_PCT 
    INTO v_expected, v_tolerance 
    FROM NEW_TEST.REFERENCE.PIPELINE_MASTER 
    WHERE PIPELINE_CODE = :P_PIPELINE_CODE AND ENVIRONMENT = :P_ENVIRONMENT;
    
    IF (v_expected IS NOT NULL AND v_expected > 0) THEN
      v_sql := ''SELECT COUNT(*) FROM '' || v_target_table;
      v_rs := (EXECUTE IMMEDIATE :v_sql);
      LET cv CURSOR FOR v_rs; 
      OPEN cv; 
      FETCH cv INTO v_actual; 
      CLOSE cv;
      v_variance := ABS(v_actual - v_expected) / v_expected * 100;
      v_var_rounded := ROUND(v_variance, 2);
      v_var_display := ''Actual: '' || v_actual::VARCHAR || '' | Expected: '' || v_expected::VARCHAR || '' | Variance: '' || v_var_rounded::VARCHAR || ''%'';
      v_tol_display := v_var_display || '' | Tolerance: '' || v_tolerance::VARCHAR || ''%'';
      v_total_checks := v_total_checks + 1;
      
      IF (v_variance <= v_tolerance) THEN
        -- Within tolerance -> PASSED
        v_passed_checks := v_passed_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_TAKEN) 
        VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, NULL, :v_target_table, NULL, ''VOLUME_CHECK'', ''Row count within tolerance'', ''VOLUME'', ''HIGH'', ''PASSED'', :v_actual, :v_actual, 0, 0.0, :v_var_display, ''NONE'');
      ELSE
        -- Outside tolerance -> WARNING (not FAILED, since action is WARN_AND_CONTINUE)
        v_warned_checks := v_warned_checks + 1;
        INSERT INTO NEW_TEST.AUDIT.DQM_CHECK_LOG (RUN_ID, ENVIRONMENT, PIPELINE_CODE, PIPELINE_LAYER, TARGET_TABLE, DOMAIN, RULE_CODE, RULE_NAME, RULE_TYPE, SEVERITY, CHECK_STATUS, ROWS_CHECKED, ROWS_PASSED, ROWS_FAILED, FAILURE_RATE_PCT, VIOLATION_SAMPLE, ACTION_ON_FAIL, ACTION_TAKEN) 
        VALUES (:P_RUN_ID, :P_ENVIRONMENT, :P_PIPELINE_CODE, NULL, :v_target_table, NULL, ''VOLUME_CHECK'', ''Row count outside tolerance'', ''VOLUME'', ''HIGH'', ''WARNING'', :v_actual, 0, 0, :v_var_rounded, :v_tol_display, ''WARN_AND_CONTINUE'', ''WARN_AND_CONTINUE'');
      END IF;
    END IF;
  END;

  RETURN OBJECT_CONSTRUCT(
    ''pipeline_code'', :P_PIPELINE_CODE, 
    ''total_checks'', v_total_checks, 
    ''passed_checks'', v_passed_checks, 
    ''failed_checks'', v_failed_checks,
    ''warned_checks'', v_warned_checks,
    ''critical_failures'', v_critical_failures, 
    ''dq_status'', CASE 
      WHEN v_critical_failures > 0 THEN ''FAILED'' 
      WHEN v_warned_checks > 0 THEN ''WARNING'' 
      ELSE ''PASSED'' 
    END, 
    ''execution_secs'', DATEDIFF(''second'', v_start_ts, CURRENT_TIMESTAMP())
  );
END;
';