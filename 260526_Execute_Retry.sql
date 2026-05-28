CREATE OR REPLACE PROCEDURE NEW_TEST.REFERENCE.EXECUTE_WITH_RETRY(
  P_PIPELINE_CODE  VARCHAR,
  P_ENVIRONMENT    VARCHAR,
  P_BATCH_ID       VARCHAR,
  P_INNER_SP_NAME  VARCHAR
)
RETURNS VARCHAR
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_max_retry      NUMBER;
  v_backoff_secs   NUMBER;
  v_backoff_type   VARCHAR;
  v_is_active      BOOLEAN;
  v_attempt        NUMBER := 0;
  v_success        BOOLEAN := FALSE;
  v_last_error     VARCHAR;
  v_wait_secs      NUMBER;
  v_inner_sql      VARCHAR;
BEGIN
  -- Read retry config
  SELECT IS_ACTIVE, MAX_RETRY_COUNT, RETRY_BACKOFF_SECS, RETRY_BACKOFF_TYPE
  INTO v_is_active, v_max_retry, v_backoff_secs, v_backoff_type
  FROM NEW_TEST.REFERENCE.PIPELINE_MASTER
  WHERE PIPELINE_CODE = :P_PIPELINE_CODE 
    AND ENVIRONMENT = :P_ENVIRONMENT;

  IF (NOT v_is_active) THEN 
    RETURN 'SKIPPED: ' || :P_PIPELINE_CODE || ' is inactive'; 
  END IF;

  LOOP
    IF (:v_attempt > :v_max_retry) THEN BREAK; END IF;

    BEGIN
      -- Build inner SP call with attempt number as 3rd param
      v_inner_sql := 'CALL ' || :P_INNER_SP_NAME 
                  || '(' || CHR(39) || :P_BATCH_ID || CHR(39) 
                  || ', ' || CHR(39) || :P_ENVIRONMENT || CHR(39) 
                  || ', ' || :v_attempt::VARCHAR 
                  || ')';

      EXECUTE IMMEDIATE :v_inner_sql;
      v_success := TRUE;
      BREAK;

    EXCEPTION
      WHEN OTHER THEN
        v_last_error := SQLERRM;
        
        IF (:v_attempt < :v_max_retry) THEN
          v_wait_secs := CASE :v_backoff_type
            WHEN 'FIXED' THEN :v_backoff_secs
            WHEN 'LINEAR' THEN :v_backoff_secs * (:v_attempt + 1)
            WHEN 'EXPONENTIAL' THEN :v_backoff_secs * POW(2, :v_attempt)
            ELSE :v_backoff_secs
          END;
          CALL SYSTEM$WAIT(:v_wait_secs, 'SECONDS');
        END IF;
        
        v_attempt := v_attempt + 1;
    END;
  END LOOP;

  IF (v_success) THEN
    RETURN 'SUCCESS: ' || :P_PIPELINE_CODE 
        || ' on attempt ' || (:v_attempt + 1)::VARCHAR;
  ELSE
    RETURN 'FAILED after ' || (:v_max_retry + 1)::VARCHAR 
        || ' attempts: ' || :P_PIPELINE_CODE 
        || ' - ' || :v_last_error;
  END IF;

EXCEPTION
  WHEN OTHER THEN
    RETURN 'EXCEPTION in EXECUTE_WITH_RETRY: ' || :P_PIPELINE_CODE || ' - ' || SQLERRM;
END;
$$;
------------------------------
