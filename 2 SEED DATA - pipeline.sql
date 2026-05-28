-- ═══════════════════════════════════════════════════════════════
-- SEED: All confirmed Praxis CDF pipelines
-- Run once after table creation
-- ═══════════════════════════════════════════════════════════════

--TRUNCATE TABLE NEW_TEST.REFERENCE.PIPELINE_MASTER;
-- SELECT * FROM NEW_TEST.REFERENCE.PIPELINE_MASTER;

INSERT INTO NEW_TEST.REFERENCE.PIPELINE_MASTER (
  PIPELINE_CODE,
  PIPELINE_NAME,
  ENVIRONMENT,
  DOMAIN,
  PIPELINE_LAYER,
  PIPELINE_TYPE,
  SOURCE_CODE,
  TARGET_TABLE,
  IS_ACTIVE,
  SCHEDULE_TYPE,
  SCHEDULE_CRON,
  WATERMARK_COLUMN,
  WATERMARK_TYPE,
  WATERMARK_CURRENT,
  WATERMARK_LOOKBACK_HRS,
  LOAD_TYPE,
  BATCH_SIZE,
  EXPECTED_ROW_COUNT,
  ROW_COUNT_TOLERANCE_PCT,
  MAX_RETRY_COUNT,
  RETRY_BACKOFF_SECS,
  RETRY_BACKOFF_TYPE,
  NOTIFY_ON_SUCCESS,
  NOTIFY_ON_FAILURE,
  NOTIFY_ON_WARNING,
  NOTIFICATION_EMAILS,
  SLACK_CHANNEL,
  ALERT_PRIORITY,
  DEPENDS_ON_PIPELINES,
  COMPONENT_TYPE,
  COMPONENT_NAME,
  TASK_NAME,
  FIA_REFERENCE,
  SCHEMA_VERSION,
  OWNER
)
VALUES

-- ─────────────────────────────────────────────────────────────
-- BRONZE — INBOUND PIPELINES
-- ─────────────────────────────────────────────────────────────

-- 0. MASTER_PIPELINE_TRIGGER — orchestration root
(
  'MASTER_PIPELINE_TRIGGER',
  'Master Pipeline Trigger',
  'DEV',
  'ALL', 'ORCHESTRATION', 'TASK',
  'SNOWFLAKE_TASK',
  'NEW_TEST.AUDIT.PIPELINE_RUN_LOG',
  TRUE,
  NULL, NULL,
  NULL, NULL, NULL, 0,
  NULL, NULL, NULL, NULL,
  1, 60, 'FIXED',
  FALSE, TRUE, FALSE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  NULL, 'P1',
  NULL,
  'TASK', 'NEW_TEST.REFERENCE.MASTER_PIPELINE_TRIGGER',
  'MASTER_PIPELINE_TRIGGER', NULL,
  'v1.0', 'MURALI'
),

-- 1. Veeva CRM — Call Activity
(
  'VEEVA_CRM_INBOUND',
  'Veeva CRM Call Activity Daily Ingestion',
  'DEV',
  'HCP', 'BRONZE', 'INBOUND',
  'VEEVA_CRM',
  'NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS',
  TRUE,
  'DAILY', '0 23 * * *',
  'LastModifiedDate', 'TIMESTAMP',
  DATEADD(DAY, -7, CURRENT_TIMESTAMP()), 2,
  'INCREMENTAL', 10000, 10000, 20.00,
  3, 300, 'EXPONENTIAL',
  TRUE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  NULL,
  'TABLE', 'RAW_VEEVA_CRM_CALLS',
  'DOMAIN_HCP',
  'https://confluence/FIA-VEEVA-CRM-001',
  'v1.0', 'MURALI'
),

-- 2. Veeva Network — Bridge File (HCP MDM)
(
  'VEEVA_NETWORK_INBOUND',
  'Veeva Network Bridge File Daily Ingestion',
  'DEV',
  'HCP', 'BRONZE', 'INBOUND',
  'VEEVA_NETWORK',
  'NEW_TEST.BRONZE.RAW_VEEVA_NETWORK',
  FALSE,
  'DAILY', '0 22 * * *',
  'BRIDGE_DATE', 'DATE',
  DATEADD(DAY, -7, CURRENT_TIMESTAMP()), 0,
  'INCREMENTAL', 5000, 50000, 10.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  NULL,
  'TABLE', 'RAW_VEEVA_NETWORK',
  'DOMAIN_HCP',
  'https://confluence/FIA-VEEVA-NETWORK-001',
  'v1.0', 'MURALI'
),

-- 3. Veeva Align — Territory Alignment
(
  'VEEVA_ALIGN_INBOUND',
  'Veeva Align Territory Alignment Monthly Ingestion',
  'DEV',
  'HCP', 'BRONZE', 'INBOUND',
  'VEEVA_ALIGN',
  'NEW_TEST.BRONZE.RAW_VEEVA_ALIGN',
  FALSE,
  'MONTHLY', '0 22 1 * *',
  'EFFECTIVE_DATE', 'DATE',
  DATEADD(DAY, -32, CURRENT_TIMESTAMP()), 0,
  'FULL_REFRESH', 5000, 5000, 15.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P1',
  NULL,
  'TABLE', 'RAW_VEEVA_ALIGN',
  'DOMAIN_HCP',
  'https://confluence/FIA-VEEVA-ALIGN-001',
  'v1.0', 'MURALI'
),

-- 4. IQVIA Claims — Weekly File (S3 / Snowpipe)
(
  'IQVIA_CLAIMS_INBOUND',
  'IQVIA Weekly RX Claims File Ingestion',
  'DEV',
  'CLAIMS', 'BRONZE', 'INBOUND',
  'IQVIA',
  'NEW_TEST.BRONZE.RAW_IQVIA_CLAIMS',
  FALSE,
  'WEEKLY', '0 23 * * 0',
  'FILE_DATE', 'FILE_DATE',
  DATEADD(DAY, -8, CURRENT_TIMESTAMP()), 0,
  'APPEND_ONLY', 50000, 2000000, 25.00,
  2, 600, 'LINEAR',
  TRUE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  NULL,
  'TABLE', 'RAW_IQVIA_CLAIMS',
  'DOMAIN_CLAIMS',
  'https://confluence/FIA-IQVIA-001',
  'v1.0', 'MURALI'
),

-- 5. Salesforce CRM — Patient Services (PHI tokenised)
(
  'SALESFORCE_INBOUND',
  'Salesforce Life Sciences Patient CRM Daily Ingestion',
  'DEV',
  'PATIENT', 'BRONZE', 'INBOUND',
  'SALESFORCE',
  'NEW_TEST.BRONZE.RAW_SALESFORCE_CRM',
  FALSE,
  'DAILY', '0 23 * * *',
  'SystemModstamp', 'TIMESTAMP',
  DATEADD(DAY, -7, CURRENT_TIMESTAMP()), 1,
  'INCREMENTAL', 5000, 5000, 15.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  NULL,
  'TABLE', 'RAW_SALESFORCE_CRM',
  'DOMAIN_PATIENT',
  'https://confluence/FIA-SFSLSC-001',
  'v1.0', 'MURALI'
),

-- 6. Orsini — Specialty Pharmacy (SFTP / PHI safe)
(
  'ORSINI_INBOUND',
  'Orsini Specialty Pharmacy SFTP Daily Ingestion',
  'DEV',
  'PATIENT', 'BRONZE', 'INBOUND',
  'ORSINI',
  'NEW_TEST.BRONZE.RAW_ORSINI_SHIPMENTS',
  FALSE,
  'DAILY', '0 22 * * *',
  'SHIPMENT_DATE', 'DATE',
  DATEADD(DAY, -7, CURRENT_TIMESTAMP()), 0,
  'INCREMENTAL', 5000, 2000, 20.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  NULL,
  'TABLE', 'RAW_ORSINI_SHIPMENTS',
  'DOMAIN_PATIENT',
  'https://confluence/FIA-ORSINI-001',
  'v1.0', 'MURALI'
),

-- 7. MMIT Payer Spine — Monthly File
(
  'PAYER_SPINE_INBOUND',
  'MMIT Payer Spine Monthly Formulary Ingestion',
  'DEV',
  'CLAIMS', 'BRONZE', 'INBOUND',
  'MMIT_PAYER',
  'NEW_TEST.BRONZE.RAW_PAYER_SPINE',
  FALSE,
  'MONTHLY', '0 23 1 * *',
  'EFFECTIVE_DATE', 'DATE',
  DATEADD(DAY, -32, CURRENT_TIMESTAMP()), 0,
  'FULL_REFRESH', 10000, 50000, 15.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P1',
  NULL,
  'TABLE', 'RAW_PAYER_SPINE',
  'DOMAIN_CLAIMS',
  'https://confluence/FIA-PAYER-001',
  'v1.0', 'CHIRAG'
),

-- 8. MarTech (Vi / Eversana) — Digital Events
(
  'MARTECH_INBOUND',
  'MarTech Vi Eversana Digital Events Daily Ingestion',
  'DEV',
  'COMMERCIAL', 'BRONZE', 'INBOUND',
  'MARTECH_VI',
  'NEW_TEST.BRONZE.RAW_MARTECH_EVENTS',
  FALSE,
  'DAILY', '0 23 * * *',
  'EVENT_TIMESTAMP', 'TIMESTAMP',
  DATEADD(DAY, -7, CURRENT_TIMESTAMP()), 1,
  'INCREMENTAL', 10000, 45000, 20.00,
  3, 300, 'EXPONENTIAL',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P1',
  NULL,
  'TABLE', 'RAW_MARTECH_EVENTS',
  'DOMAIN_COMMERCIAL',
  'https://confluence/FIA-MARTECH-001',
  'v1.0', 'CHIRAG'
),

-- ─────────────────────────────────────────────────────────────
-- SILVER — SP TRANSFORMATION PIPELINES
-- ─────────────────────────────────────────────────────────────

-- 9. Silver — HCP Domain
(
  'SP_SILVER_HCP',
  'SP Silver HCP Domain Transformation',
  'DEV',
  'HCP', 'SILVER', 'TRANSFORMATION',
  'VEEVA_CRM',
  'NEW_TEST.SILVER.HCP_MASTER',
  TRUE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 50000, 10.00,
  2, 180, 'FIXED',
  TRUE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'VEEVA_CRM_INBOUND',
  'STORED_PROCEDURE', 'NEW_TEST.REFERENCE.SP_BUILD_SILVER_HCP',
  'DOMAIN_HCP', NULL,
  'v1.0', 'CHIRAG'
),

-- 10. Silver — Claims Domain
(
  'SP_SILVER_CLAIMS',
  'SP Silver Claims Domain Transformation',
  'DEV',
  'CLAIMS', 'SILVER', 'TRANSFORMATION',
  'IQVIA',
  'NEW_TEST.SILVER.RX_CLAIMS',
  FALSE,
  'WEEKLY', NULL,
  NULL, NULL, NULL, 0,
  'INCREMENTAL', NULL, 2000000, 25.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'IQVIA_CLAIMS_INBOUND,PAYER_SPINE_INBOUND',
  'TABLE', 'RX_CLAIMS',
  'DOMAIN_CLAIMS', NULL,
  'v1.0', 'CHIRAG'
),

-- 11. Silver — Patient Domain
(
  'SP_SILVER_PATIENT',
  'SP Silver Patient Domain Transformation',
  'DEV',
  'PATIENT', 'SILVER', 'TRANSFORMATION',
  'SALESFORCE',
  'NEW_TEST.SILVER.PATIENT_JOURNEY',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'INCREMENTAL', NULL, 5000, 15.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SALESFORCE_INBOUND,ORSINI_INBOUND',
  'TABLE', 'PATIENT_JOURNEY',
  'DOMAIN_PATIENT', NULL,
  'v1.0', 'CHIRAG'
),

-- 12. Silver — Commercial Domain
(
  'SP_SILVER_COMMERCIAL',
  'SP Silver Commercial Domain Transformation',
  'DEV',
  'COMMERCIAL', 'SILVER', 'TRANSFORMATION',
  'MARTECH_VI',
  'NEW_TEST.SILVER.MARTECH_EVENTS',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'INCREMENTAL', NULL, 45000, 20.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P1',
  'MARTECH_INBOUND,VEEVA_CRM_INBOUND',
  'TABLE', 'MARTECH_EVENTS',
  'DOMAIN_COMMERCIAL', NULL,
  'v1.0', 'CHIRAG'
),

-- ─────────────────────────────────────────────────────────────
-- GOLD — SP ANALYTICS BUILD PIPELINES
-- ─────────────────────────────────────────────────────────────

-- 13. Gold — HCP Dimension
(
  'SP_GOLD_HCP',
  'SP Gold HCP Dimension Build',
  'DEV',
  'HCP', 'GOLD', 'TRANSFORMATION',
  'SILVER',
  'NEW_TEST.GOLD.DIM_HCP',
  TRUE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 50000, 10.00,
  2, 180, 'FIXED',
  TRUE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SP_SILVER_HCP',
  'STORED_PROCEDURE', 'NEW_TEST.REFERENCE.SP_BUILD_GOLD_HCP',
  'DOMAIN_HCP', NULL,
  'v1.0', 'CHIRAG'
),

-- 14. Gold — Call Activity Fact
(
  'SP_GOLD_CALLS',
  'SP Gold Call Activity Fact Build',
  'DEV',
  'HCP', 'GOLD', 'TRANSFORMATION',
  'SILVER',
  'NEW_TEST.GOLD.FACT_CALL_ACTIVITY',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 10000, 20.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SP_SILVER_HCP',
  'TABLE', 'FACT_CALL_ACTIVITY',
  'DOMAIN_HCP', NULL,
  'v1.0', 'CHIRAG'
),

-- 15. Gold — RX Trends Fact
(
  'SP_GOLD_RX',
  'SP Gold RX Trends Fact Build',
  'DEV',
  'CLAIMS', 'GOLD', 'TRANSFORMATION',
  'SILVER',
  'NEW_TEST.GOLD.FACT_RX_TRENDS',
  FALSE,
  'WEEKLY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 2000000, 25.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SP_SILVER_CLAIMS,SP_SILVER_HCP',
  'TABLE', 'FACT_RX_TRENDS',
  'DOMAIN_CLAIMS', NULL,
  'v1.0', 'CHIRAG'
),

-- 16. Gold — IC Calculation Fact
(
  'SP_GOLD_IC',
  'SP Gold IC Calculation Fact Build',
  'DEV',
  'HCP', 'GOLD', 'TRANSFORMATION',
  'SILVER',
  'NEW_TEST.GOLD.FACT_IC_CALCULATION',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 500, 10.00,
  2, 180, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SP_GOLD_CALLS,SP_GOLD_RX',
  'TABLE', 'FACT_IC_CALCULATION',
  'DOMAIN_HCP', NULL,
  'v1.0', 'CHIRAG'
),

-- ─────────────────────────────────────────────────────────────
-- OUTBOUND PIPELINES
-- ─────────────────────────────────────────────────────────────

-- 17. Outbound — IC File to Ambit
(
  'IC_TO_AMBIT',
  'IC Calculation File Push to Ambit IC Platform',
  'DEV',
  'ALL', 'OUTBOUND', 'OUTBOUND',
  'GOLD',
  'AMBIT_IC_PLATFORM',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 500, 10.00,
  2, 300, 'FIXED',
  TRUE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P0',
  'SP_GOLD_IC',
  NULL, NULL, NULL, NULL,
  'v1.0', 'MURALI'
),

-- 18. Outbound — HCP Segments to Veeva CRM
(
  'SEGMENTS_TO_VEEVA',
  'HCP Prescriber Segments Push to Veeva CRM',
  'DEV',
  'ALL', 'OUTBOUND', 'OUTBOUND',
  'GOLD',
  'VEEVA_CRM',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, 5000, 15.00,
  2, 300, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-alerts', 'P1',
  'SP_GOLD_HCP',
  NULL, NULL, NULL, NULL,
  'v1.0', 'MURALI'
),

-- 19. Outbound — Power BI Dataset Refresh
(
  'POWERBI_REFRESH',
  'Power BI Dataset Refresh Trigger',
  'DEV',
  'ALL', 'OUTBOUND', 'OUTBOUND',
  'GOLD',
  'POWER_BI',
  FALSE,
  'DAILY', NULL,
  NULL, NULL, NULL, 0,
  'FULL_REFRESH', NULL, NULL, NULL,
  3, 60, 'FIXED',
  FALSE, TRUE, TRUE,
  'murali.v@pharmsight.com, chirag.maheshwari@pharmsight.com, nitin.riyal@pharmsight.com',
  '#cdf-ops', 'P1',
  'SP_GOLD_CALLS,SP_GOLD_RX,SP_GOLD_IC',
  NULL, NULL, NULL, NULL,
  'v1.0', 'PRAVEEN'
);
---


-- ═══════════════════════════════════════════════════════════════
-- BRONZE SAMPLE DATA — 20 more rows with recent dates
-- Run after creating RAW_VEEVA_CRM_CALLS table (file 6, STEP 1)
-- ═══════════════════════════════════════════════════════════════

INSERT INTO NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS (
  CALL_ID, NPI, CALL_DATE, PRODUCT_CODE, TERRITORY_CODE,
  REP_ID, CALL_TYPE, CALL_OUTCOME, SAMPLE_UNITS, NOTES,
  _AUDIT_RUN_ID, _AUDIT_BATCH_ID, _AUDIT_SOURCE_SYSTEM,
  _AUDIT_SOURCE_FILE, _AUDIT_ROW_HASH, _AUDIT_ENV
)
SELECT * FROM (
  SELECT 'CALL-011','1234567890','2026-05-15'::DATE,'RELU_001','NE-001','REP-101','FACE_TO_FACE','COMPLETED',3,'Follow-up — patient outcomes improving','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-15',SHA2('CALL-011',256),'DEV'
  UNION ALL SELECT 'CALL-012','2345678901','2026-05-15'::DATE,'ULIXA_001','SE-002','REP-102','VIRTUAL','COMPLETED',0,'Discussed real-world evidence data','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-15',SHA2('CALL-012',256),'DEV'
  UNION ALL SELECT 'CALL-013','3456789012','2026-05-14'::DATE,'RELU_001','MW-003','REP-103','PHONE','COMPLETED',0,'Confirmed interest — scheduling lunch-and-learn','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-14',SHA2('CALL-013',256),'DEV'
  UNION ALL SELECT 'CALL-014','4567890123','2026-05-14'::DATE,'ULIXA_001','SW-004','REP-104','FACE_TO_FACE','LEFT_SAMPLE',2,'Left samples with office manager','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-14',SHA2('CALL-014',256),'DEV'
  UNION ALL SELECT 'CALL-015','5678901234','2026-05-13'::DATE,'RELU_001','NW-005','REP-105','FACE_TO_FACE','COMPLETED',1,'Presented Phase III results','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-13',SHA2('CALL-015',256),'DEV'
  UNION ALL SELECT 'CALL-016','6789012345','2026-05-13'::DATE,'ULIXA_001','NE-001','REP-101','VIRTUAL','COMPLETED',0,'Reviewed formulary status update','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-13',SHA2('CALL-016',256),'DEV'
  UNION ALL SELECT 'CALL-017','7890123456','2026-05-12'::DATE,'RELU_001','SE-002','REP-102','FACE_TO_FACE','COMPLETED',2,'High prescriber — quarterly review','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-12',SHA2('CALL-017',256),'DEV'
  UNION ALL SELECT 'CALL-018','8901234567','2026-05-12'::DATE,'ULIXA_001','MW-003','REP-103','PHONE','NO_SEE',0,'Dr unavailable — rescheduled for Friday','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-12',SHA2('CALL-018',256),'DEV'
  UNION ALL SELECT 'CALL-019','9012345678','2026-05-11'::DATE,'RELU_001','SW-004','REP-104','FACE_TO_FACE','COMPLETED',3,'Presented copay assistance program','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-11',SHA2('CALL-019',256),'DEV'
  UNION ALL SELECT 'CALL-020','1234567890','2026-05-11'::DATE,'ULIXA_001','NE-001','REP-101','VIRTUAL','COMPLETED',0,'Second product discussion with Dr Singh','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-11',SHA2('CALL-020',256),'DEV'
  UNION ALL SELECT 'CALL-021','2345678901','2026-05-16'::DATE,'RELU_001','SE-002','REP-102','FACE_TO_FACE','COMPLETED',2,'New data on treatment adherence','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-16',SHA2('CALL-021',256),'DEV'
  UNION ALL SELECT 'CALL-022','3456789012','2026-05-16'::DATE,'ULIXA_001','MW-003','REP-103','VIRTUAL','COMPLETED',0,'Discussed switching protocol','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-16',SHA2('CALL-022',256),'DEV'
  UNION ALL SELECT 'CALL-023','4567890123','2026-05-16'::DATE,'RELU_001','SW-004','REP-104','PHONE','COMPLETED',0,'Confirmed Relu formulary addition','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-16',SHA2('CALL-023',256),'DEV'
  UNION ALL SELECT 'CALL-024','5678901234','2026-05-17'::DATE,'ULIXA_001','NW-005','REP-105','FACE_TO_FACE','COMPLETED',2,'Presented patient support program','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-024',256),'DEV'
  UNION ALL SELECT 'CALL-025','6789012345','2026-05-17'::DATE,'RELU_001','NE-001','REP-101','FACE_TO_FACE','LEFT_SAMPLE',3,'Left starter kits at front desk','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-025',256),'DEV'
  UNION ALL SELECT 'CALL-026','7890123456','2026-05-17'::DATE,'ULIXA_001','SE-002','REP-102','VIRTUAL','COMPLETED',0,'Virtual detailing — MOA animation','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-026',256),'DEV'
  UNION ALL SELECT 'CALL-027','8901234567','2026-05-17'::DATE,'RELU_001','MW-003','REP-103','FACE_TO_FACE','COMPLETED',1,'Post-conference follow-up','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-027',256),'DEV'
  UNION ALL SELECT 'CALL-028','9012345678','2026-05-17'::DATE,'ULIXA_001','SW-004','REP-104','PHONE','COMPLETED',0,'Discussed managed care landscape','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-028',256),'DEV'
  UNION ALL SELECT 'CALL-029','1234567890','2026-05-17'::DATE,'RELU_001','NE-001','REP-101','FACE_TO_FACE','COMPLETED',2,'Third touch with Dr Singh — committed to try','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-029',256),'DEV'
  UNION ALL SELECT 'CALL-030','5678901234','2026-05-17'::DATE,'RELU_001','NW-005','REP-105','VIRTUAL','COMPLETED',0,'Quarterly business review','SEED-002','SEED-BATCH-002','VEEVA_CRM','veeva_crm_api/calls/2026-05-17',SHA2('CALL-030',256),'DEV'
);

SELECT COUNT(*) AS total_bronze_rows FROM NEW_TEST.BRONZE.RAW_VEEVA_CRM_CALLS;