/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Bronze/Silver (Raw Ingestion)
DESCRIPTION: 
    Consolidates historical sales data (legacy) and current reporting views.
    Unifies Mobility (WLS) and Home Services (FFH) records.
    
KEY TRANSFORMATIONS:
    - Date Normalization: Handles different string formats for DATA_DT.
    - Unique ID Generation: Creates 'core_sales_id' to prevent duplicates across sources.
    - SCD Type 2 Join: Connects agents to their roster key based on sale date.
===============================================================================
*/

-- 1. LEGACY DATA (Up to 2025-03-31)
SELECT 
  sl.BAN,
  sl.CONNECT_REC_ID,
  sl.CONS_ID,
  sl.CSS_PORTFOLIO_ID,
  sl.CSS_PRODUCT_ID,
  sl.DUE_DATE,
  sl.DUE_DATE_DRIVEN_BY,
  sl.FFH_ORDER_ID,
  sl.FILE_EXPORT_DT,
  sl.MISCNUM1,
  sl.MISCNUM2,
  sl.MISCVAR1,
  sl.MISCVAR2,
  sl.ORIG_SALE_TELUS_AGENT_ID,
  sl.PHONE1,
  sl.REFERRED,
  sl.SALE_REC_ID,
  sl.SOLD_BAN,
  sl.SOLD_PHONE_NUM,
  sl.TELUS_AGENT_ID,
  sl.TERM_MONTHS,
  sl.VENDOR_ID,
  sl.WLS_ORDER_ID,
  sl.WLS_SOC_CODE,
  -- Normalize date from string format with colon
  DATE(split(sl.DATA_DT, ':')[0]) as DATA_DT, 
  -- Unique Business Key generation
  CONCAT(COALESCE(WLS_ORDER_ID, FFH_ORDER_ID), ORIG_SALE_TELUS_AGENT_ID, SOLD_BAN, SOLD_PHONE_NUM, PROD_NAME) as core_sales_id, 
  pf.PROD_LOB, 
  pf.PROD_NAME, 
  pf.CORE_SALE,
  cls.QUEUE,
  c_agent.agent_roster_key,
  cls2.CALL_RELEASE_CODE

FROM `ti-ca-ml-start.tipbpo_temp.TIIG_SALES` sl
LEFT JOIN (
    SELECT DISTINCT QUEUE, CONNECT_REC_ID 
    FROM `ti-ca-ml-start.tipbpo_temp.TIIG_CALLS`
) cls ON sl.CONNECT_REC_ID = cls.CONNECT_REC_ID
LEFT JOIN (
    SELECT CONNECT_REC_ID, CALL_RELEASE_CODE 
    FROM `ti-ca-ml-start.tipbpo_temp.TIIG_CALLS`
) cls2 ON sl.CONNECT_REC_ID = cls2.CONNECT_REC_ID
LEFT JOIN `ti-ca-ml-start.alvaria_task.product_file` pf ON sl.CSS_PRODUCT_ID = pf.PRODUCT_ID
LEFT JOIN `ods_ptn_task.cust_agent_tbl_scd` AS c_agent
  ON DATE(split(sl.DATA_DT, ':')[0]) >= c_agent.EffectiveStartDate 
  AND DATE(split(sl.DATA_DT, ':')[0]) < c_agent.EffectiveEndDate 
  AND LOWER(sl.TELUS_AGENT_ID) = LOWER(c_agent.telus_agent_id)
LEFT JOIN `ti-ca-ml-start.ods_ptn_task.tiig_lob_queue_portfolio_mapping` queue_portfolio_map 
  ON sl.CSS_PORTFOLIO_ID = queue_portfolio_map.PORTFOLIO_ID
  AND cls.QUEUE = queue_portfolio_map.QUEUE
WHERE DATE(split(sl.DATA_DT, ':')[0]) <= '2025-03-31' 

UNION ALL 

-- 2. CURRENT REPORTING DATA (From 2025-04-01 onwards)
SELECT 
  sl.ban,
  sl.connect_rec_id,
  sl.cons_id,
  sl.css_portfolio_id,
  CAST(sl.css_product_id AS INT64) AS css_product_id,
  sl.due_date,
  sl.due_date_driven_by,
  sl.ffh_order_id,
  sl.file_export_dt,
  sl.miscnum1,
  sl.miscnum2,
  sl.miscvar1,
  sl.miscvar2,
  sl.orig_sale_telus_agent_id,
  CAST(sl.phone1 AS INT64) AS phone1,
  sl.referred,
  CAST(sl.sale_rec_id AS STRING) AS sale_rec_id,
  sl.sold_ban,
  CAST(sl.sold_phone_num AS INT64) AS sold_phone_num,
  sl.telus_agent_id,
  sl.term_months,
  CAST(sl.vendor_id AS INT64) AS vendor_id,
  sl.wls_order_id,
  sl.wls_soc_code,
  -- Normalize date from string format with space
  DATE(split(sl.data_dt, ' ')[0]) as DATA_DT, 
  CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, prod_name) as core_sales_id, 
  pf.PROD_LOB, 
  pf.PROD_NAME, 
  pf.CORE_SALE,
  cls.QUEUE,
  c_agent.agent_roster_key,
  cls2.CALL_RELEASE_CODE

FROM `ti-ca-ml-start.alvaria_reporting.rf_sale_view` sl
LEFT JOIN (
    SELECT DISTINCT QUEUE, CONNECT_REC_ID 
    FROM `ti-ca-ml-start.alvaria_reporting.rf_call_view`
) cls ON sl.connect_rec_id = cls.CONNECT_REC_ID
LEFT JOIN (
    SELECT CONNECT_REC_ID, CALL_RELEASE_CODE 
    FROM `ti-ca-ml-start.alvaria_reporting.rf_call_view`
) cls2 ON sl.connect_rec_id = cls2.CONNECT_REC_ID
LEFT JOIN `ti-ca-ml-start.alvaria_task.product_file` pf ON CAST(sl.css_product_id AS INT64) = CAST(pf.PRODUCT_ID AS INT64)
LEFT JOIN `ods_ptn_task.cust_agent_tbl_scd` AS c_agent
  ON DATE(split(sl.data_dt, ' ')[0]) >= c_agent.EffectiveStartDate 
  AND DATE(split(sl.data_dt, ' ')[0]) < c_agent.EffectiveEndDate 
  AND LOWER(sl.telus_agent_id) = LOWER(c_agent.telus
