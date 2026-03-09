/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Gold / Reporting (Fact Layer)
DESCRIPTION: 
    Materializes the main Fact Table for sales performance.
    Aggregates metrics at the Agent/Date level to optimize BI tool performance.
    
KEY METRICS:
    - Core Sales: Primary high-value activations.
    - Non-Core Sales: Secondary or add-on products.
    - Loads: Combined volume of all activations.
    - Tenure Analysis: Categorizes agents based on their time in LOB (VET vs Learning).
===============================================================================
*/

CREATE OR REPLACE PROCEDURE `ti-ca-ml-start.ods_ptn_task.vm_locker_fact_sales_aggregated`()
BEGIN

  CREATE OR REPLACE TABLE `ti-ca-ml-start.ods_ptn_task.vm_locker_fact_sales_aggregated_table`
  PARTITION BY data_dt
  AS
  SELECT DISTINCT
    -- Primary Dimensions
    sl.orig_sale_telus_agent_id,
    DATE(substr(sl.data_dt, 1,10)) AS data_dt,
    sl.css_portfolio_id,
    cls.queue,
    cls2.call_release_code,

    -- Dynamic Tenure Calculation
    CASE
      WHEN DATE_DIFF(DATE(substr(sl.data_dt, 1,10)), roster.lob_start_dt, DAY) >= 60 THEN 'VET'
      WHEN DATE_DIFF(DATE(substr(sl.data_dt, 1,10)), roster.lob_start_dt, DAY) >= 30 THEN '2 MONTHS'
      WHEN DATE_DIFF(DATE(substr(sl.data_dt, 1,10)), roster.lob_start_dt, DAY) >= 0 THEN '1 MONTH'
      ELSE 'UNKNOWN'
    END AS tenure_bucket,

    -- Aggregated Performance Metrics
    -- Note: Using DISTINCT CONCAT to ensure high-integrity counting of unique orders
    COUNT(DISTINCT CASE 
        WHEN pf.CORE_SALE = 1 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) AS core_sales,
    
    COUNT(DISTINCT CASE 
        WHEN pf.CORE_SALE = 0 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) AS non_core_sales,
    
    COUNT(DISTINCT CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR)) AS total_loads,
    
    COUNT(DISTINCT CASE 
        WHEN pf.CORE_SALE = 1 AND (sl.css_portfolio_id = 192 OR sl.css_portfolio_id = 193) THEN sl.sold_ban 
    END) AS core_orders

  FROM `ti-ca-ml-start.alvaria_reporting.rf_sale_user` sl

  -- Contextual Joins
  LEFT JOIN (
      SELECT DISTINCT QUEUE, CONNECT_REC_ID
      FROM `ti-ca-ml-start.alvaria_reporting.rf_call_user`
  ) cls ON sl.connect_rec_id = cls.CONNECT_REC_ID

  LEFT JOIN (
      SELECT CONNECT_REC_ID, CALL_RELEASE_CODE 
      FROM `ti-ca-ml-start.alvaria_reporting.rf_call_user`
  ) cls2 ON sl.connect_rec_id = cls2.CONNECT_REC_ID

  -- Metadata Joins
  LEFT JOIN `ti-ca-ml-start.ods_ptn_task.vm_product_mapping_august_2025` pf 
    ON CAST(sl.css_product_id AS INT64) = CAST(pf.PRODUCT_ID AS INT64)

  LEFT JOIN `ti-ca-ml-start.ods_ptn_task.cust_agent_tbl_scd` AS roster
    ON DATE(substr(sl.data_dt, 1,10)) >= roster.EffectiveStartDate 
    AND DATE(substr(sl.data_dt, 1,10)) < roster.EffectiveEndDate 
    AND LOWER(sl.telus_agent_id) = LOWER(roster.telus_agent_id)

  -- Filters for Production Environment
  WHERE DATE(substr(sl.data_dt, 1,10)) >= '2025-01-01'
    AND CAST(sl.vendor_id AS INT) = 2
    AND REGEXP_CONTAINS(sl.telus_agent_id, r'^X\d{6}$')
    AND sl.telus_agent_id <> 'SYS'
    AND cls2.call_release_code IN (100,104)

  GROUP BY 1, 2, 3, 4, 5, 6;

END;
