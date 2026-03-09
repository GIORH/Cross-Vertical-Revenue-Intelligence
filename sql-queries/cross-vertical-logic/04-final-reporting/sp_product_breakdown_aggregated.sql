/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Gold / Reporting Layer
DESCRIPTION: 
    Materializes a partitioned table for Product Breakdown analysis.
    Aggregates Core vs. Non-Core sales and calculates 'Loads' and 'Core Orders'.
    
KEY OPTIMIZATIONS:
    - Table Partitioning: Optimized by 'data_dt' for cost-efficient BI queries.
    - Data Cleaning: Filters specific Vendor IDs and valid Telus Agent IDs (Pattern: X000000).
    - Success Filtering: Only includes success release codes (100, 104).
===============================================================================
*/

CREATE OR REPLACE PROCEDURE `ti-ca-ml-start.ods_ptn_task.vm_locker_product_breakdown_aggregated`()
BEGIN

  -- Materialized table for optimized BI performance
  CREATE OR REPLACE TABLE `ti-ca-ml-start.ods_ptn_task.vm_product_breakdown_aggregated_table`
  PARTITION BY data_dt
  AS
  SELECT DISTINCT
    -- Dimensions: Sales Context
    sl.orig_sale_telus_agent_id,
    DATE(substr(sl.data_dt, 1,10)) AS data_dt,
    sl.css_portfolio_id,
    cls.queue,
    cls2.call_release_code,
    sl.css_product_id,
    product.PROD_NAME,

    -- Aggregations: Sales Performance KPIs
    -- Core Sales: High-value primary product activations
    COUNT(DISTINCT CASE 
        WHEN product.CORE_SALE = 1 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) AS core_sales,
    
    -- Non-Core Sales: Add-ons or secondary products
    COUNT(DISTINCT CASE 
        WHEN product.CORE_SALE = 0 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) AS non_core_sales,
    
    -- Total Loads: Sum of Core and Non-Core
    COUNT(DISTINCT CASE 
        WHEN product.CORE_SALE = 1 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) + 
    COUNT(DISTINCT CASE 
        WHEN product.CORE_SALE = 0 THEN CONCAT(COALESCE(wls_order_id, ffh_order_id), orig_sale_telus_agent_id, sold_ban, sold_phone_num, PROD_NAME_FR) 
    END) AS total_loads,
    
    -- Core Order: Unique account activations for Portfolio 192/193
    COUNT(DISTINCT CASE 
        WHEN product.CORE_SALE = 1 AND (sl.css_portfolio_id = 192 OR sl.css_portfolio_id = 193) THEN sl.sold_ban 
    END) AS core_order

  FROM `ti-ca-ml-start.alvaria_reporting.rf_sale_user` sl

  -- Call Context Joins
  LEFT JOIN (
      SELECT DISTINCT QUEUE, CONNECT_REC_ID
      FROM `ti-ca-ml-start.alvaria_reporting.rf_call_user`
  ) cls ON sl.connect_rec_id = cls.CONNECT_REC_ID

  LEFT JOIN (
      SELECT CONNECT_REC_ID, CALL_RELEASE_CODE 
      FROM `ti-ca-ml-start.alvaria_reporting.rf_call_user`
  ) cls2 ON sl.connect_rec_id = cls2.CONNECT_REC_ID

  -- Metadata & Roster Joins
  LEFT JOIN `ti-ca-ml-start.ods_ptn_task.vm_product_mapping_august_2025` product
    ON CAST(sl.css_product_id AS INT64) = CAST(product.PRODUCT_ID AS INT64)

  LEFT JOIN `ti-ca-ml-start.ods_ptn_task.cust_agent_tbl_scd` AS roster
    ON DATE(substr(sl.data_dt, 1,10)) >= roster.EffectiveStartDate 
    AND DATE(substr(sl.data_dt, 1,10)) < roster.EffectiveEndDate 
    AND LOWER(sl.telus_agent_id) = LOWER(roster.telus_agent_id)

  LEFT JOIN `ti-ca-ml-start.ods_ptn_task.tiig_lob_queue_portfolio_mapping` AS portfolio
    ON sl.CSS_PORTFOLIO_ID = portfolio.PORTFOLIO_ID
    AND cls.QUEUE = portfolio.QUEUE

  -- Business Integrity Filters
  WHERE DATE(substr(sl.data_dt, 1,10)) >= '2025-01-01'
    AND CAST(sl.vendor_id AS INT) = 2
    AND REGEXP_CONTAINS(sl.telus_agent_id, r'^X\d{6}$') -- Ensures valid Telus ID format
    AND sl.telus_agent_id <> 'SYS'
    AND cls2.call_release_code IN (100, 104)

  GROUP BY 1, 2, 3, 4, 5, 6, 7;

END;
