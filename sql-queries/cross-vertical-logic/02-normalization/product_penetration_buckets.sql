/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Silver / Normalization
DESCRIPTION: 
    Calculates Tier 1 Product Activations and categorizes them into 
    Penetration Buckets (1p, 2p, 3p, 4p+).
    
KEY BUSINESS RULES:
    - Tier 1 Only: Excludes BYOD, Tablets, and Stream+ (RGU).
    - Valid Sales: Filters by specific Call Release Codes (100, 104).
    - Scope: Focused on FFH (Future Friendly Home) and Core Sales.
===============================================================================
*/

WITH tier_1_filtered_sales AS (
    SELECT 
        *
    FROM `ti-ca-ml-start.ods_ptn_task.vw_return_file_sales`
    WHERE 
        -- Exclude non-tier 1 or secondary products
        PROD_NAME_FR NOT IN (
            'TELUS - BYOD Handset', 'Koodo - BYOD Handset', 'Koodo - BYOD Tablet', 
            'Koodo - Handset', 'TELUS - Handset', 'TELUS Tablet', 'Koodo - Tablet', 
            'TELUS - BYOD Tablet', 'Stream+ (RGU)'
        )
        -- Success/Conversion release codes
        AND CALL_RELEASE_CODE IN (100, 104)
        AND CORE_SALE = 1
        AND bundle_new_lob_mapping = 'FFH'
),

activation_summary AS (
    SELECT 
        CONCAT(b.sup_first_name, ' ', b.sup_last_name) AS flm_name,
        a.agent_roster_key,
        a.TELUS_AGENT_ID,
        a.DATA_DT,
        a.SOLD_BAN, 
        a.CSS_PORTFOLIO_ID,
        
        -- Logic: Count unique combinations of Product + Order to identify multi-play sales
        COUNT(DISTINCT CONCAT(PROD_NAME_FR, FFH_ORDER_ID)) AS product_count,
        
        -- Categorize the 'Size of the Basket'
        CASE
            WHEN COUNT(DISTINCT CONCAT(PROD_NAME_FR, FFH_ORDER_ID)) = 1 THEN '1p'
            WHEN COUNT(DISTINCT CONCAT(PROD_NAME_FR, FFH_ORDER_ID)) = 2 THEN '2p'
            WHEN COUNT(DISTINCT CONCAT(PROD_NAME_FR, FFH_ORDER_ID)) = 3 THEN '3p'
            WHEN COUNT(DISTINCT CONCAT(PROD_NAME_FR, FFH_ORDER_ID)) >= 4 THEN '4p'
            ELSE 'Other'
        END AS activation_bucket,

        1 AS sale_flag

    FROM tier_1_filtered_sales AS a 
    JOIN `ti-ca-ml-start.ods_ptn_task.vw_cust_agent_tbl_scd` AS b
      ON a.agent_roster_key = b.agent_roster_key
    GROUP BY 1, 2, 3, 4, 5, 6
)

-- FINAL OUTPUT
SELECT 
    * FROM activation_summary
ORDER BY DATA_DT DESC, agent_roster_key;
