/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Silver / Dimension Layer
DESCRIPTION: 
    Normalizes the Master Agent Roster using SCD Type 2 logic.
    Ensures that sales are attributed to the correct leadership hierarchy
    based on the transaction date.

KEY FEATURES:
    - SCD Type 2 Support: Handles historical changes in agent-to-supervisor mapping.
    - Hierarchy Flattening: Provides direct access to Supervisor (FLM) and Manager names.
    - Data Integrity: Filters for valid Telus ID patterns (X-prefix).
===============================================================================
*/

CREATE OR REPLACE PROCEDURE `ti-ca-ml-start.ods_ptn_task.sp_normalize_agent_roster`()
BEGIN

  CREATE OR REPLACE TABLE `ti-ca-ml-start.ods_ptn_task.dim_agent_roster_normalized` AS
  SELECT 
    agent_roster_key,
    telus_agent_id,
    -- Agent Identity
    CONCAT(first_name, ' ', last_name) AS agent_full_name,
    
    -- Leadership Hierarchy (FLM / Supervisor)
    sup_telus_id AS supervisor_id,
    CONCAT(sup_first_name, ' ', sup_last_name) AS supervisor_full_name,
    
    -- Management Layer
    mgr_telus_id AS manager_id,
    CONCAT(mgr_first_name, ' ', mgr_last_name) AS manager_full_name,
    
    -- Operational Context
    program,
    location,
    
    -- SCD Timestamps
    EffectiveStartDate,
    EffectiveEndDate

  FROM `ti-ca-ml-start.ods_ptn_task.cust_agent_tbl_scd`
  WHERE REGEXP_CONTAINS(telus_agent_id, r'^X\d{6}$');

END;
