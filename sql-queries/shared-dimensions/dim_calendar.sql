/*
===============================================================================
PROJECT: Enterprise Sales Performance Engine
LAYER: Gold / Dimension Layer
DESCRIPTION: 
    Generates a continuous dynamic calendar starting from 2024-01-01 
    up to the current date.
    
PURPOSE:
    - Standardize time-based reporting across all business verticals.
    - Support complex filtering (Weekends vs Weekdays, Quarters, Week of Month).
    - Enable seamless joins with Sales Fact tables for Time-Series Analysis.
===============================================================================
*/

WITH date_range AS (
  SELECT 
    DATE '2024-01-01' + INTERVAL x DAY AS date
  FROM 
    UNNEST(GENERATE_ARRAY(0, DATE_DIFF(CURRENT_DATE(), DATE '2024-01-01', DAY))) AS x
)
SELECT 
  -- Primary Key (Date)
  date,

  -- Temporal Attributes
  EXTRACT(YEAR FROM date) AS year,
  EXTRACT(MONTH FROM date) AS month_number,
  FORMAT_DATE('%B', date) AS month_name,
  EXTRACT(QUARTER FROM date) AS quarter_number,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS quarter_label,
  
  -- Weekly Logic
  EXTRACT(WEEK FROM date) AS week_of_year,
  CONCAT(CAST(EXTRACT(WEEK FROM date) AS STRING), '-', CAST(EXTRACT(YEAR FROM date) AS STRING)) AS week_year_key,
  CONCAT('W', CAST(FLOOR((EXTRACT(DAY FROM date) - 1) / 7) + 1 AS STRING)) AS week_of_month,

  -- Day Attributes
  EXTRACT(DAYOFWEEK FROM date) AS day_of_week_number, -- Sunday = 1
  FORMAT_DATE('%A', date) AS day_name,
  EXTRACT(DAYOFYEAR FROM date) AS day_of_year,
  
  -- Business Categorization
  CASE
    WHEN EXTRACT(DAYOFWEEK FROM date) IN (1, 7) THEN 'Weekend'
    ELSE 'Weekday'
  END AS day_type,

  -- Reporting Labels (Formatting for BI tools like Looker Studio)
  FORMAT_DATE('%B-%Y', date) AS month_year_label,
  CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING), ',', CAST(EXTRACT(YEAR FROM date) AS STRING)) AS quarter_year_label

FROM 
  date_range
ORDER BY 
  date DESC;
