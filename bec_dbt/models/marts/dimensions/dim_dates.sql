{{
  config(
    materialized='table',
    description='Date dimension table with date_sk as primary key and comprehensive date attributes'
  )
}}

-- This model creates a fresh dim_date table on each run
-- dbt's 'table' materialization automatically does CREATE OR REPLACE TABLE
-- which truncates existing data and inserts fresh data

WITH date_range AS (
  -- Get date range from orders in staging
  SELECT 
    MIN(DATE(order_purchase_timestamp)) as min_date,
    MAX(DATE(order_purchase_timestamp)) as max_date
  FROM {{ source('staging', 'supabase_olist_orders_dataset') }}
  WHERE order_purchase_timestamp IS NOT NULL
  UNION ALL
  SELECT 
    DATE('2015-01-01') as min_date,
    DATE('2050-12-31') as max_date
),

final_range AS (
  SELECT 
    MIN(min_date) as start_date,
    MAX(max_date) as end_date
  FROM date_range
),

date_spine AS (
  SELECT date_val
  FROM final_range,
  UNNEST(GENERATE_DATE_ARRAY(start_date, end_date, INTERVAL 1 DAY)) AS date_val
)

SELECT 
  -- Primary key: date_sk (auto-incrementing number starting from 1)
  GENERATE_UUID() AS date_sk,
  
  -- Date value (not null)
  date_val AS date_value,
  
  -- Year
  EXTRACT(YEAR FROM date_val) AS year,
  
  -- Quarter
  EXTRACT(QUARTER FROM date_val) AS quarter,
  
  -- Month
  EXTRACT(MONTH FROM date_val) AS month,
  
  -- Day of month
  EXTRACT(DAY FROM date_val) AS day_of_month,
  
  -- Day of week (1=Sunday, 7=Saturday in BigQuery)
  EXTRACT(DAYOFWEEK FROM date_val) AS day_of_week,
  
  -- Is weekend (Saturday=7, Sunday=1)
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date_val) IN (1, 7) THEN TRUE 
    ELSE FALSE 
  END AS is_weekend,
  
  -- Additional useful date attributes
  FORMAT_DATE('%A', date_val) AS day_name,
  FORMAT_DATE('%B', date_val) AS month_name,
  EXTRACT(WEEK FROM date_val) AS week_of_year,
  
  -- Business day indicator (Monday=2 to Friday=6)
  CASE 
    WHEN EXTRACT(DAYOFWEEK FROM date_val) BETWEEN 2 AND 6 THEN TRUE 
    ELSE FALSE 
  END AS is_business_day
  
FROM date_spine
ORDER BY date_val
