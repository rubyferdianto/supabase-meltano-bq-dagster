{{ config(materialized='table') }}

WITH customers_with_sk AS (
  SELECT 
    GENERATE_UUID() AS customer_sk,
    customer_id,
    customer_unique_id,
    CASE
      WHEN LENGTH(customer_zip_code_prefix) = 4 THEN CAST('0'||customer_zip_code_prefix AS STRING)
      ELSE customer_zip_code_prefix
    END AS customer_zip_code_prefix,
    customer_city,
    customer_state,
    -- Derived columns
    UPPER(customer_state) AS customer_state_code,
    CONCAT(customer_city, ', ', customer_state) AS customer_location,
    CASE 
      WHEN customer_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
      WHEN customer_state IN ('RS', 'SC', 'PR') THEN 'South'
      WHEN customer_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'
      WHEN customer_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
      WHEN customer_state IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
      ELSE 'Other'
    END AS customer_region
  FROM {{ source('staging', 'supabase_olist_customers_dataset') }}
  WHERE customer_id IS NOT NULL
)

SELECT * FROM customers_with_sk
ORDER BY customer_sk