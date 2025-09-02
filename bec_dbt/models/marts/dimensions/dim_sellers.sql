{{ config(materialized='table') }}

WITH sellers_with_sk AS (
  SELECT 
    GENERATE_UUID() AS seller_sk,
    seller_id,
    CASE
      WHEN LENGTH(seller_zip_code_prefix) = 4 THEN CAST('0'||seller_zip_code_prefix AS STRING)
      ELSE seller_zip_code_prefix
    END AS seller_zip_code_prefix,    
    seller_city,
    seller_state,
    -- Derived columns
    UPPER(seller_state) AS seller_state_code,
    CONCAT(seller_city, ', ', seller_state) AS seller_location,
    CASE 
      WHEN seller_state IN ('SP', 'RJ', 'MG') THEN 'Southeast'
      WHEN seller_state IN ('RS', 'SC', 'PR') THEN 'South'
      WHEN seller_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'
      WHEN seller_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
      WHEN seller_state IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
      ELSE 'Other'
    END AS seller_region
  FROM {{ source('staging', 'supabase_olist_sellers_dataset') }}
  WHERE seller_id IS NOT NULL
)

SELECT * FROM sellers_with_sk
ORDER BY seller_sk