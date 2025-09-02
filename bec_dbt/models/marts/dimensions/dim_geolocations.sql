{{ config(materialized='table') }}

WITH geolocations_with_sk AS (
  SELECT 
    GENERATE_UUID() AS geolocation_sk,
    geolocation_zip_code_prefix,
    geolocation_lat,
    geolocation_lng,
    geolocation_city,
    geolocation_state,
    -- Derived columns
    UPPER(geolocation_state) AS geolocation_state_code,
    CONCAT(geolocation_city, ', ', geolocation_state) AS geolocation_location,
    CASE 
      WHEN geolocation_state IN ('SP', 'RJ', 'MG', 'ES') THEN 'Southeast'
      WHEN geolocation_state IN ('RS', 'SC', 'PR') THEN 'South'
      WHEN geolocation_state IN ('GO', 'MT', 'MS', 'DF') THEN 'Central-West'
      WHEN geolocation_state IN ('BA', 'SE', 'PE', 'AL', 'PB', 'RN', 'CE', 'PI', 'MA') THEN 'Northeast'
      WHEN geolocation_state IN ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') THEN 'North'
      ELSE 'Other'
    END AS geolocation_region,
    -- Geographic calculations
    ROUND(CAST(geolocation_lat AS FLOAT64), 4) AS geolocation_lat_rounded,
    ROUND(CAST(geolocation_lng AS FLOAT64), 4) AS geolocation_lng_rounded
  FROM {{ source('staging', 'supabase_olist_geolocation_dataset') }}
  WHERE geolocation_zip_code_prefix IS NOT NULL
    AND geolocation_lat IS NOT NULL
    AND geolocation_lng IS NOT NULL
)

SELECT * FROM geolocations_with_sk
ORDER BY geolocation_sk