{{ config(materialized='table') }}

WITH orders_with_sk AS (
  SELECT 
    GENERATE_UUID() AS order_sk,
    order_id,
    customer_id,
    order_status,
    CAST(order_purchase_timestamp AS DATETIME) AS order_purchase_timestamp,
    CAST(order_approved_at AS DATETIME) AS order_approved_at,
    CAST(order_delivered_carrier_date AS DATETIME) AS order_delivered_carrier_date,
    CAST(order_delivered_customer_date AS DATETIME) AS order_delivered_customer_date,
    CAST(order_estimated_delivery_date AS DATETIME) AS order_estimated_delivery_date,
    -- Derived columns
    EXTRACT(YEAR FROM order_purchase_timestamp) AS order_year,
    EXTRACT(MONTH FROM order_purchase_timestamp) AS order_month,
    EXTRACT(DAY FROM order_purchase_timestamp) AS order_day,
    DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS delivery_days,
    CASE 
      WHEN order_status = 'delivered' THEN TRUE 
      ELSE FALSE 
    END AS is_delivered
  FROM {{ source('staging', 'supabase_olist_orders_dataset') }}
  WHERE order_id IS NOT NULL
)

SELECT * FROM orders_with_sk
ORDER BY order_sk