{{ config(materialized='table') }}

WITH payments_with_sk AS (
  SELECT 
    GENERATE_UUID() AS payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    -- Derived columns
    CASE 
      WHEN payment_installments = 1 THEN 'Single Payment'
      WHEN payment_installments BETWEEN 2 AND 6 THEN 'Short Term'
      WHEN payment_installments BETWEEN 7 AND 12 THEN 'Medium Term'
      ELSE 'Long Term'
    END AS installment_category,
    CASE 
      WHEN payment_value < 50 THEN 'Low Value'
      WHEN payment_value BETWEEN 50 AND 200 THEN 'Medium Value'
      WHEN payment_value BETWEEN 200 AND 500 THEN 'High Value'
      ELSE 'Premium Value'
    END AS payment_value_category,
    ROUND(CASE WHEN payment_installments > 0 THEN payment_value / payment_installments END, 2) AS installment_amount
  FROM {{ source('staging', 'supabase_olist_order_payments_dataset') }}
  WHERE order_id IS NOT NULL
)

SELECT * FROM payments_with_sk
ORDER BY payment_sk