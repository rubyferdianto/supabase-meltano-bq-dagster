{{ config(materialized='table') }}

WITH reviews_with_sk AS (
  SELECT 
    GENERATE_UUID() AS review_sk,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS DATETIME) AS review_creation_date,
    CAST(review_answer_timestamp AS DATETIME) AS review_answer_timestamp,
    -- Derived columns
    CASE 
      WHEN review_score >= 4 THEN 'Positive'
      WHEN review_score = 3 THEN 'Neutral'
      ELSE 'Negative'
    END AS review_sentiment,
    CASE 
      WHEN review_comment_message IS NOT NULL AND LENGTH(review_comment_message) > 0 THEN TRUE
      ELSE FALSE
    END AS has_comment,
    CASE 
      WHEN review_answer_timestamp IS NOT NULL THEN TRUE
      ELSE FALSE
    END AS has_answer,
    EXTRACT(YEAR FROM review_creation_date) AS review_year,
    EXTRACT(MONTH FROM review_creation_date) AS review_month
  FROM {{ source('staging', 'supabase_olist_order_reviews_dataset') }}
  WHERE review_id IS NOT NULL
)

SELECT * FROM reviews_with_sk
ORDER BY review_sk