{{ config(materialized='table') }}

WITH products_with_translations AS (
  SELECT 
    p.product_id,
    p.product_category_name,
    p.product_name_lenght,
    p.product_description_lenght,
    p.product_photos_qty,
    p.product_weight_g,
    p.product_length_cm,
    p.product_height_cm,
    p.product_width_cm,
    COALESCE(t.product_category_name_english, p.product_category_name) AS product_category_name_english
  FROM {{ source('staging', 'supabase_olist_products_dataset') }} p
  LEFT JOIN {{ source('staging', 'supabase_product_category_name_translation') }} t
    ON p.product_category_name = t.product_category_name
  WHERE p.product_id IS NOT NULL
),

products_with_sk AS (
  SELECT 
    GENERATE_UUID() AS product_sk,
    product_id,
    CASE 
      WHEN product_category_name = 'pc_gamer' THEN 'pc_gamer'
      WHEN product_category_name = 'portateis_cozinha_e_preparadores_de_alimentos' THEN 'portable_kitchen_and_food_preparators'
      ELSE product_category_name
    END AS product_category_name,
    CASE 
      WHEN product_category_name_english IS NULL OR product_category_name_english = '' THEN 'unknown'
      ELSE product_category_name_english
    END AS product_category_name_english,
    product_name_lenght product_name_length,
    product_description_lenght product_description_length,
    IFNULL(product_photos_qty, 0) product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    -- Derived columns
    ROUND(product_weight_g / 1000, 2) AS product_weight_kg,
    ROUND(product_length_cm * product_height_cm * product_width_cm, 2) AS product_volume_cm3,
    CASE 
      WHEN product_photos_qty = 0 THEN 'No Photos'
      WHEN product_photos_qty BETWEEN 1 AND 3 THEN 'Few Photos'
      WHEN product_photos_qty BETWEEN 4 AND 6 THEN 'Good Photos'
      ELSE 'Many Photos'
    END AS photo_quality_category
  FROM products_with_translations
)

SELECT * FROM products_with_sk
ORDER BY product_sk