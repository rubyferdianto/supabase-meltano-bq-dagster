{{
  config(
    materialized='table',
    cluster_by=['product_category_name_english']
  )
}}

/*
    Product Dimension - Enhanced with Data Quality & Transformation
    
    Structure: Staging → Warehouse (direct integration)
    Features:
    - Quality validation flags from staging
    - Dimension standardization (0 defaults for NULLs)
    - Category translation integration
    - Product classification logic
*/

with product_base as (
    select 
        p.product_id,
        
        -- Category information with translation
        p.product_category_name,
        coalesce(t.product_category_name_english, p.product_category_name) as product_category_name_english,
        
        -- Product attributes (enhanced from staging with defaults)
        p.product_name_lenght,             -- Note: keeping original typo from staging
        p.product_description_lenght,      -- Note: keeping original typo from staging
        p.product_photos_qty,              -- Already defaulted to 0 in staging
        p.product_weight_g,                -- Already defaulted to 0 in staging
        p.product_length_cm,               -- Already defaulted to 0 in staging  
        p.product_height_cm,               -- Already defaulted to 0 in staging
        p.product_width_cm,                -- Already defaulted to 0 in staging
        
        -- Quality flags (boolean from staging)
        p.is_valid_product_id,
        p.is_valid_category,
        p.is_valid_name_length,
        p.is_valid_description_length,
        p.is_valid_photos_qty,
        p.is_valid_weight,
        p.is_valid_length,
        p.is_valid_height,
        p.is_valid_width,
        
        -- Original data for audit
        p.product_category_name_original,
        p.product_name_lenght_original,       -- Note: keeping original typo from source
        p.product_description_lenght_original, -- Note: keeping original typo from source
        p.product_photos_qty_original,
        p.product_weight_g_original,
        p.product_length_cm_original,
        p.product_height_cm_original,
        p.product_width_cm_original,
        
        -- Composite key
        p.composite_product_key
        
    from {{ ref('stg_products') }} p
    left join {{ ref('stg_product_category_name_translation') }} t
        on p.product_category_name = t.product_category_name
),

product_with_enrichment as (
    select 
        -- Generate surrogate key (matching teammate's pattern)
        {{ generate_surrogate_key(['product_id']) }} as product_sk,
        
        -- All base attributes
        *,
        
        -- Business enrichment: Product size classification
        case 
            when product_weight_g = 0 or product_length_cm = 0 or product_height_cm = 0 or product_width_cm = 0 
            then 'Unknown Size'
            when product_weight_g <= 100 and (product_length_cm * product_height_cm * product_width_cm) <= 1000 
            then 'Small'
            when product_weight_g <= 1000 and (product_length_cm * product_height_cm * product_width_cm) <= 50000 
            then 'Medium'
            else 'Large'
        end as product_size_category,
        
        -- Product completeness score
        (case when is_valid_product_id then 1 else 0 end +
         case when is_valid_category then 1 else 0 end +
         case when is_valid_weight then 1 else 0 end +
         case when is_valid_length then 1 else 0 end +
         case when is_valid_height then 1 else 0 end +
         case when is_valid_width then 1 else 0 end) as quality_score,
        
        -- Data quality tier
        case 
            when is_valid_product_id and is_valid_category and is_valid_weight 
                 and is_valid_length and is_valid_height and is_valid_width 
            then 'High Quality'
            when (case when is_valid_product_id then 1 else 0 end +
                  case when is_valid_category then 1 else 0 end +
                  case when is_valid_weight then 1 else 0 end +
                  case when is_valid_length then 1 else 0 end +
                  case when is_valid_height then 1 else 0 end +
                  case when is_valid_width then 1 else 0 end) >= 4 
            then 'Medium Quality'
            else 'Low Quality'
        end as data_quality_tier,
        
        -- System fields
        current_datetime() as insertion_timestamp
        
    from product_base
)

select 
    -- Surrogate key
    product_sk,
    
    -- Business key
    product_id,
    
    -- Category dimensions
    product_category_name,
    product_category_name_english,
    
    -- Product attributes (with quality defaults)
    product_name_lenght,               -- Note: keeping original typo
    product_description_lenght,        -- Note: keeping original typo
    product_photos_qty,
    product_weight_g,
    product_length_cm,
    product_height_cm,
    product_width_cm,
    
    -- Business classifications
    product_size_category,
    data_quality_tier,
    quality_score,
    
    -- Quality validation flags
    is_valid_product_id,
    is_valid_category,
    is_valid_name_length,
    is_valid_description_length,
    is_valid_photos_qty,
    is_valid_weight,
    is_valid_length,
    is_valid_height,
    is_valid_width,
    
    -- Audit trail
    product_category_name_original,
    product_name_lenght_original,
    product_description_lenght_original,
    product_photos_qty_original,
    product_weight_g_original,
    product_length_cm_original,
    product_height_cm_original,
    product_width_cm_original,
    composite_product_key,
    
    -- System fields
    insertion_timestamp

from product_with_enrichment
