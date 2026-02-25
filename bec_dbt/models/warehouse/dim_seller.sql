{{
  config(
    materialized='table',
    cluster_by=['seller_state', 'data_quality_tier']
  )
}}

with seller_base as (
    select 
        -- Core fields
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        
        -- Quality validation flags (already boolean from staging)
        is_valid_zip_code,
        is_valid_city,
        is_valid_state,
        -- Basic calculations
        -- Compute location consistency
        case when (is_valid_zip_code = true and is_valid_city = true and is_valid_state = true) then true else false end as is_valid_location_consistency,
        
        -- Create normalized fields inline (Brazilian normalization)
        upper(normalize(coalesce(seller_city, ''), NFD)) as seller_city_normalized,
        upper(coalesce(seller_state, '')) as seller_state_normalized
        
    from {{ ref('stg_sellers') }}
),

seller_enhanced as (
    select 
        *,
        
        -- Regional classification
        case 
            when seller_state_normalized in ('SP', 'RJ', 'MG', 'ES') then 'Southeast'
            when seller_state_normalized in ('RS', 'SC', 'PR') then 'South'
            when seller_state_normalized in ('GO', 'MT', 'MS', 'DF') then 'Central-West'
            when seller_state_normalized in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'Northeast'
            when seller_state_normalized in ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') then 'North'
            else 'Unknown'
        end as region,
        
        -- Seller location type classification
        case 
            when seller_state_normalized = 'SP' and seller_city_normalized = 'SAO PAULO' then 'Major Metro'
            when seller_state_normalized = 'RJ' and seller_city_normalized = 'RIO DE JANEIRO' then 'Major Metro'
            when seller_state_normalized in ('SP', 'RJ') then 'Major State'
            when seller_state_normalized in ('MG', 'RS', 'PR', 'SC', 'BA', 'GO') then 'Large State'
            else 'Other'
        end as location_type,
        
        -- Data quality tier
        case 
            when is_valid_zip_code and is_valid_city and is_valid_state and is_valid_location_consistency 
                then 'High Quality'
            when is_valid_state and is_valid_city 
                then 'Medium Quality'
            when is_valid_state 
                then 'Low Quality'
            else 'Poor Quality'
        end as data_quality_tier,
        
        -- Quality completeness score (0-100)
        (
            case when is_valid_zip_code then 25 else 0 end +
            case when is_valid_city then 25 else 0 end +
            case when is_valid_state then 25 else 0 end +
            case when is_valid_location_consistency then 25 else 0 end
        ) as quality_completeness_score
        
    from seller_base
),

seller_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['seller_id']) }} as seller_sk,
        
        -- Natural key and attributes
        seller_id,
        seller_zip_code_prefix,
        seller_city,
        seller_state,
        
        -- Normalized and enhanced fields
        seller_city_normalized,
        seller_state_normalized,
        region,
        location_type,
        
        -- Quality fields
        data_quality_tier,
        quality_completeness_score,
        is_valid_zip_code,
        is_valid_city,
        is_valid_state,
        is_valid_location_consistency,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from seller_enhanced
)

select 
    seller_sk,
    seller_id,
    seller_zip_code_prefix,
    seller_city,
    seller_state,
    seller_city_normalized,
    seller_state_normalized,
    region,
    location_type,
    data_quality_tier,
    quality_completeness_score,
    is_valid_zip_code,
    is_valid_city,
    is_valid_state,
    is_valid_location_consistency,
    insertion_timestamp
from seller_with_sk
