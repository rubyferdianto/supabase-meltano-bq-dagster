{{
  config(
    materialized='table',
    cluster_by=['geolocation_state']
  )
}}

-- Clean geolocation dimension table ready for fact table joins
-- No validation flags - only business-ready data
select 
    -- Surrogate key for fact table relationships
    {{ generate_surrogate_key(['geolocation_zip_code_prefix']) }} as geolocation_sk,
    
    -- Business keys
    geolocation_zip_code_prefix,
    
    -- Geographic coordinates
    geolocation_lat,
    geolocation_lng,
    
    -- Location information
    geolocation_city,
    geolocation_state,
    
    -- Business classifications for analytics
    case 
        when upper(coalesce(geolocation_state, '')) in ('SP', 'RJ', 'MG', 'ES') then 'Southeast'
        when upper(coalesce(geolocation_state, '')) in ('RS', 'SC', 'PR') then 'South'
        when upper(coalesce(geolocation_state, '')) in ('GO', 'MT', 'MS', 'DF') then 'Central-West'
        when upper(coalesce(geolocation_state, '')) in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'Northeast'
        when upper(coalesce(geolocation_state, '')) in ('AM', 'RR', 'AP', 'PA', 'TO', 'RO', 'AC') then 'North'
        else 'Unknown'
    end as region,
    
    -- Market tier classification
    case 
        when upper(coalesce(geolocation_state, '')) in ('SP', 'RJ') then 'Tier 1 - Major Markets'
        when upper(coalesce(geolocation_state, '')) in ('MG', 'ES', 'RS', 'SC', 'PR') then 'Tier 2 - Regional Markets'
        when upper(coalesce(geolocation_state, '')) in ('BA', 'GO', 'DF') then 'Tier 3 - Secondary Markets'
        else 'Tier 4 - Emerging Markets'
    end as market_tier,
    
    -- State classification for business analysis
    case 
        when upper(coalesce(geolocation_state, '')) = 'SP' then 'São Paulo - Economic Hub'
        when upper(coalesce(geolocation_state, '')) = 'RJ' then 'Rio de Janeiro - Tourism & Business'
        when upper(coalesce(geolocation_state, '')) = 'MG' then 'Minas Gerais - Mining & Agriculture'
        when upper(coalesce(geolocation_state, '')) = 'RS' then 'Rio Grande do Sul - Industrial'
        else concat(coalesce(geolocation_state, 'Unknown'), ' - Standard Market')
    end as state_profile,
    
    -- Warehouse metadata
    current_datetime() as created_at,
    current_datetime() as updated_at

from {{ ref('stg_geolocation') }}
where geolocation_zip_code_prefix is not null
  and geolocation_state is not null  -- Only complete geographic data
