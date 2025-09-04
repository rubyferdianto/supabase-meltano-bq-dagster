{{ config(materialized='table') }}

with source as (
    select * from {{ source('staging', 'supabase_olist_geolocation_dataset') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by geolocation_zip_code_prefix) as duplicate_count,
        row_number() over (
            partition by geolocation_zip_code_prefix
            order by 
                case when geolocation_city is not null then 0 else 1 end,
                case when geolocation_state is not null then 0 else 1 end,
                case when geolocation_lat is not null then 0 else 1 end,
                case when geolocation_lng is not null then 0 else 1 end,
                geolocation_city
        ) as row_num 
    from source
),
unique_records as (
    select 
        * except(row_num),
        case when duplicate_count > 1 then true else false end as had_duplicates
    from deduplicated 
    where row_num = 1
),

with_quality_flags as (
    select
        -- Convert ZIP code to proper 5-digit STRING format with leading zeros
        LPAD(geolocation_zip_code_prefix, 5, '0') as geolocation_zip_code_prefix,
        case when geolocation_zip_code_prefix is null then true else false end as geolocation_zip_code_prefix_is_null,
        case when SAFE_CAST(geolocation_zip_code_prefix AS INT64) < 1 OR SAFE_CAST(geolocation_zip_code_prefix AS INT64) > 99999 then true else false end as geolocation_zip_code_prefix_invalid_range,
        case when LENGTH(LPAD(geolocation_zip_code_prefix, 5, '0')) != 5 then true else false end as geolocation_zip_code_prefix_invalid_length,
        
        SAFE_CAST(geolocation_lat AS FLOAT64) as geolocation_lat,
        case when geolocation_lat is null then true else false end as geolocation_lat_is_null,
        case when SAFE_CAST(geolocation_lat AS FLOAT64) < -90 or SAFE_CAST(geolocation_lat AS FLOAT64) > 90 then true else false end as geolocation_lat_out_of_range,
        
        SAFE_CAST(geolocation_lng AS FLOAT64) as geolocation_lng,
        case when geolocation_lng is null then true else false end as geolocation_lng_is_null,
        case when SAFE_CAST(geolocation_lng AS FLOAT64) < -180 or SAFE_CAST(geolocation_lng AS FLOAT64) > 180 then true else false end as geolocation_lng_out_of_range,
        
        geolocation_city,
        case when geolocation_city is null then true else false end as geolocation_city_is_null,
        case when length(trim(geolocation_city)) = 0 then true else false end as geolocation_city_is_empty,
        
        geolocation_state,
        case when geolocation_state is null then true else false end as geolocation_state_is_null,
        case when geolocation_state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as geolocation_state_invalid_value,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags