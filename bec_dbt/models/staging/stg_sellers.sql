{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'sellers') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by seller_id) as duplicate_count,
        row_number() over (
            partition by seller_id
            order by 
                case when seller_city is not null then 0 else 1 end,
                case when seller_state is not null then 0 else 1 end,
                seller_city,
                seller_state
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
        -- Primary key
        seller_id,
        
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0') as seller_zip_code_prefix,
        REGEXP_REPLACE(
            TRIM(UPPER(NORMALIZE(seller_city, NFD))), 
            r'[\\u0300-\\u036f]', 
            ''
        ) as seller_city,
        UPPER(TRIM(seller_state)) as seller_state,
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        seller_zip_code_prefix as seller_zip_code_prefix_original,
        seller_city as seller_city_original,
        seller_state as seller_state_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when seller_id is not null and LENGTH(TRIM(seller_id)) > 0 then true else false end as is_valid_seller_id,
        case when seller_zip_code_prefix is not null 
             and REGEXP_CONTAINS(LPAD(CAST(seller_zip_code_prefix as STRING), 5, '0'), r'^[0-9]{5}$')
             then true else false end as is_valid_zip_code,
        case when seller_city is not null and LENGTH(TRIM(seller_city)) >= 2 then true else false end as is_valid_city,
        case when seller_state is not null 
             and UPPER(TRIM(seller_state)) in ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')
             then true else false end as is_valid_state,
             
        -- COMPOSITE KEY (for advanced uniqueness testing)
        CONCAT(
            COALESCE(seller_id, 'NULL'), '_',
            COALESCE(LPAD(CAST(seller_zip_code_prefix as STRING), 5, '0'), 'NULL'), '_',
            COALESCE(UPPER(TRIM(seller_state)), 'NULL')
        ) as composite_seller_key,
        
        -- QUALITY FLAGS (for backwards compatibility with existing logic)
        case when seller_zip_code_prefix is null then true else false end as seller_zip_code_prefix_is_null,
        case when SAFE_CAST(seller_zip_code_prefix as INT64) < 1 OR SAFE_CAST(seller_zip_code_prefix as INT64) > 99999 then true else false end as seller_zip_code_prefix_invalid_range,
        case when LENGTH(LPAD(CAST(seller_zip_code_prefix AS STRING), 5, '0')) != 5 then true else false end as seller_zip_code_prefix_invalid_length,
        
        case when seller_city is null then true else false end as seller_city_is_null,
        case when length(trim(seller_city)) = 0 then true else false end as seller_city_is_empty,
        
        case when seller_state is null then true else false end as seller_state_is_null,
        case when seller_state not in ('SP', 'RJ', 'MG', 'RS', 'PR', 'SC', 'BA', 'GO', 'ES', 'PE', 'CE', 'PB', 'PA', 'RN', 'AL', 'MT', 'MS', 'DF', 'PI', 'SE', 'RO', 'TO', 'AC', 'AM', 'AP', 'RR') then true else false end as seller_state_invalid_value,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
