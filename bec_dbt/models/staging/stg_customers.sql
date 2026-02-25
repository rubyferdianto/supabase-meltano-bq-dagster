{{ config(materialized='table') }}

with source_data as (
    select 
        customer_id,
        customer_unique_id,
        customer_zip_code_prefix,
        customer_city,
        customer_state
    from {{ source('olist', 'customers') }}
),

enhanced_customers as (
    select 
        -- PRIMARY KEYS (warehouse compatibility - exactly as warehouse expects)
        customer_id,
        customer_unique_id,
        
        -- ENHANCED COLUMNS (warehouse gets improved quality data automatically)
        LPAD(CAST(customer_zip_code_prefix as STRING), 5, '0') as customer_zip_code_prefix,
        REGEXP_REPLACE(
            TRIM(UPPER(NORMALIZE(customer_city, NFD))), 
            r'[\\u0300-\\u036f]', 
            ''
        ) as customer_city,
        UPPER(TRIM(customer_state)) as customer_state,
        
        -- ORIGINAL BACKUP COLUMNS (raw data preserved with _original suffix)
        customer_zip_code_prefix as customer_zip_code_prefix_original,
        customer_city as customer_city_original,
        customer_state as customer_state_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when customer_zip_code_prefix is not null 
             and REGEXP_CONTAINS(LPAD(CAST(customer_zip_code_prefix as STRING), 5, '0'), r'^[0-9]{5}$')
             then true else false end as is_valid_zip_code,
             
        case when customer_city is not null 
             and LENGTH(TRIM(customer_city)) >= 2
             then true else false end as is_valid_city,
             
        case when customer_state is not null 
             and UPPER(TRIM(customer_state)) in ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')
             then true else false end as is_valid_state,
             
        -- COMPOSITE KEYS (for advanced uniqueness testing)
        CONCAT(
            COALESCE(customer_id, 'NULL'), '_',
            COALESCE(LPAD(CAST(customer_zip_code_prefix as STRING), 5, '0'), 'NULL'), '_',
            COALESCE(UPPER(TRIM(customer_state)), 'NULL')
        ) as composite_customer_key
        
    from source_data
)

select * from enhanced_customers
