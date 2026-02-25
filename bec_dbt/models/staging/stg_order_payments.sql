{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'order_payments') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by order_id, payment_sequential) as duplicate_count,
        row_number() over (
            partition by order_id, payment_sequential
            order by 
                payment_value desc,
                payment_type
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
staging as (
    select
        -- Primary keys
        order_id,
        payment_sequential,
        
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        UPPER(TRIM(payment_type)) as payment_type,  -- Normalize payment types
        payment_installments,           -- Keep installments as-is
        ROUND(payment_value, 2) as payment_value,  -- Standardize to 2 decimal places
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        payment_type as payment_type_original,
        payment_installments as payment_installments_original,
        payment_value as payment_value_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when payment_type is not null 
             and UPPER(TRIM(payment_type)) in ('CREDIT_CARD', 'BOLETO', 'VOUCHER', 'DEBIT_CARD', 'NOT_DEFINED')
             then true else false end as is_valid_payment_type,
        case when payment_installments is not null 
             and payment_installments >= 1 
             and payment_installments <= 24
             then true else false end as is_valid_installments,
        case when payment_value is not null 
             and payment_value > 0.00 
             and payment_value <= 50000.00
             then true else false end as is_valid_payment_value,
        case when payment_sequential is not null 
             and payment_sequential >= 1 
             and payment_sequential <= 10
             then true else false end as is_valid_payment_sequence,
             
        -- COMPOSITE KEY (for advanced uniqueness testing)
        CONCAT(
            COALESCE(order_id, 'NULL'), '_',
            COALESCE(CAST(payment_sequential as STRING), 'NULL')
        ) as composite_payment_key,
        
        -- QUALITY FLAGS (for backwards compatibility with existing logic)
        case when order_id is null then true else false end as missing_order_id,
        case when payment_sequential is null then true else false end as missing_payment_sequential,
        case when payment_type is null then true else false end as missing_payment_type,
        case when payment_installments is null then true else false end as missing_payment_installments,
        case when payment_value is null then true else false end as missing_payment_value,
        case when payment_value < 0 then true else false end as negative_payment_value,
        case when payment_installments < 1 then true else false end as invalid_installments,
        
        -- Audit trail
        had_duplicates,
        current_timestamp() as ingestion_timestamp
        
    from unique_records
)

select * from staging
