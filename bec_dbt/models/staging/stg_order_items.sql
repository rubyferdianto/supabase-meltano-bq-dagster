{{ config(materialized='table') }}

with source_data as (
    select 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value
    from {{ source('olist', 'order_items') }}
),

enhanced_order_items as (
    select 
        -- PRIMARY KEYS 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        shipping_limit_date,            -- Keep datetime as-is for accuracy
        ROUND(price, 2) as price,       -- Standardize to 2 decimal places
        ROUND(freight_value, 2) as freight_value,  -- Standardize to 2 decimal places
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        shipping_limit_date as shipping_limit_date_original,
        price as price_original,
        freight_value as freight_value_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when price is not null 
             and price > 0.01 
             and price <= 10000.00
             then true else false end as is_valid_price,
             
        case when freight_value is not null 
             and freight_value >= 0.00 
             and freight_value <= 1000.00
             then true else false end as is_valid_freight,
             
        case when order_item_id is not null 
             and order_item_id >= 1 
             and order_item_id <= 50
             then true else false end as is_valid_item_sequence,
             
        case when shipping_limit_date is not null 
             and shipping_limit_date >= TIMESTAMP('2016-01-01') 
             and shipping_limit_date <= CURRENT_TIMESTAMP()
             then true else false end as is_valid_shipping_date,
             
        -- COMPOSITE KEY (for advanced uniqueness testing)
        CONCAT(
            COALESCE(order_id, 'NULL'), '_',
            COALESCE(CAST(order_item_id as STRING), 'NULL'), '_',
            COALESCE(product_id, 'NULL'), '_',
            COALESCE(seller_id, 'NULL')
        ) as composite_key
        
    from source_data
    where order_id is not null
        and order_item_id is not null
        and product_id is not null
        and seller_id is not null
)

select * from enhanced_order_items
