{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'products') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by product_id) as duplicate_count,
        row_number() over (
            partition by product_id
            order by 
                case when product_category_name is not null then 0 else 1 end,
                case when product_weight_g is not null then 0 else 1 end,
                product_weight_g desc,
                product_category_name
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
        product_id,
        
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        coalesce(product_category_name, 'unknown') as product_category_name,
        coalesce(product_name_lenght, -1) as product_name_lenght,        -- Keep original misspelling for compatibility
        coalesce(product_description_lenght, -1) as product_description_lenght,  -- Keep original misspelling
        coalesce(product_photos_qty, 0) as product_photos_qty,
        coalesce(product_weight_g, 0) as product_weight_g,               -- Default to 0 for missing weights
        coalesce(product_length_cm, 0) as product_length_cm,             -- Default to 0 for missing dimensions
        coalesce(product_height_cm, 0) as product_height_cm,
        coalesce(product_width_cm, 0) as product_width_cm,
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        product_category_name as product_category_name_original,
        product_name_lenght as product_name_lenght_original,
        product_description_lenght as product_description_lenght_original,
        product_photos_qty as product_photos_qty_original,
        product_weight_g as product_weight_g_original,
        product_length_cm as product_length_cm_original,
        product_height_cm as product_height_cm_original,
        product_width_cm as product_width_cm_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when product_id is null then false else true end as is_valid_product_id,
        case when product_category_name is null then false else true end as is_valid_category,
        case when product_name_lenght is null or product_name_lenght < 0 then false else true end as is_valid_name_length,
        case when product_description_lenght is null or product_description_lenght < 0 then false else true end as is_valid_description_length,
        case when product_photos_qty is null or product_photos_qty < 0 then false else true end as is_valid_photos_qty,
        case when product_weight_g is null or product_weight_g <= 0 or product_weight_g > 50000 then false else true end as is_valid_weight,
        case when product_length_cm is null or product_length_cm <= 0 or product_length_cm > 300 then false else true end as is_valid_length,
        case when product_height_cm is null or product_height_cm <= 0 or product_height_cm > 300 then false else true end as is_valid_height,
        case when product_width_cm is null or product_width_cm <= 0 or product_width_cm > 300 then false else true end as is_valid_width,
        
        -- COMPOSITE KEY (for advanced uniqueness testing)
        CONCAT(
            COALESCE(product_id, 'NULL'), '_',
            COALESCE(product_category_name, 'NULL')
        ) as composite_product_key,
        
        -- Audit fields
        had_duplicates,
        current_timestamp() as ingestion_timestamp
    from unique_records
)

select * from with_quality_flags
