{{ config(materialized='table') }}

with source as (
    select * from {{ source('olist', 'order_reviews') }}
),
deduplicated as (
    select 
        *,
        count(*) over (partition by review_id) as duplicate_count,
        row_number() over (
            partition by review_id
            order by 
                case when review_creation_date is not null then 0 else 1 end,
                review_creation_date desc,
                case when review_answer_timestamp is not null then 0 else 1 end,
                review_answer_timestamp desc
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
        review_id,
        order_id,
        
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        CAST(review_score AS INT64) as review_score,    -- Ensure integer type for testing
        TRIM(review_comment_title) as review_comment_title,     -- Clean whitespace
        TRIM(review_comment_message) as review_comment_message, -- Clean whitespace
        review_creation_date,           -- Keep datetime as-is for accuracy
        review_answer_timestamp,        -- Keep timestamp as-is for accuracy
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        review_score as review_score_original,
        review_comment_title as review_comment_title_original,
        review_comment_message as review_comment_message_original,
        review_creation_date as review_creation_date_original,
        review_answer_timestamp as review_answer_timestamp_original,
        
        -- VALIDATION FLAGS (systematic boolean naming convention)
        case when review_score is not null 
             and review_score >= 1 
             and review_score <= 5
             then true else false end as is_valid_review_score,
        case when review_comment_title is not null 
             and LENGTH(TRIM(review_comment_title)) >= 1 
             and LENGTH(TRIM(review_comment_title)) <= 500
             then true else false end as is_valid_comment_title,
        case when review_comment_message is not null 
             and LENGTH(TRIM(review_comment_message)) >= 1 
             and LENGTH(TRIM(review_comment_message)) <= 5000
             then true else false end as is_valid_comment_message,
        case when review_creation_date is not null 
             and SAFE_CAST(review_creation_date as TIMESTAMP) is not null
             then true else false end as is_valid_creation_date,
        case when review_answer_timestamp is null 
             or SAFE_CAST(review_answer_timestamp as TIMESTAMP) is not null
             then true else false end as is_valid_answer_timestamp,
             
        -- COMPOSITE KEY (for advanced uniqueness testing)
        CONCAT(
            COALESCE(review_id, 'NULL'), '_',
            COALESCE(order_id, 'NULL')
        ) as composite_review_key,
        
        -- Type conversions for timestamp fields
        safe_cast(review_creation_date as timestamp) as review_creation_date_clean,
        safe_cast(review_answer_timestamp as timestamp) as review_answer_timestamp_clean,
        
        -- Data quality flags
        case when review_id is null then true else false end as missing_review_id,
        case when order_id is null then true else false end as missing_order_id,
        case when review_score is null then true else false end as missing_review_score,
        case when review_creation_date is null then true else false end as missing_creation_date,
        case when safe_cast(review_creation_date as timestamp) is null and review_creation_date is not null then true else false end as invalid_creation_date,
        case when safe_cast(review_answer_timestamp as timestamp) is null and review_answer_timestamp is not null then true else false end as invalid_answer_timestamp,
        case when review_score not in (1, 2, 3, 4, 5) then true else false end as invalid_review_score,
        case when review_score < 1 or review_score > 5 then true else false end as out_of_range_score,
        
        -- Business logic flags
        case when safe_cast(review_answer_timestamp as timestamp) < safe_cast(review_creation_date as timestamp) then true else false end as answer_before_creation,
        case when length(review_comment_title) > 200 then true else false end as long_title,
        case when length(review_comment_message) > 1000 then true else false end as long_message,
        
        -- Audit trail
        had_duplicates,
        current_timestamp() as ingestion_timestamp
        
    from unique_records
)

select * from staging
