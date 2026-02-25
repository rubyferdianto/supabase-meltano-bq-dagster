{{
  config(
    materialized='table',
    cluster_by=['review_score', 'data_quality_tier'],
    partition_by={
        'field': 'review_creation_date',
        'data_type': 'timestamp'
    }
  )
}}

with review_base as (
    select 
        -- Core fields
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp
        
    from {{ ref('stg_order_reviews') }}
),

review_enhanced as (
    select 
        *,
        
        -- Review sentiment classification
        case 
            when review_score >= 4 then 'Positive'
            when review_score = 3 then 'Neutral'
            when review_score <= 2 then 'Negative'
            else 'Unknown'
        end as review_sentiment,
        
        -- Detailed review score categories
        case 
            when review_score = 5 then 'Excellent (5 stars)'
            when review_score = 4 then 'Good (4 stars)'
            when review_score = 3 then 'Average (3 stars)'
            when review_score = 2 then 'Poor (2 stars)'
            when review_score = 1 then 'Terrible (1 star)'
            else 'No Rating'
        end as review_category,
        
        -- Review completeness classification
        case 
            when review_comment_title is not null and review_comment_message is not null then 'Full Review'
            when review_comment_title is not null or review_comment_message is not null then 'Partial Review'
            else 'Rating Only'
        end as review_completeness,
        
        -- Review response classification
        case 
            when review_answer_timestamp is not null then 'Answered'
            else 'Unanswered'
        end as response_status,
        
        -- Time to response (if answered)
        case 
            when review_answer_timestamp is not null and review_creation_date is not null then
                date_diff(date(review_answer_timestamp), date(review_creation_date), day)
            else null
        end as response_days,
        
        -- Response time classification
        case 
            when review_answer_timestamp is null then 'No Response'
            when date_diff(date(review_answer_timestamp), date(review_creation_date), day) <= 1 then 'Same/Next Day'
            when date_diff(date(review_answer_timestamp), date(review_creation_date), day) <= 7 then 'Within Week'
            when date_diff(date(review_answer_timestamp), date(review_creation_date), day) <= 30 then 'Within Month'
            else 'Over Month'
        end as response_timeliness,
        
        -- Review period classification
        extract(year from review_creation_date) as review_year,
        extract(month from review_creation_date) as review_month,
        extract(dayofweek from review_creation_date) as review_day_of_week,
        case 
            when extract(dayofweek from review_creation_date) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as review_day_type,
        
        -- Comment length analysis
        case 
            when review_comment_title is not null then length(review_comment_title)
            else 0
        end as title_length,
        
        case 
            when review_comment_message is not null then length(review_comment_message)
            else 0
        end as message_length,
        
        -- Data quality tier (simplified)
        case 
            when review_score is not null and review_creation_date is not null 
                 and (review_answer_timestamp is null or review_answer_timestamp > review_creation_date)
                then 'High Quality'
            when review_score is not null and review_creation_date is not null 
                then 'Medium Quality'
            when review_score is not null 
                then 'Low Quality'
            else 'Poor Quality'
        end as data_quality_tier,
        
        -- Quality completeness score (simplified 0-100)
        (
            case when review_score is not null then 30 else 0 end +
            case when review_creation_date is not null then 25 else 0 end +
            case when review_answer_timestamp is not null then 15 else 0 end +
            case when review_comment_title is not null then 10 else 0 end +
            case when review_comment_message is not null then 15 else 0 end +
            case when review_answer_timestamp is null or review_answer_timestamp > review_creation_date then 5 else 0 end
        ) as quality_completeness_score
        
    from review_base
),

review_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['review_id']) }} as review_sk,
        
        -- Natural key and attributes
        review_id,
        order_id,
        review_score,
        review_comment_title,
        review_comment_message,
        review_creation_date,
        review_answer_timestamp,
        
        -- Enhanced classification fields
        review_sentiment,
        review_category,
        review_completeness,
        response_status,
        response_days,
        response_timeliness,
        review_year,
        review_month,
        review_day_of_week,
        review_day_type,
        title_length,
        message_length,
        
        -- Quality fields (simplified)
        data_quality_tier,
        quality_completeness_score,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from review_enhanced
)

select 
    review_sk,
    review_id,
    order_id,
    review_score,
    review_comment_title,
    review_comment_message,
    review_creation_date,
    review_answer_timestamp,
    review_sentiment,
    review_category,
    review_completeness,
    response_status,
    response_days,
    response_timeliness,
    review_year,
    review_month,
    review_day_of_week,
    review_day_type,
    title_length,
    message_length,
    data_quality_tier,
    quality_completeness_score,
    insertion_timestamp
from review_with_sk
