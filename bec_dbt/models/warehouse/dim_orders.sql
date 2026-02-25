{{
  config(
    materialized='table',
    cluster_by=['order_status', 'data_quality_tier'],
    partition_by={
        'field': 'order_purchase_timestamp',
        'data_type': 'timestamp'
    }
  )
}}

with order_base as (
    select 
        -- Core fields (using the _clean versions)
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp_clean as order_purchase_timestamp,
        order_approved_at_clean as order_approved_at,
        order_delivered_carrier_date_clean as order_delivered_carrier_date,
        order_delivered_customer_date_clean as order_delivered_customer_date,
        order_estimated_delivery_date_clean as order_estimated_delivery_date
        
    from {{ ref('stg_orders') }}
),

order_enhanced as (
    select 
        *,
        
        -- Order lifecycle classification
        case 
            when order_status = 'delivered' then 'Complete'
            when order_status = 'shipped' then 'In Transit'
            when order_status = 'approved' then 'Processing'
            when order_status = 'invoiced' then 'Invoiced'
            when order_status = 'processing' then 'Processing'
            when order_status = 'canceled' then 'Canceled'
            when order_status = 'unavailable' then 'Canceled'
            else 'Other'
        end as order_lifecycle_stage,
        
        -- Delivery status classification
        case 
            when order_status = 'delivered' and order_delivered_customer_date is not null then 'Successfully Delivered'
            when order_status = 'shipped' then 'In Delivery'
            when order_status in ('canceled', 'unavailable') then 'Not Delivered'
            when order_status in ('approved', 'invoiced', 'processing') then 'Pre-Delivery'
            else 'Unknown Status'
        end as delivery_status,
        
        -- Time period classifications
        extract(year from order_purchase_timestamp) as order_year,
        extract(month from order_purchase_timestamp) as order_month,
        extract(dayofweek from order_purchase_timestamp) as order_day_of_week,
        case 
            when extract(dayofweek from order_purchase_timestamp) in (1, 7) then 'Weekend'
            else 'Weekday'
        end as order_day_type,
        
        -- Delivery performance (if delivered)
        case 
            when order_status = 'delivered' and order_delivered_customer_date is not null 
                 and order_estimated_delivery_date is not null then
                date_diff(date(order_delivered_customer_date), date(order_estimated_delivery_date), day)
            else null
        end as delivery_days_vs_estimate,
        
        case 
            when order_status = 'delivered' and order_delivered_customer_date is not null 
                 and order_estimated_delivery_date is not null then
                case 
                    when date_diff(date(order_delivered_customer_date), date(order_estimated_delivery_date), day) <= 0 then 'On Time or Early'
                    when date_diff(date(order_delivered_customer_date), date(order_estimated_delivery_date), day) <= 3 then 'Slightly Late'
                    when date_diff(date(order_delivered_customer_date), date(order_estimated_delivery_date), day) <= 7 then 'Moderately Late'
                    else 'Very Late'
                end
            else 'Not Applicable'
        end as delivery_performance,
        
        -- Data quality tier (simplified - based on data presence)
        case 
            when order_status is not null and order_purchase_timestamp is not null 
                 and (order_status not in ('delivered', 'shipped') or 
                     (order_delivered_customer_date is not null and order_estimated_delivery_date is not null))
                then 'High Quality'
            when order_status is not null and order_purchase_timestamp is not null 
                then 'Medium Quality'
            when order_status is not null 
                then 'Low Quality'
            else 'Poor Quality'
        end as data_quality_tier,
        
        -- Quality completeness score (simplified - 0-100, no circular refs)
        (
            case when order_status is not null then 25 else 0 end +
            case when order_purchase_timestamp is not null then 25 else 0 end +
            case when order_approved_at is not null then 15 else 0 end +
            case when order_delivered_carrier_date is not null then 15 else 0 end +
            case when order_delivered_customer_date is not null then 20 else 0 end
        ) as quality_completeness_score
        
    from order_base
),

order_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['order_id']) }} as order_sk,
        
        -- Natural key and attributes
        order_id,
        customer_id,
        order_status,
        order_purchase_timestamp,
        order_approved_at,
        order_delivered_carrier_date,
        order_delivered_customer_date,
        order_estimated_delivery_date,
        
        -- Enhanced classification fields
        order_lifecycle_stage,
        delivery_status,
        order_year,
        order_month,
        order_day_of_week,
        order_day_type,
        delivery_days_vs_estimate,
        delivery_performance,
        
        -- Quality fields (simplified)
        data_quality_tier,
        quality_completeness_score,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from order_enhanced
)

select 
    order_sk,
    order_id,
    customer_id,
    order_status,
    order_purchase_timestamp,
    order_approved_at,
    order_delivered_carrier_date,
    order_delivered_customer_date,
    order_estimated_delivery_date,
    order_lifecycle_stage,
    delivery_status,
    order_year,
    order_month,
    order_day_of_week,
    order_day_type,
    delivery_days_vs_estimate,
    delivery_performance,
    data_quality_tier,
    quality_completeness_score,
    insertion_timestamp
from order_with_sk
