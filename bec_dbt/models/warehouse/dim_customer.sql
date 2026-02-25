{{
  config(
    materialized='table',
    cluster_by=['customer_state', 'customer_region']
  )
}}

/*
    Customer Dimension - Enhanced with Direct Data Cleaning & Transformation
    
    Structure: Staging → Warehouse (no intermediate)
    Features:
    - Deduplication logic directly in warehouse layer
    - Brazilian text normalization (already done in staging)
    - Quality validation flags preserved
    - Regional analysis support
    - Master record selection strategy
*/

with customer_order_stats as (
    select 
        c.customer_id,
        c.customer_unique_id,
        max(o.order_purchase_timestamp) as last_order_date,
        count(o.order_id) as total_orders,
        sum(coalesce(oi.price, 0) + coalesce(oi.freight_value, 0)) as total_order_value
    from {{ ref('stg_customers') }} c
    left join {{ ref('stg_orders') }} o on c.customer_id = o.customer_id
    left join {{ ref('stg_order_items') }} oi on o.order_id = oi.order_id
    group by c.customer_id, c.customer_unique_id
),

customer_quality_score as (
    select 
        customer_id,
        customer_unique_id,
        -- Use boolean validation flags for quality scoring
        (case when is_valid_zip_code then 1 else 0 end +
         case when is_valid_city then 1 else 0 end +
         case when is_valid_state then 1 else 0 end) as quality_score
    from {{ ref('stg_customers') }}
),

-- Master customer selection (deduplication logic)
customer_master_selection as (
    select 
        c.*,
        cos.last_order_date,
        coalesce(cos.total_orders, 0) as total_orders,
        coalesce(cos.total_order_value, 0) as total_order_value,
        cqs.quality_score,
        
        -- Master selection ranking
        row_number() over (
            partition by c.customer_unique_id 
            order by 
                cos.last_order_date desc nulls last,
                cqs.quality_score desc,
                cos.total_order_value desc nulls last,
                c.customer_id asc
        ) as master_rank
    from {{ ref('stg_customers') }} c
    left join customer_order_stats cos on c.customer_id = cos.customer_id
    left join customer_quality_score cqs on c.customer_id = cqs.customer_id
),

-- Final warehouse dimension with business enrichment
customer_dimension as (
    select 
        -- Generate surrogate key (matching teammate's pattern)
        {{ generate_surrogate_key(['customer_id']) }} as customer_sk,
        
        -- Business keys
        customer_id,
        customer_unique_id,
        
        -- Geographic dimensions (enhanced from Quality-First staging)
        customer_zip_code_prefix,          -- Already normalized (5-digit)
        customer_city,                     -- Already normalized (Brazilian NFD)
        customer_state,                    -- Already normalized (uppercase)
        
        -- Regional enrichment
        case 
            when customer_state in ('SP', 'RJ', 'MG', 'ES') then 'Southeast'
            when customer_state in ('PR', 'SC', 'RS') then 'South'
            when customer_state in ('BA', 'SE', 'AL', 'PE', 'PB', 'RN', 'CE', 'PI', 'MA') then 'Northeast'
            when customer_state in ('GO', 'DF', 'TO', 'MT', 'MS') then 'Central-West'
            when customer_state in ('AM', 'RR', 'AP', 'PA', 'AC', 'RO') then 'North'
            else 'Unknown'
        end as customer_region,
        
        -- Business intelligence attributes
        case 
            when total_orders >= 3 then 'High Value'
            when total_orders = 2 then 'Medium Value' 
            when total_orders = 1 then 'One-time'
            else 'New Customer'
        end as customer_segment,
        
        case 
            when is_valid_zip_code and is_valid_city and is_valid_state then 'High Quality'
            when quality_score >= 2 then 'Medium Quality'
            when quality_score >= 1 then 'Low Quality'
            else 'Poor Quality'
        end as data_quality_tier,
        
        -- Customer metrics
        total_orders,
        total_order_value,
        last_order_date,
        
        -- Quality flags (preserved from staging)
        is_valid_zip_code,
        is_valid_city,
        is_valid_state,
        quality_score,
        
        -- Audit trail (preserved from staging)
        customer_zip_code_prefix_original,
        customer_city_original,
        customer_state_original,
        composite_customer_key,
        
        -- System fields
        current_datetime() as insertion_timestamp
        
    from customer_master_selection
    where master_rank = 1  -- Only master records (deduplication complete)
)

select * from customer_dimension
