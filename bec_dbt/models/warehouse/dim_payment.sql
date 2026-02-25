{{
  config(
    materialized='table',
    cluster_by=['payment_type', 'data_quality_tier']
  )
}}

with payment_base as (
    select 
        -- Core fields
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value
        
    from {{ ref('stg_order_payments') }}
),

payment_enhanced as (
    select 
        *,
        
        -- Payment method classification
        case 
            when payment_type = 'credit_card' then 'Credit Card'
            when payment_type = 'boleto' then 'Bank Slip (Boleto)'
            when payment_type = 'voucher' then 'Voucher'
            when payment_type = 'debit_card' then 'Debit Card'
            when payment_type = 'not_defined' then 'Undefined'
            else 'Other'
        end as payment_method_category,
        
        -- Payment complexity classification
        case 
            when payment_installments = 1 then 'Single Payment'
            when payment_installments between 2 and 3 then 'Short Term Installments'
            when payment_installments between 4 and 6 then 'Medium Term Installments'
            when payment_installments between 7 and 12 then 'Long Term Installments'
            when payment_installments > 12 then 'Extended Installments'
            else 'Unknown'
        end as installment_category,
        
        -- Payment value range classification
        case 
            when payment_value <= 50 then 'Low Value (≤R$50)'
            when payment_value <= 150 then 'Medium Value (R$50-150)'
            when payment_value <= 500 then 'High Value (R$150-500)'
            when payment_value <= 1500 then 'Premium Value (R$500-1500)'
            when payment_value > 1500 then 'Luxury Value (>R$1500)'
            else 'Unknown'
        end as payment_value_tier,
        
        -- Average installment value
        case 
            when payment_installments > 0 and payment_value is not null then 
                round(payment_value / payment_installments, 2)
            else null
        end as avg_installment_value,
        
        -- Data quality tier (simplified)
        case 
            when payment_type is not null and payment_sequential is not null and payment_installments is not null 
                 and payment_value is not null and payment_value > 0 then 'High Quality'
            when payment_type is not null and payment_sequential is not null and payment_value is not null 
                then 'Medium Quality'
            when payment_type is not null and payment_sequential is not null 
                then 'Low Quality'
            else 'Poor Quality'
        end as data_quality_tier,
        
        -- Quality completeness score (simplified 0-100)
        (
            case when payment_type is not null then 30 else 0 end +
            case when payment_sequential is not null then 20 else 0 end +
            case when payment_installments is not null then 20 else 0 end +
            case when payment_value is not null and payment_value > 0 then 25 else 0 end +
            case when payment_installments > 0 and payment_value > 0 then 5 else 0 end
        ) as quality_completeness_score
        
    from payment_base
),

payment_with_sk as (
    select 
        -- Generate surrogate key
        {{ generate_surrogate_key(['order_id', 'payment_sequential']) }} as payment_sk,
        
        -- Natural key and attributes
        order_id,
        payment_sequential,
        payment_type,
        payment_installments,
        payment_value,
        
        -- Enhanced classification fields
        payment_method_category,
        installment_category,
        payment_value_tier,
        avg_installment_value,
        
        -- Quality fields (simplified)
        data_quality_tier,
        quality_completeness_score,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from payment_enhanced
)

select 
    payment_sk,
    order_id,
    payment_sequential,
    payment_type,
    payment_installments,
    payment_value,
    payment_method_category,
    installment_category,
    payment_value_tier,
    avg_installment_value,
    data_quality_tier,
    quality_completeness_score,
    insertion_timestamp
from payment_with_sk
