{% macro payment_analytics_metrics() %}
    -- Installment analysis
    round(allocated_payment / nullif(payment_installments, 0), 2) as payment_per_installment,
    round((allocated_payment - item_price) / nullif(item_price, 0) * 100, 2) as payment_premium_pct,
    
    -- Payment timing value
    case 
        when payment_installments = 1 then allocated_payment
        else round(allocated_payment / power(1.02, payment_installments - 1), 2)  -- Approximate NPV with 2% monthly discount
    end as payment_present_value,
    
    -- Payment burden analysis
    case 
        when item_price <= 50 then 'low_value_purchase'
        when item_price <= 200 then 'medium_value_purchase'  
        when item_price <= 500 then 'high_value_purchase'
        else 'premium_purchase'
    end as purchase_value_category
{% endmacro %}
