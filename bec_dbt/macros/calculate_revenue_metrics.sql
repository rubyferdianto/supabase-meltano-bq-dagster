{% macro calculate_revenue_metrics() %}
    -- Core revenue measures
    f.price as item_price,
    f.freight_value as freight_cost,
    f.payment_value as allocated_payment,
    (f.price + f.freight_value) as total_item_cost,
    
    -- Calculated revenue metrics
    round(f.payment_value - f.price, 2) as payment_premium,
    round((f.payment_value / nullif(f.price, 0) - 1) * 100, 2) as payment_markup_pct,
    round(f.freight_value / nullif(f.price, 0) * 100, 2) as freight_to_price_ratio_pct,
    
    -- Revenue per unit metrics
    round(f.price / nullif(f.payment_installments, 0), 2) as price_per_installment,
    case 
        when f.payment_installments > 1 then 'installment'
        else 'single_payment'
    end as payment_behavior_type
{% endmacro %}
