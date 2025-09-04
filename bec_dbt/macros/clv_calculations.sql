{% macro clv_calculations() %}
    -- Basic CLV components
    round(total_spent / nullif(total_orders, 0), 2) as avg_order_value,
    round(total_spent / nullif(days_as_customer, 0) * 365, 2) as annual_spending_rate,
    round(total_orders / nullif(days_as_customer, 0) * 365, 2) as annual_order_frequency,
    
    -- Predictive CLV (simplified)
    round(
        (total_spent / nullif(total_orders, 0)) * 
        (total_orders / nullif(days_as_customer, 0) * 365) * 
        case 
            when avg_review_score >= 4 then 2.5  -- High satisfaction multiplier
            when avg_review_score >= 3 then 1.8  -- Medium satisfaction 
            when avg_review_score >= 2 then 1.2  -- Low satisfaction
            else 1.0  -- No review data
        end, 2
    ) as predicted_annual_clv
{% endmacro %}
