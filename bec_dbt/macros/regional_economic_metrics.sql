{% macro regional_economic_metrics() %}
    -- Market concentration
    round(total_revenue / nullif(total_customers, 0), 2) as revenue_per_customer,
    round(total_orders / nullif(total_customers, 0), 2) as orders_per_customer,
    round(total_revenue / nullif(total_orders, 0), 2) as average_order_value,
    
    -- Market efficiency
    round(total_revenue / nullif(total_sellers, 0), 2) as revenue_per_seller,
    round(total_customers / nullif(total_sellers, 0), 2) as customers_per_seller,
    
    -- Economic activity indicators
    round(total_revenue / nullif(days_active, 0), 2) as daily_revenue_rate,
    round(total_orders / nullif(days_active, 0), 2) as daily_order_rate
{% endmacro %}
