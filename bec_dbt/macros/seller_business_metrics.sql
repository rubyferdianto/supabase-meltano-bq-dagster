{% macro seller_business_metrics() %}
    -- Revenue efficiency
    round(total_revenue / nullif(total_orders, 0), 2) as revenue_per_order,
    round(total_revenue / nullif(days_active, 0), 2) as daily_revenue_rate,
    round(total_orders / nullif(days_active, 0), 2) as daily_order_rate,
    
    -- Product performance  
    round(total_items_sold / nullif(unique_products_sold, 0), 2) as avg_sales_per_product,
    round(unique_products_sold / nullif(total_orders, 0), 2) as product_diversity_per_order,
    
    -- Market reach
    round(cross_state_sales / nullif(total_orders, 0) * 100, 2) as cross_state_sales_pct,
    round(total_orders / nullif(unique_customers, 0), 2) as customer_repeat_rate
{% endmacro %}
