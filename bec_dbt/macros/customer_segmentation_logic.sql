{% macro customer_segmentation_logic() %}
    case 
        when total_orders >= 5 and total_spent >= 500 then 'champion'
        when total_orders >= 3 and total_spent >= 300 then 'loyal_customer'
        when total_orders >= 2 and total_spent >= 150 then 'potential_loyalist'
        when total_orders = 1 and total_spent >= 100 then 'new_customer_high_value'
        when total_orders = 1 and total_spent < 100 then 'new_customer_low_value'
        when days_since_last_order > 365 then 'hibernating'
        when days_since_last_order > 180 then 'at_risk'
        else 'needs_attention'
    end as customer_segment
{% endmacro %}
