{% macro seller_performance_tiers() %}
    case 
        when total_revenue >= 10000 and avg_review_score >= 4.5 then 'top_performer'
        when total_revenue >= 5000 and avg_review_score >= 4.0 then 'high_performer'
        when total_revenue >= 2000 and avg_review_score >= 3.5 then 'good_performer'
        when total_revenue >= 500 and avg_review_score >= 3.0 then 'average_performer'
        when avg_review_score < 3.0 and total_orders >= 10 then 'underperformer'
        else 'new_seller'
    end as performance_tier
{% endmacro %}
