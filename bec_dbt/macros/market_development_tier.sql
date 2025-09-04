{% macro market_development_tier() %}
    case 
        when total_revenue >= 100000 and total_customers >= 1000 then 'tier_1_developed'
        when total_revenue >= 50000 and total_customers >= 500 then 'tier_2_growing'
        when total_revenue >= 20000 and total_customers >= 200 then 'tier_3_emerging'
        when total_revenue >= 5000 and total_customers >= 50 then 'tier_4_developing'
        else 'tier_5_nascent'
    end as market_development_tier
{% endmacro %}
