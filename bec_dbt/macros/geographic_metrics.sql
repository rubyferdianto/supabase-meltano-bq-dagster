{% macro geographic_metrics() %}
    -- Distance and logistics
    case 
        when c.customer_state = s.seller_state then 'same_state'
        when c.customer_state in ('SP', 'RJ', 'MG', 'ES') and s.seller_state in ('SP', 'RJ', 'MG', 'ES') then 'southeast_region'
        when c.customer_state in ('RS', 'SC', 'PR') and s.seller_state in ('RS', 'SC', 'PR') then 'south_region'
        else 'cross_region'
    end as shipping_complexity,
    
    -- Market concentration
    case 
        when c.customer_state = 'SP' then 'sao_paulo_market'
        when c.customer_state in ('RJ', 'MG') then 'major_southeast'
        when c.customer_state in ('RS', 'PR', 'SC') then 'south_market'
        else 'other_markets'
    end as market_segment
{% endmacro %}
