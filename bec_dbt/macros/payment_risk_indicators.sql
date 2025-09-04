{% macro payment_risk_indicators() %}
    case 
        when payment_installments >= 12 then 'high_risk'
        when payment_installments >= 6 then 'medium_risk'
        when payment_installments >= 3 then 'low_risk'
        else 'minimal_risk'
    end as payment_risk_level,
    
    -- Credit behavior classification
    case 
        when payment_type = 'credit_card' and payment_installments = 1 then 'credit_single_payment'
        when payment_type = 'credit_card' and payment_installments <= 6 then 'credit_short_term'
        when payment_type = 'credit_card' and payment_installments <= 12 then 'credit_medium_term'
        when payment_type = 'credit_card' and payment_installments > 12 then 'credit_long_term'
        when payment_type = 'debit_card' then 'debit_immediate'
        when payment_type = 'boleto' then 'boleto_traditional'
        when payment_type = 'voucher' then 'voucher_discount'
        else 'other_payment'
    end as credit_behavior_type
{% endmacro %}
