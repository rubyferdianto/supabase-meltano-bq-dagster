{{
  config(
    materialized='table',
    partition_by={
      'field': 'date_value',
      'data_type': 'date',
      'granularity': 'month'
    },
    cluster_by=['year', 'month']
  )
}}

with date_spine as (
    -- Generate comprehensive date dimension from 2016-2025 (covering Olist data period + future)
    select 
        date_value
    from unnest(generate_date_array('2016-01-01', '2025-12-31', interval 1 day)) as date_value
),

date_attributes as (
    select 
        date_value,
        
        -- Generate surrogate key in YYYYMMDD format
        cast(format_date('%Y%m%d', date_value) as int64) as date_sk,
        
        -- Extract date components
        extract(year from date_value) as year,
        extract(quarter from date_value) as quarter,
        extract(month from date_value) as month,
        extract(day from date_value) as day_of_month,
        extract(dayofweek from date_value) as day_of_week,
        extract(dayofyear from date_value) as day_of_year,
        
        -- Day name and month name
        format_date('%A', date_value) as day_name,
        format_date('%B', date_value) as month_name,
        
        -- Weekend indicator (Sunday = 1, Saturday = 7)
        case 
            when extract(dayofweek from date_value) in (1, 7) then true 
            else false 
        end as is_weekend,
        
        -- Brazilian business day indicator (excluding weekends)
        case 
            when extract(dayofweek from date_value) not in (1, 7) then true 
            else false 
        end as is_business_day,
        
        -- Season classification (Southern Hemisphere - Brazil)
        case 
            when extract(month from date_value) in (12, 1, 2) then 'Summer'
            when extract(month from date_value) in (3, 4, 5) then 'Autumn'
            when extract(month from date_value) in (6, 7, 8) then 'Winter'
            when extract(month from date_value) in (9, 10, 11) then 'Spring'
        end as season_brazil,
        
        -- Month classification
        case 
            when extract(month from date_value) = 12 then 'Holiday Season (Dec)'
            when extract(month from date_value) in (1, 2) then 'Summer Peak'
            when extract(month from date_value) in (6, 7) then 'Winter Period'
            when extract(month from date_value) in (11) then 'Black Friday Season'
            else 'Regular Season'
        end as seasonal_period,
        
        -- Week of month
        ceil(extract(day from date_value) / 7.0) as week_of_month,
        
        -- First/Last day of month indicators
        case 
            when extract(day from date_value) = 1 then true 
            else false 
        end as is_month_start,
        
        case 
            when date_value = last_day(date_value) then true 
            else false 
        end as is_month_end,
        
        -- Quarter boundaries
        case 
            when extract(month from date_value) in (1, 4, 7, 10) and extract(day from date_value) = 1 then true 
            else false 
        end as is_quarter_start,
        
        case 
            when extract(month from date_value) in (3, 6, 9, 12) and date_value = last_day(date_value) then true 
            else false 
        end as is_quarter_end,
        
        -- Year boundaries
        case 
            when extract(month from date_value) = 1 and extract(day from date_value) = 1 then true 
            else false 
        end as is_year_start,
        
        case 
            when extract(month from date_value) = 12 and extract(day from date_value) = 31 then true 
            else false 
        end as is_year_end,
        
        -- Audit timestamp
        current_datetime() as insertion_timestamp
        
    from date_spine
)

select 
    date_sk,
    date_value,
    year,
    quarter,
    month,
    day_of_month,
    day_of_week,
    day_of_year,
    day_name,
    month_name,
    is_weekend,
    is_business_day,
    season_brazil,
    seasonal_period,
    week_of_month,
    is_month_start,
    is_month_end,
    is_quarter_start,
    is_quarter_end,
    is_year_start,
    is_year_end,
    insertion_timestamp
from date_attributes
