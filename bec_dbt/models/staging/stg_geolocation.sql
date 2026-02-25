{{ config(materialized='table') }}

with source_data as (
    select 
        geolocation_zip_code_prefix,
        geolocation_lat,
        geolocation_lng,
        geolocation_city,
        geolocation_state
    from {{ source('olist', 'geolocation') }}
),

enhanced_geolocation as (
    select 
        -- ENHANCED COLUMNS AS PRIMARY FIELDS (warehouse gets enhanced data)
        LPAD(CAST(geolocation_zip_code_prefix as STRING), 5, '0') as geolocation_zip_code_prefix,
        SAFE_CAST(geolocation_lat as FLOAT64) as geolocation_lat,
        SAFE_CAST(geolocation_lng as FLOAT64) as geolocation_lng,
        REGEXP_REPLACE(
            REGEXP_REPLACE(
                REGEXP_REPLACE(
                    TRIM(UPPER(
                        REGEXP_REPLACE(
                            TRANSLATE(
                                NORMALIZE(COALESCE(geolocation_city, ''), NFD),
                                'ÀÁÂÃÄÅàáâãäåÒÓÔÕÖØòóôõöøÈÉÊËèéêëÇçÌÍÎÏìíîïÙÚÛÜùúûüÿÑñ£',
                                'AAAAAAaaaaaaoOOOOOoooooooEEEEeeeeeCcIIIIiiiiUUUUuuuuyNnA'
                            ),
                            r'[^A-Za-z\s]', ''  -- Remove any remaining special characters completely
                        )
                    )),
                    r'SANO PAULO', 'SAO PAULO'  -- Fix the specific £ case
                ),
                r'\s+', ' '  -- Replace multiple spaces with single space
            ),
            r'^\s+|\s+$', ''  -- Trim leading and trailing spaces
        ) as geolocation_city,
        UPPER(TRIM(geolocation_state)) as geolocation_state,
        
        -- ORIGINAL RAW DATA (with _original suffix for reference)
        geolocation_zip_code_prefix as geolocation_zip_code_prefix_original,
        SAFE_CAST(geolocation_lat as FLOAT64) as geolocation_lat_original,
        SAFE_CAST(geolocation_lng as FLOAT64) as geolocation_lng_original,
        geolocation_city as geolocation_city_original,
        geolocation_state as geolocation_state_original,
        
        -- VALIDATION FLAGS (for advanced quality testing)
        case when geolocation_zip_code_prefix is not null 
             and REGEXP_CONTAINS(LPAD(CAST(geolocation_zip_code_prefix as STRING), 5, '0'), r'^[0-9]{5}$')
             then true else false end as valid_zip_code,
             
        case when geolocation_lat is not null 
             and SAFE_CAST(geolocation_lat as FLOAT64) between -35.0 and 5.0
             then true else false end as valid_lat_brazil,
             
        case when geolocation_lng is not null 
             and SAFE_CAST(geolocation_lng as FLOAT64) between -75.0 and -30.0
             then true else false end as valid_lng_brazil,
             
        case when geolocation_city is not null 
             and LENGTH(TRIM(geolocation_city)) >= 2
             then true else false end as valid_city,
             
        case when geolocation_state is not null 
             and UPPER(TRIM(geolocation_state)) in ('AC', 'AL', 'AP', 'AM', 'BA', 'CE', 'DF', 'ES', 'GO', 'MA', 'MT', 'MS', 'MG', 'PA', 'PB', 'PR', 'PE', 'PI', 'RJ', 'RN', 'RS', 'RO', 'RR', 'SC', 'SP', 'SE', 'TO')
             then true else false end as valid_state,
             
        -- COMPOSITE KEYS (for advanced uniqueness testing)
        CONCAT(
            COALESCE(LPAD(CAST(geolocation_zip_code_prefix as STRING), 5, '0'), 'NULL'), '_',
            COALESCE(CAST(geolocation_lat as STRING), 'NULL'), '_',
            COALESCE(CAST(geolocation_lng as STRING), 'NULL')
        ) as composite_geo_key
        
    from source_data
    where geolocation_zip_code_prefix is not null
        and geolocation_lat is not null 
        and geolocation_lng is not null
)

select * from enhanced_geolocation
