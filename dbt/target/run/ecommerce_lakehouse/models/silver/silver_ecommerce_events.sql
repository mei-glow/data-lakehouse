
  
    
        create table silver.silver_ecommerce_events
      
      
    using parquet
      
      
      partitioned by (event_date)
      
      
      

      as
      

WITH source_data AS (
    SELECT *
    FROM bronze.ecommerce_events
    
    
),

cleaned_events AS (
    SELECT
        -- Generate unique ID
        MD5(CONCAT(
            CAST(user_id AS STRING),
            COALESCE(CAST(product_id AS STRING), 'NULL'),
            event_type,
            event_time
        )) AS event_unique_id,
        
        -- Parse timestamp
        CAST(event_time AS TIMESTAMP) AS event_timestamp,
        DATE(CAST(event_time AS TIMESTAMP)) AS event_date,
        HOUR(CAST(event_time AS TIMESTAMP)) AS event_hour,
        DAYOFWEEK(CAST(event_time AS TIMESTAMP)) AS day_of_week_num,
        
        -- Clean event type
        LOWER(TRIM(event_type)) AS event_type,
        
        -- IDs (user_id required, product_id nullable)
        user_id,
        TRIM(user_session) AS user_session,
        product_id,  -- Nullable OK
        
        -- Parse category hierarchy
        CASE 
            WHEN category_code IS NOT NULL THEN SPLIT(category_code, '\\.')[0]
            ELSE NULL 
        END AS category_level_1,
        
        CASE 
            WHEN category_code IS NOT NULL AND SIZE(SPLIT(category_code, '\\.')) >= 2 
            THEN SPLIT(category_code, '\\.')[1]
            ELSE NULL 
        END AS category_level_2,
        
        CASE 
            WHEN category_code IS NOT NULL AND SIZE(SPLIT(category_code, '\\.')) >= 3 
            THEN SPLIT(category_code, '\\.')[2]
            ELSE NULL 
        END AS category_level_3,
        
        -- Standardize brand
        CASE 
            WHEN brand IS NOT NULL THEN UPPER(TRIM(brand))
            ELSE NULL 
        END AS brand_standardized,
        
        -- Price validation
        CASE 
            WHEN price IS NULL THEN NULL
            WHEN price < 0 THEN NULL  -- Invalid prices → NULL
            ELSE ROUND(price, 2)
        END AS price_clean,
        
        -- Derived: Price bucket
        CASE 
            WHEN price IS NULL THEN 'UNKNOWN'
            WHEN price < 50 THEN '0-50'
            WHEN price < 100 THEN '50-100'
            WHEN price < 200 THEN '100-200'
            WHEN price < 500 THEN '200-500'
            ELSE '500+'
        END AS price_bucket,
        
        -- Derived: Time of day
        CASE 
            WHEN HOUR(CAST(event_time AS TIMESTAMP)) >= 6 AND HOUR(CAST(event_time AS TIMESTAMP)) < 12 THEN 'MORNING'
            WHEN HOUR(CAST(event_time AS TIMESTAMP)) >= 12 AND HOUR(CAST(event_time AS TIMESTAMP)) < 18 THEN 'AFTERNOON'
            WHEN HOUR(CAST(event_time AS TIMESTAMP)) >= 18 AND HOUR(CAST(event_time AS TIMESTAMP)) < 22 THEN 'EVENING'
            ELSE 'NIGHT'
        END AS time_of_day,
        
        -- Derived: Is weekend
        CASE 
            WHEN DAYOFWEEK(CAST(event_time AS TIMESTAMP)) IN (1, 7) THEN TRUE  -- Sunday=1, Saturday=7
            ELSE FALSE 
        END AS is_weekend,
        
        -- Metadata
        _ingestion_time AS _bronze_ingestion_time,
        _source_file,
        CURRENT_TIMESTAMP() AS _silver_processed_at,
        _processing_date
        
    FROM source_data
    WHERE user_id IS NOT NULL  -- User ID is required
),

-- Remove duplicates
deduped_events AS (
    SELECT *
    FROM (
        SELECT 
            *,
            ROW_NUMBER() OVER (
                PARTITION BY event_unique_id 
                ORDER BY _bronze_ingestion_time DESC
            ) AS row_num
        FROM cleaned_events
    ) ranked
    WHERE row_num = 1
)

SELECT
    event_unique_id,
    event_timestamp,
    event_date,
    event_hour,
    day_of_week_num,
    
    event_type,
    user_id,
    user_session,
    product_id,
    
    category_level_1,
    category_level_2,
    category_level_3,
    
    brand_standardized AS brand,
    price_clean AS price,
    price_bucket,
    
    time_of_day,
    is_weekend,
    
    _bronze_ingestion_time,
    _source_file,
    _silver_processed_at,
    _processing_date
    
FROM deduped_events
  