{{
    config(
        materialized='incremental',
        partition_by=['event_date'],
        incremental_strategy='append',
        unique_key=['event_date', 'event_hour']
    )
}}

WITH hourly_metrics AS (
    SELECT
        event_date,
        event_hour,
        
        -- Day info
        CASE day_of_week_num
            WHEN 1 THEN 'Sunday'
            WHEN 2 THEN 'Monday'
            WHEN 3 THEN 'Tuesday'
            WHEN 4 THEN 'Wednesday'
            WHEN 5 THEN 'Thursday'
            WHEN 6 THEN 'Friday'
            WHEN 7 THEN 'Saturday'
        END AS day_of_week,
        
        -- Traffic metrics
        COUNT(*) AS total_events,
        COUNT(DISTINCT user_id) AS unique_users,
        
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        
        -- Revenue
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS revenue,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM {{ ref('silver_ecommerce_events') }}
    
    {% if is_incremental() %}
    WHERE event_date > (SELECT MAX(event_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY event_date, event_hour, day_of_week_num
)

SELECT * FROM hourly_metrics
ORDER BY event_date, event_hour