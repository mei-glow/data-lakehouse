{{
    config(
        materialized='incremental',
        partition_by=['sale_date'],
        incremental_strategy='append',
        unique_key='sale_date'
    )
}}

WITH daily_metrics AS (
    SELECT
        event_date AS sale_date,
        
        -- Revenue metrics
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_orders,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_customers,
        
        -- Average order value
        ROUND(
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) / 
            NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0),
            2
        ) AS avg_order_value,
        
        -- Items purchased
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_items_purchased,
        
        -- Traffic metrics
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS unique_viewers,
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        
        -- Conversion rates
        ROUND(
            COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) * 100.0 / 
            NULLIF(COUNT(CASE WHEN event_type = 'view' THEN 1 END), 0),
            2
        ) AS conversion_rate,
        
        ROUND(
            (COUNT(CASE WHEN event_type = 'cart' THEN 1 END) - 
             COUNT(CASE WHEN event_type = 'purchase' THEN 1 END)) * 100.0 / 
            NULLIF(COUNT(CASE WHEN event_type = 'cart' THEN 1 END), 0),
            2
        ) AS cart_abandonment_rate,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM {{ ref('silver_ecommerce_events') }}
    
    {% if is_incremental() %}
    WHERE event_date > (SELECT MAX(sale_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY event_date
)

SELECT * FROM daily_metrics
ORDER BY sale_date