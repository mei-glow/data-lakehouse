{{
    config(
        materialized='table'
    )
}}

WITH user_events AS (
    SELECT
        user_id,
        MIN(CASE WHEN event_type = 'view' THEN event_date END) AS first_view_date,
        MIN(CASE WHEN event_type = 'cart' THEN event_date END) AS first_cart_date,
        MIN(CASE WHEN event_type = 'purchase' THEN event_date END) AS first_purchase_date,
        MAX(event_date) AS last_activity_date,
        
        -- Stage flags
        MAX(CASE WHEN event_type = 'view' THEN 1 ELSE 0 END) AS has_viewed,
        MAX(CASE WHEN event_type = 'cart' THEN 1 ELSE 0 END) AS has_added_to_cart,
        MAX(CASE WHEN event_type = 'purchase' THEN 1 ELSE 0 END) AS has_purchased,
        MAX(CASE WHEN event_type = 'remove_from_cart' THEN 1 ELSE 0 END) AS has_removed_from_cart,
        
        -- Event counts
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        COUNT(CASE WHEN event_type = 'remove_from_cart' THEN 1 END) AS total_cart_removals,
        
        COUNT(DISTINCT user_session) AS total_sessions,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM {{ ref('silver_ecommerce_events') }}
    WHERE user_id IS NOT NULL
    GROUP BY user_id
),

with_journey_metrics AS (
    SELECT
        *,
        'lifetime' AS analysis_period,
        
        -- Time analysis
        DATEDIFF(first_cart_date, first_view_date) AS days_to_first_cart,
        DATEDIFF(first_purchase_date, first_view_date) AS days_to_first_purchase,
        
        -- Funnel stage
        CASE
            WHEN has_purchased = 1 AND total_purchases >= 3 THEN 'repeat_buyer'
            WHEN has_purchased = 1 THEN 'purchaser'
            WHEN has_added_to_cart = 1 AND has_purchased = 0 THEN 'cart_abandoner'
            WHEN has_viewed = 1 AND has_added_to_cart = 0 THEN 'viewer_only'
            ELSE 'unknown'
        END AS current_funnel_stage,
        
        -- Behavior flags
        CASE WHEN has_added_to_cart = 1 AND has_purchased = 0 THEN TRUE ELSE FALSE END AS is_cart_abandoner,
        
        CASE 
            WHEN has_purchased = 1 AND DATEDIFF(first_purchase_date, first_view_date) <= 0 THEN TRUE 
            ELSE FALSE 
        END AS is_impulse_buyer,
        
        CASE WHEN total_views >= 5 THEN TRUE ELSE FALSE END AS is_researcher
        
    FROM user_events
)

SELECT * FROM with_journey_metrics
WHERE user_id IS NOT NULL
ORDER BY total_purchases DESC, total_views DESC