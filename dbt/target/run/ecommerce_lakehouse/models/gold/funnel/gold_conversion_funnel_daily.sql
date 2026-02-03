
  
    
        create table silver_gold.gold_conversion_funnel_daily
      
      
    using parquet
      
      
      partitioned by (analysis_date)
      
      
      

      as
      

WITH funnel_metrics AS (
    SELECT
        event_date AS analysis_date,
        category_level_1,
        category_level_2,
        time_of_day,
        is_weekend,
        
        -- Unique users per stage
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS stage_1_view_users,
        COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS stage_2_cart_users,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS stage_3_purchase_users,
        
        -- Total events per stage
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS stage_1_view_events,
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS stage_2_cart_events,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS stage_3_purchase_events,
        
        -- Revenue
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM silver.silver_ecommerce_events
    
    
    
    GROUP BY event_date, category_level_1, category_level_2, time_of_day, is_weekend
),

with_rates AS (
    SELECT
        *,
        
        -- Conversion rates
        ROUND(
            stage_2_cart_users * 100.0 / NULLIF(stage_1_view_users, 0),
            2
        ) AS view_to_cart_rate,
        
        ROUND(
            stage_3_purchase_users * 100.0 / NULLIF(stage_2_cart_users, 0),
            2
        ) AS cart_to_purchase_rate,
        
        ROUND(
            stage_3_purchase_users * 100.0 / NULLIF(stage_1_view_users, 0),
            2
        ) AS overall_conversion_rate,
        
        -- Drop-offs
        (stage_1_view_users - stage_2_cart_users) AS dropoff_after_view,
        (stage_2_cart_users - stage_3_purchase_users) AS dropoff_after_cart,
        
        ROUND(
            (stage_1_view_users - stage_2_cart_users) * 100.0 / NULLIF(stage_1_view_users, 0),
            2
        ) AS dropoff_rate_view,
        
        ROUND(
            (stage_2_cart_users - stage_3_purchase_users) * 100.0 / NULLIF(stage_2_cart_users, 0),
            2
        ) AS dropoff_rate_cart,
        
        -- Revenue metrics
        ROUND(
            total_revenue / NULLIF(stage_3_purchase_users, 0),
            2
        ) AS avg_order_value,
        
        ROUND(
            total_revenue / NULLIF(stage_1_view_users, 0),
            2
        ) AS revenue_per_viewer
        
    FROM funnel_metrics
)

SELECT * FROM with_rates
WHERE stage_1_view_users > 0
ORDER BY analysis_date, total_revenue DESC
  