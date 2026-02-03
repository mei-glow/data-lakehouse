
  
    
        create table silver_gold.gold_daily_sales_summary
      
      
    using parquet
      
      
      partitioned by (sale_date)
      
      
      

      as
      

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
        
    FROM silver.silver_ecommerce_events
    
    
    
    GROUP BY event_date
)

SELECT * FROM daily_metrics
ORDER BY sale_date
  