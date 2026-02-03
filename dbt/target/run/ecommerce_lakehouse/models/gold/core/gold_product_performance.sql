
  
    
        create table silver_gold.gold_product_performance
      
      
    using parquet
      
      
      partitioned by (category_level_1)
      
      
      

      as
      

WITH product_metrics AS (
    SELECT
        product_id,
        MAX(category_level_1) AS category_level_1,
        MAX(category_level_2) AS category_level_2,
        MAX(brand) AS brand,
        
        -- Event counts
        COUNT(CASE WHEN event_type = 'view' THEN 1 END) AS total_views,
        COUNT(DISTINCT CASE WHEN event_type = 'view' THEN user_id END) AS unique_viewers,
        
        COUNT(CASE WHEN event_type = 'cart' THEN 1 END) AS total_carts,
        COUNT(DISTINCT CASE WHEN event_type = 'cart' THEN user_id END) AS unique_carters,
        
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_purchases,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_buyers,
        
        -- Revenue metrics
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue,
        ROUND(AVG(CASE WHEN event_type = 'purchase' THEN price END), 2) AS avg_price,
        
        -- Last sold
        MAX(CASE WHEN event_type = 'purchase' THEN event_date END) AS last_sold_date,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM silver.silver_ecommerce_events
    WHERE product_id IS NOT NULL
    GROUP BY product_id
),

with_conversion_rates AS (
    SELECT
        *,
        
        -- Conversion rates
        ROUND(
            total_carts * 100.0 / NULLIF(total_views, 0),
            2
        ) AS view_to_cart_rate,
        
        ROUND(
            total_purchases * 100.0 / NULLIF(total_carts, 0),
            2
        ) AS cart_to_purchase_rate,
        
        ROUND(
            total_purchases * 100.0 / NULLIF(total_views, 0),
            2
        ) AS overall_conversion_rate,
        
        -- Days since last sale
        DATEDIFF(CURRENT_DATE(), last_sold_date) AS days_since_last_sale
        
    FROM product_metrics
)

SELECT * FROM with_conversion_rates
WHERE product_id IS NOT NULL
ORDER BY total_revenue DESC
  