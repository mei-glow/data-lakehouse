{{
    config(
        materialized='incremental',
        partition_by=['analysis_date'],
        incremental_strategy='append',
        unique_key=['analysis_date', 'category_level_1', 'category_level_2']
    )
}}

-- 1️⃣ Daily category metrics
WITH category_daily AS (
    SELECT
        event_date AS analysis_date,
        category_level_1,
        category_level_2,
        
        -- Revenue metrics
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS total_revenue,
        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS total_orders,
        COUNT(DISTINCT CASE WHEN event_type = 'purchase' THEN user_id END) AS unique_customers,
        
        ROUND(
            SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) /
            NULLIF(COUNT(CASE WHEN event_type = 'purchase' THEN 1 END), 0),
            2
        ) AS avg_order_value,
        
        CURRENT_TIMESTAMP() AS _updated_at
        
    FROM {{ ref('silver_ecommerce_events') }}
    WHERE category_level_1 IS NOT NULL
    
    {% if is_incremental() %}
    AND event_date > (SELECT MAX(analysis_date) FROM {{ this }})
    {% endif %}
    
    GROUP BY event_date, category_level_1, category_level_2
),

-- 2️⃣ Brand revenue per day per category
brand_daily_revenue AS (
    SELECT
        event_date AS analysis_date,
        category_level_1,
        category_level_2,
        brand,
        SUM(price) AS brand_revenue
    FROM {{ ref('silver_ecommerce_events') }}
    WHERE event_type = 'purchase'
      AND brand IS NOT NULL
      AND category_level_1 IS NOT NULL
    GROUP BY event_date, category_level_1, category_level_2, brand
),

-- 3️⃣ Rank brands by revenue
ranked_brands AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY analysis_date, category_level_1, category_level_2
            ORDER BY brand_revenue DESC
        ) AS brand_rank
    FROM brand_daily_revenue
),

-- 4️⃣ Pick top brand per category per day
top_brand_per_category AS (
    SELECT
        analysis_date,
        category_level_1,
        category_level_2,
        brand AS top_brand,
        brand_revenue AS top_brand_revenue
    FROM ranked_brands
    WHERE brand_rank = 1
)

-- 5️⃣ Final output
SELECT
    cd.*,
    tb.top_brand,
    tb.top_brand_revenue
FROM category_daily cd
LEFT JOIN top_brand_per_category tb
  ON cd.analysis_date = tb.analysis_date
 AND cd.category_level_1 = tb.category_level_1
 AND cd.category_level_2 = tb.category_level_2
ORDER BY cd.analysis_date, cd.total_revenue DESC
