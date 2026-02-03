
  
    
        create table silver_gold.gold_user_rfm_segments
      
      
    using parquet
      
      
      
      
      
      

      as
      

WITH user_purchase_history AS (
    SELECT
        user_id,

        MIN(CASE WHEN event_type = 'purchase' THEN event_date END) AS first_purchase_date,
        MAX(CASE WHEN event_type = 'purchase' THEN event_date END) AS last_purchase_date,

        COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) AS frequency,
        SUM(CASE WHEN event_type = 'purchase' THEN price ELSE 0 END) AS monetary_value,

        COUNT(DISTINCT user_session) AS total_sessions,

        -- Recency (days since last purchase)
        DATEDIFF(
            CURRENT_DATE(),
            MAX(CASE WHEN event_type = 'purchase' THEN event_date END)
        ) AS recency_days

    FROM silver.silver_ecommerce_events
    WHERE user_id IS NOT NULL
    GROUP BY user_id
    HAVING COUNT(CASE WHEN event_type = 'purchase' THEN 1 END) > 0
),

rfm_base AS (
    SELECT
        *,
        ROUND(monetary_value / NULLIF(frequency, 0), 2) AS avg_order_value
    FROM user_purchase_history
),

rfm_scores AS (
    SELECT
        *,

        -- Quantile-based RFM scores (1–5)
        NTILE(5) OVER (ORDER BY recency_days ASC)  AS rfm_recency_score,
        NTILE(5) OVER (ORDER BY frequency DESC)    AS rfm_frequency_score,
        NTILE(5) OVER (ORDER BY monetary_value DESC) AS rfm_monetary_score,

        CURRENT_TIMESTAMP() AS _updated_at

    FROM rfm_base
),

with_segments AS (
    SELECT
        *,

        CASE
            WHEN rfm_recency_score >= 4
             AND rfm_frequency_score >= 4
             AND rfm_monetary_score >= 4
                THEN 'Champions'

            WHEN rfm_recency_score >= 3
             AND rfm_frequency_score >= 3
             AND rfm_monetary_score >= 3
                THEN 'Loyal Customers'

            WHEN rfm_recency_score >= 4
             AND rfm_frequency_score <= 2
                THEN 'Potential Loyalists'

            WHEN rfm_recency_score <= 2
             AND rfm_frequency_score >= 4
                THEN 'At Risk'

            WHEN rfm_recency_score <= 2
             AND rfm_frequency_score >= 3
             AND rfm_monetary_score >= 4
                THEN 'Cannot Lose Them'

            WHEN rfm_recency_score <= 2
             AND rfm_frequency_score <= 2
                THEN 'Hibernating'

            WHEN rfm_recency_score = 1
             AND rfm_frequency_score = 1
                THEN 'Lost'

            ELSE 'New Customers'
        END AS rfm_segment,

        -- Approximate CLV
        ROUND(monetary_value * 1.5, 2) AS customer_lifetime_value

    FROM rfm_scores
)

SELECT *
FROM with_segments
ORDER BY monetary_value DESC;
  