-- Create lakehouse database
CREATE DATABASE IF NOT EXISTS lakehouse;

USE lakehouse;

-- Table 1: Daily Sales Summary
CREATE TABLE IF NOT EXISTS daily_sales_summary
(
    sale_date Date,
    total_revenue Decimal(15, 2),
    total_orders UInt64,
    unique_customers UInt64,
    unique_viewers UInt64,
    total_views UInt64,
    total_carts UInt64,
    avg_order_value Decimal(10, 2),
    total_items_purchased UInt64,
    conversion_rate Decimal(5, 2),
    cart_abandonment_rate Decimal(5, 2),
    _updated_at DateTime
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(sale_date)
ORDER BY sale_date
SETTINGS index_granularity = 8192;

-- Table 2: Product Performance
CREATE TABLE IF NOT EXISTS product_performance
(
    product_id Nullable(Int64),

    category_level_1 Nullable(String),
    category_level_2 Nullable(String),
    brand Nullable(String),

    total_views Nullable(Int64),
    unique_viewers Nullable(Int64),
    total_carts Nullable(Int64),
    unique_carters Nullable(Int64),
    total_purchases Nullable(Int64),
    unique_buyers Nullable(Int64),

    total_revenue Nullable(Float64),
    avg_price Nullable(Float64),

    view_to_cart_rate Nullable(Decimal(27, 2)),
    cart_to_purchase_rate Nullable(Decimal(27, 2)),
    overall_conversion_rate Nullable(Decimal(27, 2)),

    last_sold_date Nullable(Date),
    days_since_last_sale Nullable(Int32),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY
(
    ifNull(category_level_1, ''),
    ifNull(total_revenue, 0)
)
SETTINGS index_granularity = 8192;

-- Table 3: Category Performance
CREATE TABLE IF NOT EXISTS category_performance
(
    analysis_date Date,

    category_level_1 Nullable(String),
    category_level_2 Nullable(String),

    total_revenue Nullable(Float64),
    total_orders Nullable(Int64),
    unique_customers Nullable(Int64),

    avg_order_value Nullable(Float64),

    top_brand Nullable(String),
    top_brand_revenue Nullable(Float64),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(analysis_date)
ORDER BY (analysis_date, ifNull(category_level_1, ''),
    ifNull(total_revenue, 0))
SETTINGS index_granularity = 8192;


-- Table 4: User RFM Segments
CREATE TABLE IF NOT EXISTS user_rfm_segments
(
    user_id Int64,

    first_purchase_date Nullable(Date),
    last_purchase_date Nullable(Date),

    recency_days Nullable(Int32),
    frequency Nullable(Int64),

    monetary_value Nullable(Float64),
    avg_order_value Nullable(Float64),

    total_sessions Nullable(Int64),

    rfm_recency_score Nullable(Int32),
    rfm_frequency_score Nullable(Int32),
    rfm_monetary_score Nullable(Int32),

    rfm_segment Nullable(String),

    customer_lifetime_value Nullable(Float64),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY (
    ifNull(rfm_segment, ''),
    ifNull(monetary_value, 0)
)
SETTINGS index_granularity = 8192;


-- Table 5: Conversion Funnel Daily
CREATE TABLE IF NOT EXISTS conversion_funnel_daily
(
    analysis_date Date,

    category_level_1 Nullable(String),
    category_level_2 Nullable(String),

    time_of_day Nullable(String),
    is_weekend Nullable(UInt8),

    stage_1_view_users Nullable(Int64),
    stage_2_cart_users Nullable(Int64),
    stage_3_purchase_users Nullable(Int64),

    stage_1_view_events Nullable(Int64),
    stage_2_cart_events Nullable(Int64),
    stage_3_purchase_events Nullable(Int64),

    view_to_cart_rate Nullable(Decimal(27, 2)),
    cart_to_purchase_rate Nullable(Decimal(27, 2)),
    overall_conversion_rate Nullable(Decimal(27, 2)),

    dropoff_after_view Nullable(Int64),
    dropoff_after_cart Nullable(Int64),

    dropoff_rate_view Nullable(Decimal(27, 2)),
    dropoff_rate_cart Nullable(Decimal(27, 2)),

    total_revenue Nullable(Float64),
    avg_order_value Nullable(Float64),
    revenue_per_viewer Nullable(Float64),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(analysis_date)
ORDER BY
(
    analysis_date,
    ifNull(category_level_1, ''),
    ifNull(time_of_day, '')
)
SETTINGS index_granularity = 8192;


-- Table 6: User Journey Funnel
CREATE TABLE IF NOT EXISTS user_journey_funnel
(
    user_id Int64,

    analysis_period Nullable(String),

    first_view_date Nullable(Date),
    first_cart_date Nullable(Date),
    first_purchase_date Nullable(Date),
    last_activity_date Nullable(Date),

    has_viewed Nullable(Int32),
    has_added_to_cart Nullable(Int32),
    has_purchased Nullable(Int32),
    has_removed_from_cart Nullable(Int32),

    total_views Nullable(Int64),
    total_carts Nullable(Int64),
    total_purchases Nullable(Int64),
    total_cart_removals Nullable(Int64),
    total_sessions Nullable(Int64),

    days_to_first_cart Nullable(Int32),
    days_to_first_purchase Nullable(Int32),

    current_funnel_stage Nullable(String),

    is_cart_abandoner Nullable(UInt8),
    is_impulse_buyer Nullable(UInt8),
    is_researcher Nullable(UInt8),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
ORDER BY
(
    ifNull(current_funnel_stage, ''),
    ifNull(user_id, 0)
)
SETTINGS index_granularity = 8192;


-- Table 7: Hourly Traffic
CREATE TABLE IF NOT EXISTS hourly_traffic
(
    event_date Date,
    event_hour Int32,
    day_of_week Nullable(String),

    total_events Nullable(Int64),
    unique_users Nullable(Int64),
    total_views Nullable(Int64),
    total_carts Nullable(Int64),
    total_purchases Nullable(Int64),

    revenue Nullable(Float64),

    _updated_at Nullable(DateTime)
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(event_date)
ORDER BY (event_date, event_hour)
SETTINGS index_granularity = 8192;
