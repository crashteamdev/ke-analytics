--liquibase formatted sql
--changeset vitaxa:create-aggregated-category-product-tables
CREATE TABLE IF NOT EXISTS kazanex.category_product_weekly_stats
(
    date        Date,
    category_id UInt64,
    product_id  String,
    title       AggregateFunction(anyLast, String),
    max_total_order_amount AggregateFunction(max, Int64),
    min_total_order_amount AggregateFunction(min, Int64),
    median_price AggregateFunction(quantile, Int64),
    available_amount AggregateFunction(anyLast, UInt64),
    reviews_amount AggregateFunction(anyLast, UInt32),
    photo_key AggregateFunction(anyLast, String),
    rating AggregateFunction(anyLast, Decimal32(2))
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, product_id)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_product_two_week_stats
(
    date        Date,
    category_id UInt64,
    product_id  String,
    title       AggregateFunction(anyLast, String),
    max_total_order_amount AggregateFunction(max, Int64),
    min_total_order_amount AggregateFunction(min, Int64),
    median_price AggregateFunction(quantile, Int64),
    available_amount AggregateFunction(anyLast, UInt64),
    reviews_amount AggregateFunction(anyLast, UInt32),
    photo_key AggregateFunction(anyLast, String),
    rating AggregateFunction(anyLast, Decimal32(2))
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, product_id)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_product_monthly_stats
(
    date        Date,
    category_id UInt64,
    product_id  String,
    title       AggregateFunction(anyLast, String),
    max_total_order_amount AggregateFunction(max, Int64),
    min_total_order_amount AggregateFunction(min, Int64),
    median_price AggregateFunction(quantile, Int64),
    available_amount AggregateFunction(anyLast, UInt64),
    reviews_amount AggregateFunction(anyLast, UInt32),
    photo_key AggregateFunction(anyLast, String),
    rating AggregateFunction(anyLast, Decimal32(2))
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, product_id)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_product_two_month_stats
(
    date        Date,
    category_id UInt64,
    product_id  String,
    title       AggregateFunction(anyLast, String),
    max_total_order_amount AggregateFunction(max, Int64),
    min_total_order_amount AggregateFunction(min, Int64),
    median_price AggregateFunction(quantile, Int64),
    available_amount AggregateFunction(anyLast, UInt64),
    reviews_amount AggregateFunction(anyLast, UInt32),
    photo_key AggregateFunction(anyLast, String),
    rating AggregateFunction(anyLast, Decimal32(2))
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, product_id)
        TTL date + INTERVAL 3 DAY DELETE;
