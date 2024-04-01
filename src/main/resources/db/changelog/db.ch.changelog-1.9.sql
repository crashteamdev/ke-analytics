--liquibase formatted sql
--changeset vitaxa:add-category-aggregate-tables
CREATE TABLE IF NOT EXISTS kazanex.category_weekly_stats
(
    date                     Date,
    category_id              UInt64,
    order_amount             String,
    available_amount         UInt64,
    revenue                  UInt64,
    median_price             Decimal64(2),
    avg_bill                 Decimal64(2),
    seller_count             UInt32,
    product_count            UInt32,
    order_per_product        Decimal32(2),
    order_per_seller         Decimal32(2),
    revenue_per_product      Decimal64(2),
    prev_order_amount        String,
    prev_available_amount    UInt64,
    prev_revenue             UInt64,
    prev_median_price        Decimal64(2),
    prev_avg_bill            Decimal64(2),
    prev_seller_count        UInt32,
    prev_product_count       UInt32,
    prev_order_per_product   Decimal32(2),
    prev_order_per_seller    Decimal32(2),
    prev_revenue_per_product Decimal64(2)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_two_week_stats
(
    date                     Date,
    category_id              UInt64,
    order_amount             String,
    available_amount         UInt64,
    revenue                  UInt64,
    median_price             Decimal64(2),
    avg_bill                 Decimal64(2),
    seller_count             UInt32,
    product_count            UInt32,
    order_per_product        Decimal32(2),
    order_per_seller         Decimal32(2),
    revenue_per_product      Decimal64(2),
    prev_order_amount        String,
    prev_available_amount    UInt64,
    prev_revenue             UInt64,
    prev_median_price        Decimal64(2),
    prev_avg_bill            Decimal64(2),
    prev_seller_count        UInt32,
    prev_product_count       UInt32,
    prev_order_per_product   Decimal32(2),
    prev_order_per_seller    Decimal32(2),
    prev_revenue_per_product Decimal64(2)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_monthly_stats
(
    date                     Date,
    category_id              UInt64,
    order_amount             String,
    available_amount         UInt64,
    revenue                  UInt64,
    median_price             Decimal64(2),
    avg_bill                 Decimal64(2),
    seller_count             UInt32,
    product_count            UInt32,
    order_per_product        Decimal32(2),
    order_per_seller         Decimal32(2),
    revenue_per_product      Decimal64(2),
    prev_order_amount        String,
    prev_available_amount    UInt64,
    prev_revenue             UInt64,
    prev_median_price        Decimal64(2),
    prev_avg_bill            Decimal64(2),
    prev_seller_count        UInt32,
    prev_product_count       UInt32,
    prev_order_per_product   Decimal32(2),
    prev_order_per_seller    Decimal32(2),
    prev_revenue_per_product Decimal64(2)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date)
        TTL date + INTERVAL 3 DAY DELETE;

CREATE TABLE IF NOT EXISTS kazanex.category_two_month_stats
(
    date                     Date,
    category_id              UInt64,
    order_amount             String,
    available_amount         UInt64,
    revenue                  UInt64,
    median_price             Decimal64(2),
    avg_bill                 Decimal64(2),
    seller_count             UInt32,
    product_count            UInt32,
    order_per_product        Decimal32(2),
    order_per_seller         Decimal32(2),
    revenue_per_product      Decimal64(2),
    prev_order_amount        String,
    prev_available_amount    UInt64,
    prev_revenue             UInt64,
    prev_median_price        Decimal64(2),
    prev_avg_bill            Decimal64(2),
    prev_seller_count        UInt32,
    prev_product_count       UInt32,
    prev_order_per_product   Decimal32(2),
    prev_order_per_seller    Decimal32(2),
    prev_revenue_per_product Decimal64(2)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date)
        TTL date + INTERVAL 3 DAY DELETE;
