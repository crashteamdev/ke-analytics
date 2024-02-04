--liquibase formatted sql
--changeset vitaxa:create-product-daily-sales
CREATE TABLE IF NOT EXISTS kazanex.ke_product_daily_sales
(
    date              Date,
    product_id        UInt64,
    sku_id            UInt64,
    category_id       UInt64,
    min_total_order_amount AggregateFunction(min, UInt64),
    max_total_order_amount AggregateFunction(max, UInt64),
    min_available_amount AggregateFunction(min, UInt64),
    max_available_amount AggregateFunction(max, UInt64),
    median_full_price AggregateFunction(quantile, Nullable(UInt64)),
    median_price AggregateFunction(quantile, UInt64),
    seller_id         UInt64,
    seller_account_id UInt64,
    seller_title AggregateFunction(max, String),
    seller_link       String,
    seller_registration_date AggregateFunction(max, DateTime),
    rating AggregateFunction(max, Decimal(9, 2)),
    is_eco AggregateFunction(max, UInt8),
    is_adult AggregateFunction(max, UInt8)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (product_id, sku_id, date);

CREATE MATERIALIZED VIEW IF NOT EXISTS kazanex.ke_product_daily_sales_mv
            TO kazanex.ke_product_daily_sales AS
SELECT toDate(timestamp)                 AS date,
       product_id                        AS product_id,
       sku_id                            AS sku_id,
       max(latest_category_id)           AS category_id,
       minState(total_orders_amount)     AS min_total_order_amount,
       maxState(total_orders_amount)     AS max_total_order_amount,
       minState(available_amount)        AS min_available_amount,
       maxState(available_amount)        AS max_available_amount,
       quantileState(full_price)         AS median_full_price,
       quantileState(purchase_price)     AS median_price,
       max(seller_id)                    AS seller_id,
       max(seller_account_id)            AS seller_account_id,
       maxState(seller_title)            AS seller_title,
       max(seller_link)                  AS seller_link,
       maxState(seller_registrationDate) AS seller_registration_date,
       maxState(rating)                  AS rating,
       maxState(is_eco)                  AS is_eco,
       maxState(is_adult)                AS is_adult
FROM kazanex.product
GROUP BY product_id, sku_id, date;
