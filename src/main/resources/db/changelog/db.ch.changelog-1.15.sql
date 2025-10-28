--liquibase formatted sql
--changeset vitaxa:category-aggregate-optimization-uniq
CREATE TABLE IF NOT EXISTS kazanex.category_daily_uniqs
(
    `date`         Date,
    `category_id`  UInt64,
    `seller_state`  AggregateFunction(uniqCombined, UInt64),
    `product_state` AggregateFunction(uniqCombined, UInt64)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date);

CREATE MATERIALIZED VIEW IF NOT EXISTS kazanex.category_daily_uniqs_mv
            TO kazanex.category_daily_uniqs
AS
SELECT
    date,
    category_id,
    uniqCombinedState(seller_id)  AS seller_state,
    uniqCombinedState(product_id) AS product_state
FROM kazanex.ke_product_daily_sales
GROUP BY category_id, date;

