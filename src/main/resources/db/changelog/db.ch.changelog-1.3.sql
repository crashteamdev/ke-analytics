CREATE TABLE IF NOT EXISTS kazanex.category_daily_stats
(
    date        Date,
    category_id UInt64,
    orders AggregateFunction(sum, Int64),
    available_amount AggregateFunction(sum, UInt64),
    median_price_all AggregateFunction(quantile, Float64),
    median_price_with_sales AggregateFunction(quantile, Float64),
    revenue AggregateFunction(sum, Float64),
    seller_count AggregateFunction(uniq, UInt64),
    product_count AggregateFunction(uniq, UInt64),
    seller_with_sales AggregateFunction(uniqExactIfState, UInt64, UInt8),
    product_with_sales AggregateFunction(uniqExactIfState, UInt64, UInt8)
)
    ENGINE = AggregatingMergeTree
        ORDER BY (category_id, date);

CREATE MATERIALIZED VIEW IF NOT EXISTS kazanex.category_daily_stats_mv
            TO kazanex.category_daily_stats AS
SELECT date,
       c_id                                                  AS category_id,
       sumState(final_order_amount)                          AS orders,
       sumState(available_amount)                            AS available_amount,
       quantileState(median_price)                           AS median_price_all,
       quantileIfState(median_price, final_order_amount > 0) AS median_price_with_sales,
       sumState(revenue)                                     AS revenue,
       uniqState(seller_id)                                  AS seller_count,
       uniqState(product_id)                                 AS product_count,
       uniqExactIfState(seller_id, final_order_amount > 0)   AS seller_with_sales,
       uniqExactIfState(product_id, final_order_amount > 0)  AS product_with_sales
FROM (
         SELECT date,
                p.product_id,
                anyLast(p.category_id)                                                                           AS c_id,
                maxMerge(p.max_total_order_amount)                                                               AS max_total_order_amount,
                minMerge(p.min_total_order_amount)                                                               AS min_total_order_amount,
                max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                lagInFrame(maxMerge(p.max_total_order_amount))
                           over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                multiIf(min_total_order_amount < max_total_order_amount_delta,
                        daily_order_amount - (max_total_order_amount_delta - min_total_order_amount),
                        max_total_order_amount_delta - max_total_order_amount >= 0,
                        daily_order_amount + (max_total_order_amount_delta - max_total_order_amount),
                        max_total_order_amount_delta > 0 AND min_total_order_amount > max_total_order_amount_delta,
                        daily_order_amount + (min_total_order_amount - max_total_order_amount_delta),
                        daily_order_amount)                                                                      AS final_order_amount,
                median_price * final_order_amount                                                                AS revenue,
                minMerge(p.min_available_amount)                                                                 AS available_amount,
                quantileMerge(p.median_price)                                                                    AS median_price,
                anyLast(seller_id)                                                                               AS seller_id
         FROM kazanex.ke_product_daily_sales p
         GROUP BY product_id, date)
GROUP BY c_id, date;

