DROP TABLE IF EXISTS kazanex.category_daily_stats_mv;

CREATE MATERIALIZED VIEW IF NOT EXISTS kazanex.category_daily_stats_mv
    TO kazanex.category_daily_stats AS
SELECT date,
       c_id                                                   AS category_id,
       sumStateIf(final_order_amount, final_order_amount > 0) AS orders,
       sumState(available_amount)                             AS available_amount,
       quantileState(median_price)                            AS median_price_all,
       quantileIfState(median_price, final_order_amount > 0)  AS median_price_with_sales,
       sumState(revenue)                                      AS revenue,
       uniqState(seller_id)                                   AS seller_count,
       uniqState(product_id)                                  AS product_count,
       uniqExactIfState(seller_id, final_order_amount > 0)    AS seller_with_sales,
       uniqExactIfState(product_id, final_order_amount > 0)   AS product_with_sales
FROM (
         SELECT date,
                toUInt64(p.product_id)                                                                           AS product_id,
                anyLast(p.latest_category_id)                                                                    AS c_id,
                max(p.total_orders_amount)                                                                       AS max_total_order_amount,
                min(p.total_orders_amount)                                                                       AS min_total_order_amount,
                max_total_order_amount - min_total_order_amount                                                  AS daily_order_amount,
                lagInFrame(max(p.total_orders_amount))
                           over (partition by product_id order by date ROWS BETWEEN 1 PRECEDING AND 1 PRECEDING) AS max_total_order_amount_delta,
                multiIf(min_total_order_amount < max_total_order_amount_delta,
                        daily_order_amount - (max_total_order_amount_delta - min_total_order_amount),
                        max_total_order_amount_delta - max_total_order_amount >= 0,
                        daily_order_amount + (max_total_order_amount_delta - max_total_order_amount),
                        max_total_order_amount_delta > 0 AND min_total_order_amount > max_total_order_amount_delta,
                        daily_order_amount + (min_total_order_amount - max_total_order_amount_delta),
                        daily_order_amount)                                                                      AS final_order_amount,
                median_price * final_order_amount                                                                AS revenue,
                min(p.total_available_amount)                                                                    AS available_amount,
                quantile(p.purchase_price)                                                                       AS median_price,
                anyLast(seller_id)                                                                               AS seller_id
         FROM kazanex.product p
         GROUP BY product_id, toDate(timestamp) AS date)
GROUP BY c_id, date;

