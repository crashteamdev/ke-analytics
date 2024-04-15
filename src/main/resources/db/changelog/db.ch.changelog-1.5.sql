--liquibase formatted sql
--changeset vitaxa:add-title-to-product-daily-sales
DROP TABLE IF EXISTS kazanex.ke_product_daily_sales_mv;

ALTER TABLE kazanex.ke_product_daily_sales
    ADD COLUMN IF NOT EXISTS title AggregateFunction(anyLast, String);

CREATE MATERIALIZED VIEW IF NOT EXISTS kazanex.ke_product_daily_sales_mv
    TO kazanex.ke_product_daily_sales AS
SELECT toDate(timestamp)                 AS date,
       product_id                        AS product_id,
       sku_id                            AS sku_id,
       anyLastState(title)               AS title,
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
