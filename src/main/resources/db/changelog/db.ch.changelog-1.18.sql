--liquibase formatted sql
--changeset vitaxa:add-ke-product-daily-sales-daily-counters-projection
ALTER TABLE kazanex.ke_product_daily_sales
    ADD PROJECTION IF NOT EXISTS product_daily_sales_daily_counters_projection
    (
        SELECT
            category_id,
            date,
            product_id,
            maxMerge(max_total_order_amount) AS max_total_order_amount,
            minMerge(min_total_order_amount) AS min_total_order_amount
        GROUP BY
            category_id, date, product_id
    );

ALTER TABLE kazanex.ke_product_daily_sales
    MATERIALIZE PROJECTION product_daily_sales_daily_counters_projection;
