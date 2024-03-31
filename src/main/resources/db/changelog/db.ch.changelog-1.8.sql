--liquibase formatted sql
--changeset vitaxa:delete-category-stats-table
DROP TABLE IF EXISTS kazanex.category_daily_stats;
DROP TABLE IF EXISTS kazanex.category_daily_stats_mv;

--changeset vitaxa:add-ke-product-daily-category-projection
ALTER TABLE kazanex.ke_product_daily_sales ADD PROJECTION IF NOT EXISTS product_daily_sales_category_id_projection (
SELECT
    *
ORDER BY category_id, date
    );

ALTER TABLE kazanex.ke_product_daily_sales MATERIALIZE PROJECTION IF NOT EXISTS product_daily_sales_category_id_projection;
