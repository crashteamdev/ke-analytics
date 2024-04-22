--liquibase formatted sql
--changeset vitaxa:add-ke-product-daily-seller-link-projection
ALTER TABLE kazanex.ke_product_daily_sales ADD PROJECTION IF NOT EXISTS product_daily_sales_seller_link_projection (
SELECT
    *
ORDER BY seller_link, date
    );
