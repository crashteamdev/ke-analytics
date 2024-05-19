--liquibase formatted sql
--changeset vitaxa:modify_product_aggregate_available_amount_type_v2
ALTER TABLE kazanex.category_product_two_month_stats DROP COLUMN IF EXISTS available_amount_sum;

ALTER TABLE kazanex.category_product_monthly_stats DROP COLUMN IF EXISTS available_amount_sum;

ALTER TABLE kazanex.category_product_two_week_stats DROP COLUMN IF EXISTS available_amount_sum;

ALTER TABLE kazanex.category_product_weekly_stats DROP COLUMN IF EXISTS available_amount_sum;

ALTER TABLE kazanex.category_product_two_month_stats
    ADD COLUMN IF NOT EXISTS available_amount AggregateFunction(anyLast(), UInt64);
ALTER TABLE kazanex.category_product_monthly_stats
    ADD COLUMN IF NOT EXISTS available_amount AggregateFunction(anyLast(), UInt64);
ALTER TABLE kazanex.category_product_two_week_stats
    ADD COLUMN IF NOT EXISTS available_amount AggregateFunction(anyLast(), UInt64);
ALTER TABLE kazanex.category_product_weekly_stats
    ADD COLUMN IF NOT EXISTS available_amount AggregateFunction(anyLast(), UInt64);
