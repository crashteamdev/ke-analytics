--liquibase formatted sql
--changeset vitaxa:modify_product_aggregate_available_amount_type
ALTER TABLE kazanex.category_product_two_month_stats
    ADD COLUMN IF NOT EXISTS available_amount_sum AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_monthly_stats
    ADD COLUMN IF NOT EXISTS available_amount_sum AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_two_week_stats
    ADD COLUMN IF NOT EXISTS available_amount_sum AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_weekly_stats
    ADD COLUMN IF NOT EXISTS available_amount_sum AggregateFunction(sum, UInt64);

ALTER TABLE kazanex.category_product_two_month_stats DROP COLUMN IF EXISTS available_amount;

ALTER TABLE kazanex.category_product_monthly_stats DROP COLUMN IF EXISTS available_amount;

ALTER TABLE kazanex.category_product_two_week_stats DROP COLUMN IF EXISTS available_amount;

ALTER TABLE kazanex.category_product_weekly_stats DROP COLUMN IF EXISTS available_amount;
