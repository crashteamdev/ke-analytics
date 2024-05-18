--liquibase formatted sql
--changeset vitaxa:modify_product_aggregate_available_amount_type
ALTER TABLE kazanex.category_product_two_month_stats
    MODIFY COLUMN available_amount AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_monthly_stats
    MODIFY COLUMN available_amount AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_two_week_stats
    MODIFY COLUMN available_amount AggregateFunction(sum, UInt64);
ALTER TABLE kazanex.category_product_weekly_stats
    MODIFY COLUMN available_amount AggregateFunction(sum, UInt64);
