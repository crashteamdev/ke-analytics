--liquibase formatted sql
--changeset vitaxa:category-aggregate-optimization
ALTER TABLE kazanex.category_weekly_stats
    ADD PROJECTION IF NOT EXISTS by_date_cat
    (SELECT * ORDER BY date, category_id);

ALTER TABLE kazanex.category_two_week_stats
    ADD PROJECTION IF NOT EXISTS by_date_cat
    (SELECT * ORDER BY date, category_id);

ALTER TABLE kazanex.category_monthly_stats
    ADD PROJECTION IF NOT EXISTS by_date_cat
    (SELECT * ORDER BY date, category_id);

ALTER TABLE kazanex.category_two_month_stats
    ADD PROJECTION IF NOT EXISTS by_date_cat
    (SELECT * ORDER BY date, category_id);
