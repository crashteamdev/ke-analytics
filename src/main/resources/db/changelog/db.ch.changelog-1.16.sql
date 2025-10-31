ALTER TABLE kazanex.category_daily_uniqs
    ADD PROJECTION IF NOT EXISTS by_date_cat
    (SELECT * ORDER BY date, category_id);

ALTER TABLE kazanex.category_daily_uniqs
    MATERIALIZE PROJECTION by_date_cat;
