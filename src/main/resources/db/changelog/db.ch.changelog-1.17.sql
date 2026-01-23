--liquibase formatted sql
--changeset vitaxa:create-categories-hierarchical-dictionary
CREATE NAMED COLLECTION IF NOT EXISTS pg_kazanex AS
    host = 'localhost' NOT OVERRIDABLE,
    port = 6432 NOT OVERRIDABLE,
    user = 'dbuser' NOT OVERRIDABLE,
    password = '***' NOT OVERRIDABLE,
    database = 'ke-analytics',
    schema = 'public';

CREATE DICTIONARY IF NOT EXISTS kazanex.categories_hierarchical_dictionary
(
    category_id UInt64,
    parent_category_id UInt64 HIERARCHICAL,
    title String
)
    PRIMARY KEY category_id
    SOURCE(POSTGRESQL(
            NAME pg_kazanex
            DB 'ke-analytics'
            SCHEMA 'public'
            TABLE 'category_hierarchical'
           ))
    LIFETIME(MIN 0 MAX 300)
    LAYOUT(FLAT());

