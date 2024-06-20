--liquibase formatted sql
--changeset vitaxa:modify_product_table_ttl
ALTER TABLE kazanex.product
    MODIFY TTL timestamp + INTERVAL 4 MONTH DELETE;
