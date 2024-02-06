--liquibase formatted sql
--changeset vitaxa:create-product-position
CREATE TABLE IF NOT EXISTS kazanex.product_position
(
    timestamp   DateTime,
    product_id  String,
    sku_id      String,
    category_id String,
    position    UInt64
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(timestamp)
      ORDER BY (category_id, product_id, sku_id, timestamp)
      TTL timestamp + INTERVAL 1 MONTH;
