--liquibase formatted sql
--changeset vitaxa:create-kazanex-db
CREATE DATABASE IF NOT EXISTS kazanex;

--changeset vitaxa:create-kazanex-product-table
DROP TABLE IF EXISTS kazanex.product;
CREATE TABLE kazanex.product
(
    timestamp               DateTime,
    product_id              String,
    sku_id                  String,
    title                   String,
    rating                  Decimal32(2),
    latest_category_id      UInt64,
    reviews_amount          UInt32,
    total_orders_amount     UInt64,
    total_available_amount  UInt64,
    available_amount        UInt64,
    attributes Array(String),
    tags Array(String),
    photo_key               String,
    characteristics Map(String, String),
    seller_id               UInt64,
    seller_account_id       UInt64,
    seller_title            String,
    seller_link             String,
    seller_registrationDate DateTime,
    seller_rating           Decimal32(2),
    seller_reviewsCount     UInt64,
    seller_orders           UInt64,
    seller_official         UInt8,
    seller_contacts Map(String, String),
    is_eco                  UInt8,
    is_adult                UInt8,
    full_price Nullable(UInt64),
    purchase_price          UInt64
) ENGINE = MergeTree()
      PARTITION BY toYYYYMM(timestamp)
      PRIMARY KEY (product_id, sku_id)
      ORDER BY (product_id, sku_id, timestamp)
