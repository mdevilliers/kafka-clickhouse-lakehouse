CREATE DATABASE IF NOT EXISTS silver;

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.books_authors_mv
TO polaris_catalog.`silver.books_authors` AS
SELECT b.id, a.name AS author_name, b.title, b.price, b.stock_quantity
FROM polaris_catalog.`raw_data.books`  b
INNER JOIN polaris_catalog.`raw_data.authors` a ON b.author_id = a.id;

CREATE TABLE IF NOT EXISTS silver.book_authors_sink (
    id Int64,
    author_name String,
    title String,
    price Float64,
    stock_quantity Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'book_authors',
    kafka_group_name = 'clickhouse-silver-book-authors-sink',
    kafka_format = 'JSONEachRow';

CREATE MATERIALIZED VIEW IF NOT EXISTS silver.book_authors_kafka_mv
TO silver.book_authors_sink AS
SELECT b.id, a.name AS author_name, b.title, b.price, b.stock_quantity
FROM polaris_catalog.`raw_data.books`  b
INNER JOIN polaris_catalog.`raw_data.authors` a ON b.author_id = a.id;
