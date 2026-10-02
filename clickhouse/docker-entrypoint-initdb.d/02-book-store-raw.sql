CREATE DATABASE IF NOT EXISTS raw_data;

CREATE TABLE IF NOT EXISTS raw_data.books (
    id Int64,
    author_id Int64,
    title String,
    price Float64,
    stock_quantity Int32
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'books',
    kafka_group_name = 'clickhouse-raw-data-books',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE TABLE IF NOT EXISTS raw_data.authors (
    id Int64,
    name String
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'authors',
    kafka_group_name = 'clickhouse-raw-data-authors',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_data.books_mv
TO polaris_catalog.`raw_data.books` AS
SELECT id, author_id, title, price, stock_quantity FROM raw_data.books;

CREATE MATERIALIZED VIEW IF NOT EXISTS raw_data.authors_mv
TO polaris_catalog.`raw_data.authors` AS
SELECT id, name FROM raw_data.authors;
