CREATE DATABASE IF NOT EXISTS polaris_catalog
ENGINE = DataLakeCatalog('http://polaris:8181/api/catalog/v1')
SETTINGS
    catalog_type = 'rest',
    catalog_credential = 'quickstart_user:quickstart_pass',
    warehouse = 'quickstart_catalog',
    auth_scope = 'PRINCIPAL_ROLE:ALL',
    oauth_server_uri = 'http://polaris:8181/api/catalog/v1/oauth/tokens',
    storage_endpoint = 'http://rustfs:9900/warehouse',
    vended_credentials = false;

CREATE TABLE IF NOT EXISTS default.events_kafka (
    id Int64,
    msg String,
    ts DateTime64(6)
) ENGINE = Kafka
SETTINGS
    kafka_broker_list = 'kafka:19092',
    kafka_topic_list = 'events',
    kafka_group_name = 'clickhouse-events',
    kafka_format = 'JSONEachRow',
    kafka_num_consumers = 1;

CREATE MATERIALIZED VIEW IF NOT EXISTS default.events_mv
TO polaris_catalog.`default.events` AS
SELECT id, msg, ts FROM default.events_kafka;

