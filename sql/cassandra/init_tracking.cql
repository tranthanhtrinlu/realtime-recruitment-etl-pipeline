CREATE KEYSPACE IF NOT EXISTS logs
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

USE logs;

DROP TABLE IF EXISTS tracking_raw;

CREATE TABLE tracking_raw (
    create_time text PRIMARY KEY,
    bid int,
    bn text,
    campaign_id int,
    cd int,
    custom_track text,
    de text,
    dl text,
    dt text,
    ed text,
    ev int,
    group_id int,
    id text,
    job_id int,
    md text,
    publisher_id int,
    rl text,
    sr text,
    ts timestamp,
    tz int,
    ua text,
    uid text,
    utm_campaign text,
    utm_content text,
    utm_medium text,
    utm_source text,
    utm_term text,
    v int,
    vp text
);