CREATE DATABASE IF NOT EXISTS etl_dw;
USE etl_dw;

DROP TABLE IF EXISTS dim_search_job;
CREATE TABLE dim_search_job (
    search_row_id INT NULL,
    job_id INT PRIMARY KEY,
    benefits TEXT NULL,
    bid DOUBLE NULL,
    campaign_budget DOUBLE NULL,
    campaign_id INT NULL,
    city_name VARCHAR(255) NULL,
    company_logo TEXT NULL,
    company_name VARCHAR(255) NULL,
    description TEXT NULL,
    feed_id VARCHAR(100) NULL,
    lat DOUBLE NULL,
    lon DOUBLE NULL,
    major_category VARCHAR(255) NULL,
    minor_category VARCHAR(255) NULL,
    pay_currentcy VARCHAR(50) NULL,
    pay_from DOUBLE NULL,
    pay_option DOUBLE NULL,
    pay_to DOUBLE NULL,
    pay_type DOUBLE NULL,
    postal_code VARCHAR(50) NULL,
    requirements TEXT NULL,
    state VARCHAR(100) NULL,
    status INT NULL,
    title VARCHAR(255) NULL,
    work_schedule VARCHAR(100) NULL
);

CREATE TABLE IF NOT EXISTS etl_watermark (
    pipeline_name VARCHAR(100) PRIMARY KEY,
    last_loaded_ts DATETIME NOT NULL
);

INSERT INTO etl_watermark (pipeline_name, last_loaded_ts)
VALUES ('tracking_to_events_hourly', '1970-01-01 00:00:00')
ON DUPLICATE KEY UPDATE last_loaded_ts = last_loaded_ts;

DROP TABLE IF EXISTS fact_events_hourly_stage;
CREATE TABLE fact_events_hourly_stage (
    job_id INT NOT NULL,
    dates DATE NOT NULL,
    hours INT NOT NULL,
    disqualified_application INT DEFAULT 0,
    qualified_application INT DEFAULT 0,
    conversion INT DEFAULT 0,
    group_id INT NULL,
    campaign_id INT NULL,
    publisher_id INT NULL,
    company_name VARCHAR(255) NULL,
    bid_set DOUBLE DEFAULT 0,
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    spend_hour DOUBLE DEFAULT 0,
    sources VARCHAR(50) DEFAULT 'Cassandra',
    latest_update DATETIME NOT NULL
);

DROP TABLE IF EXISTS fact_events_hourly;
CREATE TABLE fact_events_hourly (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    job_id INT NOT NULL,
    dates DATE NOT NULL,
    hours INT NOT NULL,
    disqualified_application INT DEFAULT 0,
    qualified_application INT DEFAULT 0,
    conversion INT DEFAULT 0,
    group_id INT NULL,
    campaign_id INT NULL,
    publisher_id INT NULL,
    company_name VARCHAR(255) NULL,
    bid_set DOUBLE DEFAULT 0,
    clicks INT DEFAULT 0,
    impressions INT DEFAULT 0,
    spend_hour DOUBLE DEFAULT 0,
    sources VARCHAR(50) DEFAULT 'Cassandra',
    latest_update DATETIME NOT NULL,
    UNIQUE KEY uk_fact_events_hourly (job_id, dates, hours, campaign_id, publisher_id)
);