\c project_1

ALTER SCHEMA public RENAME TO bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS config;

CREATE TABLE config.params (
    param_name VARCHAR(50) PRIMARY KEY,
    param_value VARCHAR(300) 
);

INSERT INTO config.params (param_name, param_value) VALUES ('last_pipeline_execution', NULL);
INSERT INTO config.params (param_name, param_value) VALUES ('records_per_page', '500');

CREATE TABLE config.pipeline_runs (
    run_id        SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    start_time    TIMESTAMP    NOT NULL,
    end_time      TIMESTAMP,
    status        VARCHAR(20),
    records_extracted  INTEGER,
    records_loaded     INTEGER,
    error_message      TEXT
);
