\c project_1

ALTER SCHEMA public RENAME TO bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS config;

CREATE TABLE config.params (
    param_name VARCHAR(50) PRIMARY KEY,
    param_value VARCHAR(300) 
);

INSERT INTO config.params (param_name) VALUES ('last_pipeline_execution');
INSERT INTO config.params (param_name) VALUES ('records_per_page');