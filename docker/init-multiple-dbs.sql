CREATE DATABASE meltano; -- For managing Meltano State Backend
CREATE DATABASE custom; -- For managing Data Platform Logic
\c custom;

CREATE TYPE file_status_enum AS ENUM (
    'new',
    'inprogress',
    'success',
    'failed'
);

CREATE TABLE file_status (
    file_name TEXT PRIMARY KEY,
    status file_status_enum,
    job_id TEXT,
    started_at TIMESTAMP,
    finished_at TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_file_status_status
ON file_status(status);

CREATE INDEX idx_file_status_job_id
ON file_status(job_id);