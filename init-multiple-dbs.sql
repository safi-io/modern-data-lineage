CREATE DATABASE custom;
\c custom;

CREATE TYPE file_status_enum AS ENUM (
    'new',
    'inprogress',
    'success',
    'failed'
);

CREATE TABLE file_status (
    file_name TEXT PRIMARY KEY,
    status file_status_enum
);