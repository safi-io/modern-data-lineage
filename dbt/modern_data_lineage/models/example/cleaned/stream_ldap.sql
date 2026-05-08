{{ config(
    materialized='table',
    schema='cleaned',
    tags=['cleaned']
) }}

select *
from {{ source('raw', 'stream_ldap') }}
where _raw is not null