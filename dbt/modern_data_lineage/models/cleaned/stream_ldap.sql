{{ config(
    materialized='table',
    schema='cleaned',
    tags=['cleaned']
) }}

with src as (
  select
    cast(_serial as bigint) as _serial,
    from_iso8601_timestamp(
        json_extract_scalar(_raw, '$.timestamp')
    ) AS _time,

    json_extract_scalar(_raw, '$.src_ip') as src_ip,
    json_extract_scalar(_raw, '$.dest_ip') as dest_ip,

    try_cast(json_extract_scalar(_raw, '$.src_port') as integer) as src_port,
    try_cast(json_extract_scalar(_raw, '$.dest_port') as integer) as dest_port,

    json_extract_scalar(_raw, '$.message_type') as event_type

  from {{ source('raw', 'stream_ldap') }}
  where _raw is not null
)

select * from src