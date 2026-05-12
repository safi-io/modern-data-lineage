{{ config(
    materialized='table',
    schema='cleaned',
    tags=['cleaned']
) }}

select

    _serial as event_id,
    _time as event_time,

    src_ip,
    dest_ip,

    src_port,
    dest_port,

    event_type,

    case
        when event_type like 'Bind%'
        then 'authentication'

        when event_type like 'Search%'
        then 'directory_query'

        else 'other'
    end as event_category,

    case
        when dest_port = 389 then 'LDAP'
        when dest_port = 636 then 'LDAPS'
        else 'UNKNOWN'
    end as protocol,

    case
        when dest_port = 636 then true
        else false
    end as is_encrypted,

    case
        when event_type in (
            'Bind request',
            'Bind result'
        )
        then true
        else false
    end as is_authentication,

    case
        when event_type in (
            'Search request',
            'Search result',
            'Search entry result'
        )
        then true
        else false
    end as is_search_activity,

    case
        when event_type = 'Search entry result'
        then 'high'

        when event_type = 'Search request'
        then 'medium'

        else 'low'
    end as severity,

    hour(_time) as event_hour,
    cast(_time as date) as event_date

from {{ ref('stream_ldap') }}