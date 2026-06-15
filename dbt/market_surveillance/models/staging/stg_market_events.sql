-- Typed, tidy interface over the valid landing zone.
with source as (select * from {{ source('lake', 'valid') }})
select
    event_id,
    source,
    event_type,
    cast(event_ts as timestamp) as event_ts,
    market_id,
    asset_id,
    outcome,
    cast(price as double)       as price,
    cast(size as double)        as size,
    side
from source
