-- Did the stream lie? Reconcile real-time price_range flags against a batch
-- recomputation of the same rule. BOTH / STREAM_ONLY / BATCH_ONLY.
with stream_flags as (
    select distinct asset_id, date_trunc('minute', window_start) as minute
    from {{ source('lake', 'flagged') }}
),

batch_flags as (
    select asset_id, minute
    from {{ ref('fct_price_minutely') }}
    where price_range >= {{ var('price_range_threshold') }}
),

joined as (
    select
        coalesce(s.asset_id, b.asset_id)   as asset_id,
        coalesce(s.minute, b.minute)        as minute,
        s.asset_id is not null              as in_stream,
        b.asset_id is not null              as in_batch
    from stream_flags s
    full outer join batch_flags b on s.asset_id = b.asset_id and s.minute = b.minute
)

select *,
    case when in_stream and in_batch then 'BOTH'
         when in_stream then 'STREAM_ONLY'
         else 'BATCH_ONLY' end as reconciliation_status
from joined
