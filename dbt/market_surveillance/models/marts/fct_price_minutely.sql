-- Per-asset, per-minute price/volume summary. The base for coherence, volume
-- anomalies, and the batch side of reconciliation.
with ev as (select * from {{ ref('stg_market_events') }}),

ranked as (
    select *,
        row_number() over (
            partition by market_id, asset_id, date_trunc('minute', event_ts)
            order by event_ts desc, event_id
        ) as rn
    from ev
),

agg as (
    select
        market_id,
        asset_id,
        date_trunc('minute', event_ts)                          as minute,
        count(*)                                                as ticks,
        max(price) - min(price)                                 as price_range,
        sum(case when event_type = 'trade' then size else 0 end) as volume
    from ev
    group by 1, 2, 3
),

last_px as (
    select market_id, asset_id,
           date_trunc('minute', event_ts) as minute,
           price as last_price
    from ranked
    where rn = 1
)

select
    a.market_id,
    a.asset_id,
    a.minute,
    a.ticks,
    a.price_range,
    a.volume,
    l.last_price
from agg a
join last_px l using (market_id, asset_id, minute)
