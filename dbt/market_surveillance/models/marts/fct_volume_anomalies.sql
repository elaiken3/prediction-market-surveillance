-- Batch volume detector: per-minute traded volume that is a large positive
-- z-score outlier vs the asset's own history (wash-trading footprint).
with pm as (select * from {{ ref('fct_price_minutely') }}),

stats as (
    select asset_id, avg(volume) as mu, stddev_pop(volume) as sigma, count(*) as n
    from pm group by 1
)

select
    pm.market_id,
    pm.asset_id,
    pm.minute,
    pm.volume,
    case when s.sigma > 0 then round((pm.volume - s.mu) / s.sigma, 2) end as volume_z,
    'volume_zscore' as rule
from pm
join stats s using (asset_id)
where s.n >= 6
  and s.sigma > 0
  and (pm.volume - s.mu) / s.sigma >= {{ var('volume_z_threshold') }}
