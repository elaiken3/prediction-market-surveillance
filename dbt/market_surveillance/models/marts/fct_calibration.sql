-- Resolution as ground truth. For each resolved outcome token, compare the last
-- observed price to the actual outcome (1 if it won, else 0) and score it with
-- the Brier score (lower = better calibrated). This turns "is the market/feed
-- trustworthy?" into a measurable number.
with res as (select * from {{ source('lake', 'resolutions') }}),

last_seen as (
    select asset_id, last_price,
           row_number() over (partition by asset_id order by minute desc) as rn
    from {{ ref('fct_price_minutely') }}
)

select
    r.market_id,
    r.asset_id,
    r.outcome,
    r.won,
    l.last_price                                   as final_price,
    round(power(l.last_price - r.won, 2), 4)       as brier_score
from res r
join last_seen l on l.asset_id = r.asset_id and l.rn = 1
