-- Do a market's outcomes price to ~1.0? A persistent deviation is arbitrage or
-- manipulation -- and a data-quality signal on the feed itself.
with pm as (select * from {{ ref('fct_price_minutely') }}),

summed as (
    select
        market_id,
        minute,
        count(*)            as n_outcomes,
        sum(last_price)     as outcome_sum
    from pm
    group by 1, 2
)

select
    market_id,
    minute,
    n_outcomes,
    round(outcome_sum, 4)              as outcome_sum,
    round(outcome_sum - 1.0, 4)        as deviation,
    abs(outcome_sum - 1.0) > {{ var('coherence_tolerance') }} as is_incoherent
from summed
where n_outcomes >= 2
