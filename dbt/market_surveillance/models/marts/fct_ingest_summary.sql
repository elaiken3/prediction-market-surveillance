-- Small summary table so the cloud dashboard can show ingest health by reading
-- ONE MotherDuck table instead of the raw lake.
with c as (
    select
        (select count(*) from {{ source('lake', 'valid') }}) as valid_count,
        (select count(*) from {{ source('lake', 'dlq') }})   as dlq_count
)
select
    valid_count,
    dlq_count,
    case when (valid_count + dlq_count) > 0
         then round(dlq_count::double / (valid_count + dlq_count), 4)
         else 0 end as reject_rate
from c
