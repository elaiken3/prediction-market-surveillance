-- Aggregated reject reasons for the cloud dashboard.
select reject_reason, count(*) as n
from {{ source('lake', 'dlq') }}
group by 1
order by n desc
