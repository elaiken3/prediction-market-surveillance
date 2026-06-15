-- Most recent real-time flags, materialised for the cloud dashboard.
select asset_id, market_id, window_start, price_range, volume
from {{ source('lake', 'flagged') }}
order by window_start desc
limit 200
