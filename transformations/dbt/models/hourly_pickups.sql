select zone_id, date_trunc('hour', pickup_local) as hour_local,
       count(*)::bigint as recorded_pickups
from {{ source('raw', 'pickups') }}
group by zone_id, date_trunc('hour', pickup_local)
