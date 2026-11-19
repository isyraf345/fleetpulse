-- Eligibility uses the pinned reference only, never held-out demand.
with activity as (
    select hour_local, sum(recorded_pickups) as citywide_pickups
    from {{ ref('hourly_pickups') }} group by hour_local
), grid as (
    select z.zone_id, c.hour_local,
           coalesce(p.recorded_pickups, 0)::bigint as recorded_pickups,
           case when c.transition_day then 'dst_transition_day'
                when not c.snapshot_present then 'missing_snapshot'
                when a.citywide_pickups is null then 'no_citywide_pickups'
                else 'recorded_hour' end as coverage_status
    from {{ source('raw', 'zones') }} z
    cross join {{ source('raw', 'calendar') }} c
    left join {{ ref('hourly_pickups') }} p using (zone_id, hour_local)
    left join activity a using (hour_local)
)
select zone_id, hour_local, recorded_pickups, coverage_status,
       case when coverage_status = 'recorded_hour' then recorded_pickups end as pickup_count
from grid
