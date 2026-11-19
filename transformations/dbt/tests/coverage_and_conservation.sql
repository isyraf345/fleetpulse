select 'invalid actual' as problem from {{ ref('zone_hours') }}
where recorded_pickups < 0 or pickup_count < 0
   or (coverage_status = 'recorded_hour' and pickup_count is null)
   or (coverage_status != 'recorded_hour' and pickup_count is not null)
union all
select 'lost pickups' where
  (select coalesce(sum(recorded_pickups), 0) from {{ ref('zone_hours') }}) !=
  (select count(*) from {{ source('raw', 'pickups') }})
