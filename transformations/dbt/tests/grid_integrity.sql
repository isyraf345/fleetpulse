with problems as (
    select 'duplicate zone-hour' as problem from {{ ref('zone_hours') }}
    group by zone_id, hour_local having count(*) != 1
    union all
    select 'incorrect grid size' where
      (select count(*) from {{ ref('zone_hours') }}) !=
      (select count(*) from {{ source('raw', 'zones') }}) *
      (select count(*) from {{ source('raw', 'calendar') }})
    union all
    select 'unmatched input zone' from {{ ref('hourly_pickups') }} h
    left join {{ source('raw', 'zones') }} z using (zone_id) where z.zone_id is null
    union all
    select 'unmatched input hour' from {{ ref('hourly_pickups') }} h
    left join {{ source('raw', 'calendar') }} c using (hour_local) where c.hour_local is null
)
select * from problems
