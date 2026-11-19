-- At origin T, inputs end before T. Target actuals stay in zone_hours.
with past as (
    select zone_id, hour_local,
           count(pickup_count) over history as observed_hours_24,
           avg(pickup_count) over history as partial_mean_24
    from {{ ref('zone_hours') }}
    window history as (
        partition by zone_id order by hour_local rows between 24 preceding and 1 preceding
    )
), features as (
    select p.zone_id, p.hour_local as forecast_origin_local,
           p.hour_local as feature_cutoff_exclusive_local,
           extract(hour from p.hour_local)::integer as hour_of_day,
           extract(isodow from p.hour_local)::integer as day_of_week,
           h1.pickup_count as lag_1h,
           h24.pickup_count as lag_24h,
           h168.pickup_count as lag_168h,
           p.observed_hours_24,
           case when p.observed_hours_24 = 24 then p.partial_mean_24 end as mean_previous_24h
    from past p
    left join {{ ref('zone_hours') }} h1
      on h1.zone_id = p.zone_id and h1.hour_local = p.hour_local - interval '1 hour'
    left join {{ ref('zone_hours') }} h24
      on h24.zone_id = p.zone_id and h24.hour_local = p.hour_local - interval '24 hours'
    left join {{ ref('zone_hours') }} h168
      on h168.zone_id = p.zone_id and h168.hour_local = p.hour_local - interval '168 hours'
)
select *, (lag_1h is not null and lag_24h is not null and lag_168h is not null
           and observed_hours_24 = 24) as history_ready
from features
