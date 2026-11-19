select 'duplicate feature key' as problem from {{ ref('hourly_features') }}
group by zone_id, forecast_origin_local having count(*) != 1
union all
select 'incorrect feature grid' where
  (select count(*) from {{ ref('hourly_features') }}) !=
  (select count(*) from {{ ref('zone_hours') }})
union all
select 'invalid window' from {{ ref('hourly_features') }}
where observed_hours_24 not between 0 and 24
   or (observed_hours_24 < 24 and mean_previous_24h is not null)
   or feature_cutoff_exclusive_local != forecast_origin_local
   or (history_ready and (lag_1h is null or lag_24h is null or lag_168h is null or mean_previous_24h is null))
