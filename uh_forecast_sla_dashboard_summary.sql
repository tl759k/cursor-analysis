-- UH Forecast SLA Dashboard Summary
-- This query provides dashboard-ready SLA tracking metrics

with uh_fcst as (
select distinct
  a.active_week
  , a.scenario
  , a.config
  , a.submarket_id
  , a.horizon
  , a.pred_uh / 100 as pred_uh
from martech.dasher.dac_optimizer_uh_forecast_v2 a
inner join martech.dasher.dac_optimizer_full_executed_config b 
  on a.active_week = b.active_week 
  and substring(a.scenario, 1, 18) = substring(b.scenario, 1, 18) 
  and a.config = b.config
where submarket_id != 0
)

, actual_uh as (
select
  date_trunc('week', local_hour) as week
  , a.submarket_id
  , div0(sum(total_hours_undersupply), nullif(sum(total_hours_online_ideal),0)) as actual_uh
  , sum(total_hours_undersupply) as undersupplied_hours
  , sum(total_hours_online_ideal) as ideal_online_hours
  , sum(total_deliveries) as total_delivs
from edw.dasher.view_agg_supply_metrics_sp_hour a
where date_trunc('week', local_hour) between '2025-01-01' and dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, forecast_actuals as (
select
  a.active_week as forecast_created_week
  , dateadd('week', a.horizon, a.active_week) as forecast_week
  , a.horizon
  , a.scenario
  , a.config
  , a.submarket_id
  , a.pred_uh as fcst_uh
  , b.actual_uh
  , abs(a.pred_uh - b.actual_uh) as error_abs 
  , case 
      when a.horizon = 0 then 0.3 / 100
      when a.horizon between 1 and 4 then 0.3 / 100
      when a.horizon between 5 and 13 then 0.45 / 100
      else 0.45 / 100
    end as sla_threshold
  , case when abs(a.pred_uh - b.actual_uh) <= sla_threshold then 1 else 0 end as within_SLA
  , b.total_delivs
  , b.ideal_online_hours
  , case when b.total_delivs >= 2000 then 'Large (>=2k delivs)' else 'Small (<2k delivs)' end as submarket_size
from uh_fcst a
left join actual_uh b on a.active_week = b.week and a.submarket_id = b.submarket_id
where dateadd('week', a.horizon, a.active_week) <= dateadd('week', -1, date_trunc('week', current_date))
  and b.actual_uh is not null
)

-- Submarket Level SLA Performance
, submarket_sla_summary as (
select
  'Submarket' as aggregation_level
  , scenario
  , config
  , submarket_id
  , submarket_size
  , horizon
  , case 
      when horizon = 0 then 'W0 (Same Week)'
      when horizon between 1 and 4 then 'W1-4 (Short Term)'
      when horizon between 5 and 13 then 'W5-13 (Long Term)'
      else 'W14+ (Extended)'
    end as horizon_group
  , count(*) as total_forecasts
  , sum(within_sla) as forecasts_hitting_sla
  , div0(forecasts_hitting_sla, total_forecasts) as sla_hit_rate
  , avg(error_abs) as avg_error_abs
  , median(error_abs) as median_error_abs
  , max(sla_threshold) as sla_threshold
  -- SLA Status based on requirements
  , case 
      when horizon = 0 and forecasts_hitting_sla >= 11 and total_forecasts >= 13 then 'PASS'
      when horizon = 0 then 'FAIL'
      when horizon between 1 and 13 and sla_hit_rate = 1.0 then 'PASS'
      when horizon between 1 and 13 then 'FAIL'
      else 'N/A'
    end as sla_status
from forecast_actuals
where horizon <= 13
group by all
)

-- Global Level SLA Performance  
, global_sla_summary as (
select
  'Global' as aggregation_level
  , scenario
  , config
  , 0 as submarket_id
  , 'All Submarkets' as submarket_size
  , horizon
  , case 
      when horizon = 0 then 'W0 (Same Week)'
      when horizon between 1 and 4 then 'W1-4 (Short Term)'
      when horizon between 5 and 13 then 'W5-13 (Long Term)'
      else 'W14+ (Extended)'
    end as horizon_group
  , count(*) as total_forecasts
  , sum(within_sla) as forecasts_hitting_sla
  , div0(forecasts_hitting_sla, total_forecasts) as sla_hit_rate
  -- Weighted averages for global metrics
  , sum(error_abs * ideal_online_hours) / sum(ideal_online_hours) as avg_error_abs
  , max(sla_threshold) as sla_threshold
  , case 
      when horizon = 0 and forecasts_hitting_sla >= 11 and total_forecasts >= 13 then 'PASS'
      when horizon = 0 then 'FAIL'
      when horizon between 1 and 13 and sla_hit_rate = 1.0 then 'PASS'
      when horizon between 1 and 13 then 'FAIL'
      else 'N/A'
    end as sla_status
from forecast_actuals
where horizon <= 13
  and ideal_online_hours > 0
group by all
)

-- Combined SLA Dashboard Data
, sla_dashboard_data as (
select * from submarket_sla_summary
union all
select 
  aggregation_level, scenario, config, submarket_id, submarket_size, horizon, horizon_group,
  total_forecasts, forecasts_hitting_sla, sla_hit_rate, avg_error_abs, 
  avg_error_abs as median_error_abs, sla_threshold, sla_status
from global_sla_summary
)

-- Weekly SLA Trend (for time series dashboard)
, weekly_sla_trend as (
select
  forecast_created_week
  , horizon
  , case 
      when horizon = 0 then 'W0'
      when horizon between 1 and 4 then 'W1-4'
      when horizon between 5 and 13 then 'W5-13'
    end as horizon_group
  , scenario
  , config
  , count(*) as total_forecasts
  , sum(within_sla) as forecasts_hitting_sla
  , div0(forecasts_hitting_sla, total_forecasts) as weekly_sla_hit_rate
  , avg(error_abs) as avg_weekly_error
from forecast_actuals
where horizon <= 13
group by all
)

-- Overall SLA Health Score
, sla_health_score as (
select
  scenario
  , config
  , -- W0 Score: 1 if >= 11/13 weeks hit SLA, 0 otherwise
    max(case when horizon = 0 and sla_status = 'PASS' then 1 else 0 end) as w0_sla_score
  , -- W1-4 Score: average SLA hit rate across horizons 1-4
    avg(case when horizon between 1 and 4 then sla_hit_rate else null end) as w1_4_sla_score
  , -- W5-13 Score: average SLA hit rate across horizons 5-13  
    avg(case when horizon between 5 and 13 then sla_hit_rate else null end) as w5_13_sla_score
  , -- Overall Health Score (weighted average)
    (w0_sla_score * 0.4 + w1_4_sla_score * 0.3 + w5_13_sla_score * 0.3) as overall_health_score
  , case 
      when overall_health_score >= 0.9 then 'Excellent'
      when overall_health_score >= 0.8 then 'Good' 
      when overall_health_score >= 0.7 then 'Fair'
      else 'Poor'
    end as health_grade
from sla_dashboard_data
where aggregation_level = 'Global'
group by all
)

-- Main Dashboard Output
select 
  'SLA_Summary' as report_type
  , current_timestamp as report_generated_at
  , *
from sla_dashboard_data
order by aggregation_level, scenario, config, horizon

-- Uncomment sections below for additional dashboard views:

-- union all
-- select 
--   'Weekly_Trend' as report_type
--   , current_timestamp as report_generated_at
--   , forecast_created_week, horizon, horizon_group, scenario, config
--   , null as aggregation_level, null as submarket_id, null as submarket_size
--   , total_forecasts, forecasts_hitting_sla, weekly_sla_hit_rate as sla_hit_rate
--   , avg_weekly_error as avg_error_abs, null as median_error_abs
--   , null as sla_threshold, null as sla_status
-- from weekly_sla_trend
-- order by forecast_created_week, horizon

-- union all  
-- select
--   'Health_Score' as report_type
--   , current_timestamp as report_generated_at
--   , null as forecast_created_week, null as horizon, null as horizon_group
--   , scenario, config, null as aggregation_level, null as submarket_id, null as submarket_size
--   , null as total_forecasts, null as forecasts_hitting_sla
--   , overall_health_score as sla_hit_rate, null as avg_error_abs, null as median_error_abs
--   , null as sla_threshold, health_grade as sla_status
-- from sla_health_score