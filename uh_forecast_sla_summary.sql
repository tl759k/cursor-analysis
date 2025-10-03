-- UH Forecast SLA Summary Metrics
-- This query provides aggregated SLA metrics for dashboard KPIs

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
  , sum(total_hours_online_ideal) as ideal_online_hours
  , sum(total_deliveries) as total_delivs
from edw.dasher.view_agg_supply_metrics_sp_hour a
where date_trunc('week', local_hour) between '2025-01-01' 
  and dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, forecasts_with_actuals as (
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
      when a.horizon between 0 and 4 and abs(a.pred_uh - b.actual_uh) < (0.3 / 100) then 1
      when a.horizon between 5 and 13 and abs(a.pred_uh - b.actual_uh) < (0.45 / 100) then 1
      else 0 
    end as within_SLA
  , b.total_delivs
  , b.ideal_online_hours
  , case when b.total_delivs >= 2000 then 1 else 0 end as is_large_sm
from uh_fcst a
left join actual_uh b 
  on a.active_week = b.week 
  and a.submarket_id = b.submarket_id
where dateadd('week', a.horizon, a.active_week) <= dateadd('week', -1, date_trunc('week', current_date))
)

-- W0 specific: 11/13 weeks SLA calculation
, w0_rolling_13wk_sla as (
select
  forecast_created_week
  , scenario
  , config
  , submarket_id
  , count(*) as total_weeks_in_window
  , sum(within_SLA) as weeks_hitting_sla
  , case when weeks_hitting_sla >= 11 then 1 else 0 end as meets_w0_sla
  , div0(weeks_hitting_sla, total_weeks_in_window) as w0_sla_rate
from forecasts_with_actuals
where horizon = 0
  and forecast_created_week >= dateadd('week', -13, date_trunc('week', current_date))
group by 1,2,3,4
)

-- Global level W0 SLA
, w0_global_sla as (
select
  forecast_created_week
  , scenario
  , config
  , sum(within_SLA * ideal_online_hours) / sum(ideal_online_hours) as weighted_sla_rate
  , case when weighted_sla_rate >= (11.0/13.0) then 1 else 0 end as meets_w0_sla_global
from forecasts_with_actuals
where horizon = 0
  and forecast_created_week >= dateadd('week', -13, date_trunc('week', current_date))
group by 1,2,3
)

-- Horizon-level SLA rates
, horizon_sla_summary as (
select
  horizon
  , case 
      when horizon = 0 then 'W0 (Same Week)'
      when horizon between 1 and 4 then 'W1-W4'
      when horizon between 5 and 13 then 'W5-W13'
      else 'Other'
    end as horizon_group
  , scenario
  , config
  , count(*) as total_forecasts
  , sum(within_SLA) as forecasts_within_sla
  , div0(forecasts_within_sla, total_forecasts) as sla_achievement_rate
  , avg(error_abs) as avg_error_abs
  , median(error_abs) as median_error_abs
  , percentile_cont(0.90) within group (order by error_abs) as p90_error_abs
from forecasts_with_actuals
group by 1,2,3,4
)

-- Submarket-level performance (for large submarkets)
, submarket_sla_performance as (
select
  submarket_id
  , scenario
  , config
  , sum(case when horizon = 0 then 1 else 0 end) as w0_total_forecasts
  , sum(case when horizon = 0 then within_SLA else 0 end) as w0_within_sla
  , div0(w0_within_sla, w0_total_forecasts) as w0_sla_rate
  , sum(case when horizon between 1 and 4 then 1 else 0 end) as w1_4_total_forecasts
  , sum(case when horizon between 1 and 4 then within_SLA else 0 end) as w1_4_within_sla
  , div0(w1_4_within_sla, w1_4_total_forecasts) as w1_4_sla_rate
  , sum(case when horizon between 5 and 13 then 1 else 0 end) as w5_13_total_forecasts
  , sum(case when horizon between 5 and 13 then within_SLA else 0 end) as w5_13_within_sla
  , div0(w5_13_within_sla, w5_13_total_forecasts) as w5_13_sla_rate
  , avg(total_delivs) as avg_weekly_delivs
from forecasts_with_actuals
where is_large_sm = 1
group by 1,2,3
)

-- Time-series SLA trend
, weekly_sla_trend as (
select
  forecast_week
  , horizon
  , scenario
  , config
  , count(*) as total_submarkets
  , sum(within_SLA) as submarkets_within_sla
  , div0(submarkets_within_sla, total_submarkets) as sla_achievement_rate
  , avg(error_abs) as avg_error_abs
from forecasts_with_actuals
where is_large_sm = 1
group by 1,2,3,4
)

-- Final output: choose the view you need for your dashboard
select 
  'Horizon_Summary' as view_type
  , horizon
  , horizon_group
  , scenario
  , config
  , total_forecasts
  , forecasts_within_sla
  , sla_achievement_rate
  , avg_error_abs
  , median_error_abs
  , p90_error_abs
  , null as submarket_id
  , null as forecast_week
from horizon_sla_summary

union all

select
  'Submarket_Performance' as view_type
  , null as horizon
  , null as horizon_group
  , scenario
  , config
  , w0_total_forecasts as total_forecasts
  , w0_within_sla as forecasts_within_sla
  , w0_sla_rate as sla_achievement_rate
  , null as avg_error_abs
  , null as median_error_abs
  , null as p90_error_abs
  , submarket_id
  , null as forecast_week
from submarket_sla_performance

union all

select
  'Weekly_Trend' as view_type
  , horizon
  , null as horizon_group
  , scenario
  , config
  , total_submarkets as total_forecasts
  , submarkets_within_sla as forecasts_within_sla
  , sla_achievement_rate
  , avg_error_abs
  , null as median_error_abs
  , null as p90_error_abs
  , null as submarket_id
  , forecast_week
from weekly_sla_trend

order by view_type, horizon, forecast_week, submarket_id
