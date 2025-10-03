-- UH Forecast SLA Dashboard View
-- This query provides a comprehensive view for tracking UH forecast SLA performance

with uh_fcst as (
select distinct
  '1_Actuals_vs_Forecasts' as category
  , a.active_week
  , a.scenario
  , a.config
  , a.submarket_id
  , a.horizon
  , a.pred_uh / 100 as pred_uh
from martech.dasher.dac_optimizer_uh_forecast_v2 a
-- filter on locked versions only
inner join martech.dasher.dac_optimizer_full_executed_config b 
  on a.active_week = b.active_week 
  and substring(a.scenario, 1, 18) = substring(b.scenario, 1, 18) 
  and a.config = b.config
where submarket_id != 0 -- exclude global UH fcst
)

, actual_uh as (
select
  date_trunc('week', local_hour) as week
  , a.submarket_id
  , div0(sum(total_hours_undersupply), nullif(sum(total_hours_online_ideal),0)) as actual_uh
  , sum(total_hours_online_ideal) as ideal_online_hours
  , sum(total_deliveries) as total_delivs
from edw.dasher.view_agg_supply_metrics_sp_hour a
where 1=1
  and date_trunc('week', local_hour) between '2025-01-01' 
  and dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, actuals_vs_forecasts as (
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
      when a.horizon = 0 then 0.003  -- 0.3%
      when a.horizon between 1 and 4 then 0.003  -- 0.3%
      when a.horizon between 5 and 13 then 0.0045  -- 0.45%
      else null
    end as sla_threshold
  , case 
      when a.horizon = 0 then 
        case when error_abs <= 0.003 then 1 else 0 end
      when a.horizon between 1 and 4 then 
        case when error_abs <= 0.003 then 1 else 0 end
      when a.horizon between 5 and 13 then 
        case when error_abs <= 0.0045 then 1 else 0 end
      else 0
    end as within_SLA
  , b.total_delivs
  , b.ideal_online_hours
from uh_fcst a
left join actual_uh b 
  on a.active_week = b.week 
  and a.submarket_id = b.submarket_id
where b.week is not null  -- Only include weeks with actuals
)

-- Global level aggregation for SLA tracking
, global_sla_metrics as (
select 
  forecast_created_week,
  forecast_week,
  horizon,
  scenario,
  config,
  'Global' as level_name,
  0 as submarket_id,
  -- Weighted average for global UH
  sum(fcst_uh * ideal_online_hours) / sum(ideal_online_hours) as fcst_uh_global,
  sum(actual_uh * ideal_online_hours) / sum(ideal_online_hours) as actual_uh_global,
  abs(fcst_uh_global - actual_uh_global) as error_abs_global,
  max(sla_threshold) as sla_threshold,
  case 
    when error_abs_global <= sla_threshold then 1 
    else 0 
  end as within_SLA,
  sum(total_delivs) as total_delivs,
  sum(ideal_online_hours) as ideal_online_hours
from actuals_vs_forecasts
group by all
)

-- Submarket level metrics
, submarket_sla_metrics as (
select 
  forecast_created_week,
  forecast_week,
  horizon,
  scenario,
  config,
  'Submarket' as level_name,
  submarket_id,
  fcst_uh,
  actual_uh,
  error_abs,
  sla_threshold,
  within_SLA,
  total_delivs,
  ideal_online_hours
from actuals_vs_forecasts
)

-- Combined metrics
, all_metrics as (
select * from global_sla_metrics
union all
select * from submarket_sla_metrics
)

-- Weekly SLA performance summary
, weekly_sla_performance as (
select 
  forecast_created_week,
  scenario,
  config,
  level_name,
  horizon,
  case 
    when horizon = 0 then 'W0 (Current Week)'
    when horizon between 1 and 4 then 'W1-4 (Near Term)'
    when horizon between 5 and 13 then 'W5-13 (Long Term)'
    else 'Other'
  end as horizon_bucket,
  count(distinct forecast_week) as total_weeks_forecasted,
  sum(within_sla) as weeks_meeting_sla,
  round(100.0 * sum(within_sla) / count(distinct forecast_week), 2) as sla_achievement_pct,
  -- Special logic for W0 SLA requirement (>11 out of 13 weeks)
  case 
    when horizon = 0 and level_name = 'Global' then 
      case 
        when count(distinct forecast_week) = 13 and sum(within_sla) > 11 then 'PASS'
        when count(distinct forecast_week) < 13 then 'INSUFFICIENT DATA'
        else 'FAIL'
      end
    when horizon between 1 and 4 then
      case when sla_achievement_pct = 100 then 'PASS' else 'FAIL' end
    when horizon between 5 and 13 then
      case when sla_achievement_pct = 100 then 'PASS' else 'FAIL' end
    else 'N/A'
  end as sla_status,
  avg(sla_threshold) as avg_sla_threshold
from all_metrics
where level_name = 'Global'  -- Focus on global metrics for dashboard
group by all
)

-- Recent performance trends
, recent_performance as (
select 
  forecast_week,
  horizon,
  horizon_bucket,
  scenario,
  config,
  round(100.0 * sum(within_sla) / count(*), 2) as weekly_sla_pct,
  count(*) as submarket_count,
  sum(within_sla) as submarkets_meeting_sla
from all_metrics
where level_name = 'Submarket'
  and forecast_week >= dateadd('week', -8, date_trunc('week', current_date))
group by all
)

-- Output for dashboard
select 
  *,
  case 
    when horizon = 0 then 1
    when horizon between 1 and 4 then 2
    when horizon between 5 and 13 then 3
    else 4
  end as horizon_sort_order
from weekly_sla_performance
where forecast_created_week >= dateadd('week', -13, date_trunc('week', current_date))
order by forecast_created_week desc, horizon_sort_order