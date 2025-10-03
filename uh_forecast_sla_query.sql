-- UH forecast vs Actuals with SLA Tracking
with uh_fcst as (
select distinct
  '1_Actuals_vs_Forecasts' as category
  , a.active_week
  , a.scenario
  , a.config
  , a.submarket_id
  , a.horizon
  , a.pred_uh_no_dac_no_dxo / 100 as pred_uh_no_dac_no_dxo
  , a.pred_uh_no_dac / 100 as pred_uh_no_dac
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
  , sum(total_hours_undersupply) as undersupplied_hours
  , sum(total_hours_online_ideal) as ideal_online_hours
  , sum(total_deliveries) as total_delivs
from edw.dasher.view_agg_supply_metrics_sp_hour a
where 1=1
  and date_trunc('week', local_hour) between '2025-01-01' and dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, actuals_vs_forecasts as (
select
  'Forecasts' as metric_cat
  , 1 as metric_cat_order
  , a.active_week as forecast_created_week
  , dateadd('week', a.horizon, a.active_week) as forecast_week
  , a.horizon
  , a.scenario
  , a.config
  , a.submarket_id
  , a.pred_uh as fcst_uh
  , b.actual_uh
  , a.pred_uh - b.actual_uh as error
  , abs(a.pred_uh - b.actual_uh) as error_abs 
  , div0(a.pred_uh, b.actual_uh) - 1 as error_pct
  , abs(error_pct) as error_abs_pct
  -- Updated SLA logic based on horizon
  , case 
      when a.horizon = 0 then case when error_abs <= (0.3 / 100) then 1 else 0 end
      when a.horizon between 1 and 4 then case when error_abs <= (0.3 / 100) then 1 else 0 end
      when a.horizon between 5 and 13 then case when error_abs <= (0.45 / 100) then 1 else 0 end
      else 0
    end as within_SLA
  , b.total_delivs
  , b.ideal_online_hours
from uh_fcst a
left join actual_uh b 
  on a.active_week = b.week 
  and a.submarket_id = b.submarket_id
)

, submarket_level_summary as (
select 
  'Submarket' as aggregation_level
  , metric_cat
  , metric_cat_order
  , forecast_created_week
  , forecast_week
  , horizon
  , scenario
  , config
  , submarket_id
  , fcst_uh
  , actual_uh
  , error
  , error_abs 
  , error_pct
  , error_abs_pct
  , within_SLA
  , total_delivs
  , ideal_online_hours
  , case when total_delivs >= 2000 then '>=2k_delivs' else '<2k_delivs' end as large_sm_flag
  , case 
      when total_delivs < 2000 then '0-2k'
      when total_delivs <= 10000 then '2k-10k'
      when total_delivs <= 100000 then '10k-100k'
      when total_delivs <= 500000 then '100k-500k'
    else '500k+' end as total_delivs_bucket
  , case 
      when total_delivs < 2000 then '1'
      when total_delivs <= 10000 then '2'
      when total_delivs <= 100000 then '3'
      when total_delivs <= 500000 then '4'
    else '5' end as total_delivs_bucket_order
from actuals_vs_forecasts
where 1=1
  and forecast_week <= dateadd('week', -1, date_trunc('week', current_date))
)

, global_level_summary as (
select 
  'Global' as aggregation_level
  , metric_cat
  , metric_cat_order
  , forecast_created_week
  , forecast_week
  , horizon
  , scenario
  , config
  , 0 as submarket_id
  , sum(fcst_uh * ideal_online_hours) / sum(ideal_online_hours) as fcst_uh
  , sum(actual_uh * ideal_online_hours) / sum(ideal_online_hours) as actual_uh
  , fcst_uh - actual_uh as error
  , abs(fcst_uh - actual_uh) as error_abs 
  , div0(fcst_uh, actual_uh) - 1 as error_pct
  , abs(error_pct) as error_abs_pct
  -- Updated SLA logic for global level
  , case 
      when horizon = 0 then case when error_abs <= (0.3 / 100) then 1 else 0 end
      when horizon between 1 and 4 then case when error_abs <= (0.3 / 100) then 1 else 0 end
      when horizon between 5 and 13 then case when error_abs <= (0.45 / 100) then 1 else 0 end
      else 0
    end as within_SLA 
  , sum(total_delivs) as total_delivs
  , sum(ideal_online_hours) as ideal_online_hours
  , 'Global' as large_sm_flag
  , 'Global' as total_delivs_bucket
  , '0' as total_delivs_bucket_order
from actuals_vs_forecasts
where 1=1
  and forecast_week <= dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, combined_with_flags as (
select * from submarket_level_summary
union all
select * from global_level_summary
)

-- SLA Performance Summary for W0 (11/13 weeks requirement)
, w0_sla_summary as (
select
  scenario
  , config
  , submarket_id
  , aggregation_level
  , large_sm_flag
  , total_delivs_bucket
  , count(distinct case when horizon = 0 then forecast_week end) as w0_total_weeks
  , sum(case when horizon = 0 then within_sla else 0 end) as w0_weeks_hit_sla
  , div0(w0_weeks_hit_sla, w0_total_weeks) as w0_sla_hit_rate
  , case when w0_weeks_hit_sla >= 11 and w0_total_weeks >= 13 then 1 else 0 end as w0_meets_11_of_13_requirement
from combined_with_flags
group by all
)

, metric_base_long_format as (
  select 'Actuals' as metric_cat, 0 as metric_cat_order, 0 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Actual_UH' as metric_name, actual_uh as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 1 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Forecast_UH' as metric_name, fcst_uh as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 2 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Error' as metric_name, error as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 3 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Error_ABS' as metric_name, error_abs as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 4 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Error_PCT' as metric_name, error_pct as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 5 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order
  , 'Error_ABS_PCT' as metric_name, error_abs_pct as metric_value from combined_with_flags
)

-- Main output with all metrics
select * from metric_base_long_format

-- Uncomment to see W0 SLA summary instead:
-- select * from w0_sla_summary

-- Uncomment to see combined view with SLA summary joined:
-- select 
--   a.*
--   , b.w0_total_weeks
--   , b.w0_weeks_hit_sla
--   , b.w0_sla_hit_rate
--   , b.w0_meets_11_of_13_requirement
-- from metric_base_long_format a
-- left join w0_sla_summary b 
--   on a.scenario = b.scenario 
--   and a.config = b.config 
--   and a.submarket_id = b.submarket_id
--   and a.aggregation_level = b.aggregation_level
