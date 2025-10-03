-- UH Forecast vs Actuals Dashboard with SLA Tracking
-- SLA Definition:
-- w0 forecast: more than 11/13 weeks hitting SLA (error < 0.3%)
-- w1-4: error < 0.3%
-- w5-13: error < 0.45%

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
  -- Dynamic SLA thresholds based on horizon
  , case 
      when a.horizon = 0 then 0.3 / 100  -- w0: 0.3%
      when a.horizon between 1 and 4 then 0.3 / 100  -- w1-4: 0.3%
      when a.horizon between 5 and 13 then 0.45 / 100  -- w5-13: 0.45%
      else 0.45 / 100  -- default for horizons > 13
    end as sla_threshold
  , case when error_abs <= sla_threshold then 1 else 0 end as within_SLA
  , b.total_delivs
  , b.ideal_online_hours
from uh_fcst a
left join actual_uh b on a.active_week = b.week and a.submarket_id = b.submarket_id
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
  , sla_threshold
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
  , sum(fcst_uh * ideal_online_hours) / sum(ideal_online_hours) as fcst_uh_agg
  , sum(actual_uh * ideal_online_hours) / sum(ideal_online_hours) as actual_uh_agg
  , fcst_uh_agg - actual_uh_agg as error_agg
  , abs(fcst_uh_agg - actual_uh_agg) as error_abs_agg 
  , div0(fcst_uh_agg, actual_uh_agg) - 1 as error_pct_agg
  , abs(error_pct_agg) as error_abs_pct_agg
  -- Use the same SLA threshold logic for global aggregation
  , case 
      when horizon = 0 then 0.3 / 100
      when horizon between 1 and 4 then 0.3 / 100
      when horizon between 5 and 13 then 0.45 / 100
      else 0.45 / 100
    end as sla_threshold_agg
  , case when error_abs_agg <= sla_threshold_agg then 1 else 0 end as within_SLA 
  , sum(total_delivs) total_delivs_agg
  , sum(ideal_online_hours) ideal_online_hours_agg
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

-- SLA Performance Summary for W0 (special case: needs 11/13 weeks hitting SLA)
, w0_sla_performance as (
select
  aggregation_level
  , scenario
  , config
  , submarket_id
  , large_sm_flag
  , total_delivs_bucket
  , total_delivs_bucket_order
  , count(*) as total_w0_forecasts
  , sum(within_sla) as w0_forecasts_hitting_sla
  , div0(w0_forecasts_hitting_sla, total_w0_forecasts) as w0_sla_hit_rate
  , case when w0_forecasts_hitting_sla >= 11 and total_w0_forecasts >= 13 then 1 else 0 end as w0_overall_sla_met
from combined_with_flags
where horizon = 0
  and forecast_week is not null
  and actual_uh is not null
group by all
)

-- SLA Performance Summary for W1-13 (standard SLA tracking)
, w1_13_sla_performance as (
select
  aggregation_level
  , scenario
  , config
  , submarket_id
  , horizon
  , large_sm_flag
  , total_delivs_bucket
  , total_delivs_bucket_order
  , count(*) as total_forecasts
  , sum(within_sla) as forecasts_hitting_sla
  , div0(forecasts_hitting_sla, total_forecasts) as sla_hit_rate
  , case 
      when horizon between 1 and 4 and sla_hit_rate >= 1.0 then 1  -- 100% for w1-4
      when horizon between 5 and 13 and sla_hit_rate >= 1.0 then 1  -- 100% for w5-13
      else 0 
    end as horizon_sla_met
from combined_with_flags
where horizon between 1 and 13
  and forecast_week is not null
  and actual_uh is not null
group by all
)

, metric_base_long_format as (
  select 'Actuals' as metric_cat, 0 as metric_cat_order, 0 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Actual_UH' as metric_name, actual_uh as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 1 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Forecast_UH' as metric_name, fcst_uh as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 2 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Error' as metric_name, error as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 3 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Error_ABS' as metric_name, error_abs as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 4 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Error_PCT' as metric_name, error_pct as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 5 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Error_ABS_PCT' as metric_name, error_abs_pct as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 6 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'SLA_Threshold' as metric_name, sla_threshold as metric_value from combined_with_flags union all
  
  select metric_cat, metric_cat_order, 7 as metric_name_order, forecast_created_week, forecast_week, horizon, scenario, config, submarket_id, within_sla, total_delivs, aggregation_level, large_sm_flag, ideal_online_hours, total_delivs_bucket, total_delivs_bucket_order, sla_threshold
  , 'Within_SLA' as metric_name, within_sla as metric_value from combined_with_flags
)

-- Main output for dashboard
select * from metric_base_long_format

-- Uncomment below for SLA summary views:
-- 
-- -- W0 SLA Summary
-- select 'W0_SLA_Summary' as report_type, * from w0_sla_performance
-- 
-- -- W1-13 SLA Summary  
-- select 'W1_13_SLA_Summary' as report_type, * from w1_13_sla_performance