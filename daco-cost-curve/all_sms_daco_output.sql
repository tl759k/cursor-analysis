  
    
  with dedupe_sms as (
  select 
    active_week
    , scenario
    , submarket_id
    , horizon_spend
    , spending
    , applicants
    , horizon_conv
    , new_dx
    , horizon_ret
    , inc_wad
    , inc_hours
    , forecast_horizon
  from martech.dasher.dac_optimizer_granular_acquisition
  where submarket_id in (334, 237, 236, 73, 305, 79, 78, 92, 8, 55, 81, 17, 36, 34)
    and created_at = (select max(created_at) from martech.dasher.dac_optimizer_granular_acquisition)
)

, other_sms as (
  select 
    active_week
    , scenario
    , submarket_id
    , horizon_spend
    , spending
    , applicants
    , horizon_conv
    , new_dx
    , horizon_ret
    , inc_wad
    , inc_hours
    , forecast_horizon
  from martech.dasher.dac_optimizer_granular_acquisition
  where submarket_id not in (334, 237, 236, 73, 305, 79, 78, 92, 8, 55, 81, 17, 36, 34)
    -- and created_at = (select max(created_at) from martech.dasher.dac_optimizer_granular_acquisition)
)

, dedupe_results as (
select * from dedupe_sms
union all
select * from other_sms
)

  
  
, daco_simulation_output as (
  select
    dateadd(week, horizon_spend, active_week) as spend_date
    , submarket_id
    -- , dateadd(week, (horizon_spend + horizon_conv + horizon_ret), active_week) as final_week
    , avg(spending) as spending
    , sum(applicants) as applicants
    , sum(new_dx) as new_dx
    , sum(inc_wad) as wad
    , sum(inc_hours) as hours
    , avg(spending) / nullif(sum(applicants),0) as cpa
    , avg(spending) / nullif(sum(new_dx),0) as cpd
    , avg(spending) / nullif(sum(inc_wad),0) as cp_wad
    , avg(spending) / nullif(sum(inc_hours),0) as cp_hour
    , case
        when scenario = 'throughQ1-3kMCPD' then 3000
        when scenario = 'planning-1kQ4Q1' then 1000
        when scenario = 'planning-2kQ4Q1' then 2000
        when scenario = 'planning-4kQ4Q1' then 4000
        when scenario = 'planning-5kQ4Q1' then 5000
        when scenario = 'planning-6kQ4Q1' then 6000
        when scenario = 'planning-7kQ4Q1' then 7000
        when scenario = 'planning-8kQ4Q1' then 8000
        when scenario = 'planning-9kQ4Q1' then 9000
        when scenario = 'planning-10kQ4Q1' then 10000
        when scenario = 'planning-11kQ4Q1' then 11000
        when scenario = 'planning-12kQ4Q1' then 12000
        else null
      end as mcpd_scenario
  from martech.dasher.dac_optimizer_granular_acquisition 
  where true
    -- and submarket_id = 81
    and horizon_conv + horizon_ret <= 26
    -- and spend_date = '2025-10-06'
    and spend_date >= '2025-10-01'
    and spend_date <= '2025-12-31'
group by all
order by mcpd_scenario asc
)

, all_spend_date as (
select
  *
  , spending - lag(spending) over(partition by spend_date, submarket_id order by mcpd_scenario asc) as inc_spend
  , applicants - lag(applicants) over(partition by spend_date, submarket_id order by mcpd_scenario asc) as inc_apps
  , new_dx - lag(new_dx) over(partition by spend_date, submarket_id order by mcpd_scenario asc) as inc_new_dx
  , wad - lag(wad) over(partition by spend_date, submarket_id order by mcpd_scenario asc) as inc_wad
  , hours - lag(hours) over(partition by spend_date, submarket_id order by mcpd_scenario asc) as inc_hours
  , coalesce(inc_spend, spending) inc_spend_final
  , coalesce(inc_apps, applicants) inc_apps_final
  , coalesce(inc_new_dx, new_dx) inc_new_dx_final
  , coalesce(inc_wad, wad) inc_wad_final
  , coalesce(inc_hours, hours) inc_hours_final
  , inc_new_dx_final * 170 as inc_hours_lifetime
from daco_simulation_output
where spending <> 0
)

, all_spend_date_agg as (
select
  submarket_id
  , mcpd_scenario
  , avg(spending) as spending
  , avg(applicants) as applicants
  , avg(new_dx) as new_dx
  , avg(wad) as wad
  , avg(hours) as hours
  -- , avg(inc_spend_final) as inc_spend_final
  -- , avg(inc_apps_final) as inc_apps_final
  -- , avg(inc_new_dx_final) as inc_new_dx_final
  -- , avg(inc_wad_final) as inc_wad_final
  -- , avg(inc_hours_final) as inc_hours_final
  -- , avg(inc_hours_lifetime) as inc_hours_lifetime
from all_spend_date
group by submarket_id, mcpd_scenario
)


select
  submarket_id
  , mcpd_scenario
  , spending
  , applicants
  , new_dx
  , wad
  , hours
  -- , inc_spend_final
  -- , inc_hours_final
  -- , inc_wad_final
  -- , inc_new_dx_final
  -- , inc_hours_lifetime
  -- , div0(inc_spend_final, inc_hours_final) as cpih
  -- , div0(inc_spend_final, inc_hours_final) as cpih_adj
  -- , div0(inc_spend_final, inc_hours_lifetime) as cpih_lifetime
  -- , div0(inc_hours_final, inc_wad_final) as hours_per_dx
  -- , div0(inc_spend_final, inc_wad_final) as cpiwad
  -- , div0(inc_spend_final, inc_new_dx_final) as cpid
from all_spend_date_agg
order by submarket_id, mcpd_scenario asc



