with market_facts as (
select
  date_trunc('week', local_hour) as week
  , a.submarket_id
  , b.submarket_name
  , sum(a.total_deliveries) total_deliveries
  , div0(sum(total_hours_undersupply), nullif(sum(total_hours_online_ideal),0)) as uh_pct
  , sum(total_hours_undersupply) as undersupplied_hours
  , sum(total_hours_online_ideal) as ideal_online_hours
from edw.dasher.view_agg_supply_metrics_sp_hour a
left join fact_region b on a.submarket_id = b.submarket_id
where 1=1
  and date_trunc('week', local_hour) = dateadd('week', -1, date_trunc('week', current_date))
  and b.country_id = 1
group by all
)

-- ops locked budget 
, planned_spend as (
select 
  week
  , submarket_id
  , sum(abs(total_overrides)) total_overrides_abs
  , sum(total_overrides) total_overrides
  , sum(total_budget) total_budget
  -- there should be 17 channels in the source table
  -- , sum(SEM_nonbrand) SEM_nonbrand_planned
  -- , sum(SEM_brand) SEM_brand_planned
  -- , sum(Facebook) Facebook_planned
  -- -- , sum(Craigslist) Craigslist
  -- , sum(Recruitics) Recruitics_planned
  -- , sum(Snapchat) Snapchat_planned
  -- , sum(Tiktok_ads) Tiktok_ads_planned
  -- , sum(Youtube) Youtube_planned
  -- , sum(Discovery_ads) Discovery_ads_planned
  -- , sum(Performance_max)  Performance_max_planned
  -- , sum(Zeta_global) Zeta_global_planned
  -- , sum(Blisspoint) Blisspoint_planned
  -- , sum(Aci) Aci_planned
  -- , sum(Discovery_spanish) Discovery_spanish_planned
  -- , sum(Liftoff) Liftoff_planned
  -- , sum(Performance_max_spanish) Performance_max_spanish_planned
  -- , sum(Tiktok_ads_spanish) Tiktok_ads_spanish_planned
from static.optimized_dx_budget a
where 1=1
  and week = dateadd('week', -1, date_trunc('week', current_date))
group by all
)

select 
  a.*
  , uh_pct * 100
  , round(total_deliveries / 10000) * 10000 as bucket_delivs
  , round(uh_pct / 0.001) * 0.001 as bucket_uh
  , b.total_budget
  , round(b.total_budget / 10000) * 10000 as bucket_spend
from market_facts a
left join planned_spend b on a.submarket_id = b.submarket_id
where 1=1
  -- and total_deliveries >= 2000
  -- and  b.total_budget > 1000
  -- and submarket_id = 81 -- 280000
  -- and bucket_delivs between 200000 and 350000
  -- and bucket_uh >= 0.01
order by total_deliveries