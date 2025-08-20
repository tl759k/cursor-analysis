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
select 
  *
  , uh_pct * 100
  , round(total_deliveries / 10000) * 10000 as bucket_delivs
  , round(uh_pct / 0.001) * 0.001 as bucket_uh
from market_facts
where 1=1
  -- and submarket_id = 81 -- 280000
  and bucket_delivs between 200000 and 350000
  and bucket_uh >= 0.01
order by total_deliveries