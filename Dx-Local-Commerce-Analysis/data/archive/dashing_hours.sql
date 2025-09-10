select
  date_trunc('week', local_hour) as reporting_week
  , extract(hour from local_hour) as reporting_hour
  , daypart
  -- , submarket_id
  , sum(total_deliveries) volume
  , sum(total_adj_hours_online) online_hours
  , sum(total_hours_active) active_hours
from edw.dasher.view_agg_supply_metrics_sp_hour a
left join fact_region fr on a.submarket_id = fr.submarket_id
where 1=1
  and fr.country_id = 1 -- limit to U.S. only
  and date_trunc('week', local_hour) between '2025-01-01' and '2025-07-31'
group by all