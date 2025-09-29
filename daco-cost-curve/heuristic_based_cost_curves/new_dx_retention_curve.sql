-- Retention based on WAD
with paid_media_dasher as (
select 
  date_trunc('week', dda.first_dash_date) first_dash_week  
  , date_trunc('week', a.active_date) shift_week
  , datediff('week', first_dash_week, shift_week) as tenure
  , count(distinct a.dasher_id) active_dx
from dimension_deliveries a
left join edw.dasher.dimension_dasher_applicants dda on a.dasher_id = dda.dasher_id
where true
  and a.is_filtered = true
  and a.is_consumer_pickup = false
  and nvl(a.fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
  and dda.first_dash_date between '2023-01-01' and '2025-07-31'
  and dda.dx_acquisition_allocation_channel not in ('Referral', 'Direct')
group by all
)


, total_new_dx as (
select
  first_dash_week
  , sum(active_dx) total_active_dx
from paid_media_dasher
where 1=1 
  and tenure = 0
group by all
)

, weekly_results as (
select
  a.first_dash_week
  , a.shift_week
  , a.tenure
  , a.active_dx
  , b.total_active_dx
  , div0(a.active_dx, b.total_active_dx) as dx_retention
from paid_media_dasher a
left join total_new_dx b on a.first_dash_week = b.first_dash_week
)


select 
  tenure as horizon_ret
  , avg(dx_retention) dx_retention
from weekly_results
where horizon_ret >= 0
group by all
order by tenure asc