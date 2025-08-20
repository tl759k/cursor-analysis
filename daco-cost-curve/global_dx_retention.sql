with paid_media_dasher as (
select
  date_trunc('week', first_dash_date) first_dash_week
  , date_trunc('week', b.created_at) shift_week
  , datediff('week', first_dash_week, shift_week) as tenure
  , count(distinct a.dasher_id) active_dx
  , sum(b.adj_shift_seconds) / 3600 as online_hours
from edw.dasher.dimension_dasher_applicants a
left join edw.dasher.dasher_shifts b on a.dasher_id = b.dasher_id
where 1=1
  and b.created_at::date >= a.first_dash_date::date
  and a.first_dash_date between '2024-07-01' and '2025-07-31'
  and a.dx_acquisition_allocation_channel not in ('Referral', 'Direct')
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
  , a.online_hours
  , b.total_active_dx
  , div0(a.active_dx, b.total_active_dx) as dx_retention
from paid_media_dasher a
left join total_new_dx b on a.first_dash_week = b.first_dash_week
)

select 
  tenure 
  , avg(dx_retention) dx_retention
from weekly_results
group by all
order by tenure asc