
with paid_media_dasher as (
select
  date_trunc('week', first_dash_date) first_dash_week
  , date_trunc('week', b.created_at) shift_week
  , datediff('week', first_dash_week, shift_week) as tenure
  , a.dasher_id
  , sum(b.adj_shift_seconds) / 3600 as online_hours
from edw.dasher.dimension_dasher_applicants a
left join edw.dasher.dasher_shifts b on a.dasher_id = b.dasher_id
where 1=1
  and b.created_at::date >= a.first_dash_date::date
  and a.first_dash_date between '2024-01-01' and '2024-07-31' -- cohort
  and a.dx_acquisition_allocation_channel not in ('Referral', 'Direct')
group by all
)

, weeks as (
select 
  row_number() over (order by seq4()) - 1 as tenure 
from table(generator(rowcount => 53)) -- lifetime_weeks
)

, all_dx_all_weeks as (
select distinct
  a.tenure
  , b.dasher_id
from weeks a
cross join (select distinct dasher_id from paid_media_dasher) b
)

, all_dx_all_weeks_hrs as (
select
  a.dasher_id
  , a.tenure
  , ifnull(b.online_hours, 0) as online_hours
from all_dx_all_weeks a
left join paid_media_dasher b on a.dasher_id = b.dasher_id and a.tenure = b.tenure
)

, dasher_level_summary as (
select
  dasher_id
  , tenure
  , online_hours 
  , sum(online_hours) over(partition by dasher_id order by tenure rows between unbounded preceding and current row) cum_sum_hr
  , sum(online_hours) over (partition by dasher_id) tot_hr
  , div0(cum_sum_hr, tot_hr) pct_cum_sum
from all_dx_all_weeks_hrs
)

select
  tenure
  , count(distinct case when online_hours > 0 then dasher_id else null end) as tot_active_dx
  , count(distinct dasher_id) as tot_baseline_dx
  , avg(pct_cum_sum) as pct_cum_sum
  , avg(cum_sum_hr) as cum_sum_hr
  , percentile_disc(0.25) within group (order by cum_sum_hr asc) as cum_sum_hr_20perc
  , percentile_disc(0.5) within group (order by cum_sum_hr asc) as cum_sum_hr_50perc
  , percentile_disc(0.75) within group (order by cum_sum_hr asc) as cum_sum_hr_75perc
  , tot_active_dx / tot_baseline_dx as pct_retained
  , avg(online_hours) online_hours
from dasher_level_summary
group by all
order by all