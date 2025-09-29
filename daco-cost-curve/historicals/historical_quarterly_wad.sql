-- Actual WAD from those who applied during Q4/Q1
with submarket_level as (
select 
  date_trunc('quarter', ds.active_date) as quarter
  , date_trunc('week', ds.active_date) as week
  , ds.submarket_id
  , count(distinct ds.dasher_id) dx_cnt
  , sum(ds.adj_shift_seconds / 3600) as online_hours
from edw.dasher.dasher_shift_starting_points ds
left join edw.dasher.dimension_dasher_applicants b on ds.dasher_id = b.dasher_id
where 1=1
  and ds.dasher_id is not null
  and ds.check_in_time is not null
  and ds.check_out_time is not null
  and ds.check_out_time > ds.check_in_time
  and ds.has_preassign = false
  and ds.active_date between '2024-01-01' and '2025-06-30'
  and b.applied_date between '2024-01-01' and '2025-06-30'
  and date_trunc('quarter', ds.active_date) = date_trunc('quarter', b.applied_date)
  and b.dx_acquisition_allocation_channel not in ('Direct', 'Referral') 
  and ds.submarket_id in (5, 7, 81)
group by all
)

, global_level as (
select 
  date_trunc('quarter', ds.active_date) as quarter
  , date_trunc('week', ds.active_date) as week
  , 0 as submarket_id
  , count(distinct ds.dasher_id) dx_cnt
  , sum(ds.adj_shift_seconds / 3600) as online_hours
from edw.dasher.dasher_shift_starting_points ds
left join edw.dasher.dimension_dasher_applicants b on ds.dasher_id = b.dasher_id
where 1=1
  and ds.dasher_id is not null
  and ds.check_in_time is not null
  and ds.check_out_time is not null
  and ds.check_out_time > ds.check_in_time
  and ds.has_preassign = false
  and ds.active_date between '2024-01-01' and '2025-06-30'
  and b.applied_date between '2024-01-01' and '2025-06-30'
  and date_trunc('quarter', ds.active_date) = date_trunc('quarter', b.applied_date)
  and b.dx_acquisition_allocation_channel not in ('Direct', 'Referral') 
group by all
)

, actuals as (
select * from submarket_level
union all
select * from global_level
)

select
  quarter
  , submarket_id
  , avg(online_hours) online_hours_avg
  , avg(dx_cnt) dx_cnt_avg
  , online_hours_avg / dx_cnt_avg as hours_per_dx
from actuals
where 1=1
group by all
order by quarter asc, submarket_id asc
