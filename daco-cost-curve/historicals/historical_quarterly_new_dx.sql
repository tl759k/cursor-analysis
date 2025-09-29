with submarket_level as (
select
  date_trunc('quarter', first_dash_date) as quarter
  , date_trunc('week', first_dash_date) as week
  , applied_submarket_id as submarket_id
  , count(distinct dasher_applicant_id) paid_media_new_dx
from edw.dasher.dimension_dasher_applicants
where 1=1
  and applied_date between '2024-01-01' and '2025-06-30' -- from applicants who applied during this period from the spend
  and first_dash_date between '2024-01-01' and '2025-06-30' -- new dx gained during this period
  and dx_acquisition_allocation_channel not in ('Referral', 'Direct')
  and applied_submarket_id in (5, 7, 81)
  and date_trunc('quarter', first_dash_date) = date_trunc('quarter', applied_date) -- applicants who dashed during the same quarter as they applied
group by all
)

, global_level as (
select
  date_trunc('quarter', first_dash_date) as quarter
  , date_trunc('week', first_dash_date) as week
  , 0 as submarket_id
  , count(distinct dasher_applicant_id) paid_media_new_dx
from edw.dasher.dimension_dasher_applicants
where 1=1
  and applied_date between '2024-01-01' and '2025-06-30' -- from applicants who applied during this period from the spend
  and first_dash_date between '2024-01-01' and '2025-06-30' -- new dx gained during this period
  and dx_acquisition_allocation_channel not in ('Referral', 'Direct')
  and date_trunc('quarter', first_dash_date) = date_trunc('quarter', applied_date) -- applicants who dashed during the same quarter as they applied
group by all
)

, actuals as (
select * from submarket_level
union all
select * from global_level
)

select 
  submarket_id
  , avg(new_dx_paid_media_optimized_channel) weekly_avg_new_dx
from actuals
group by all
order by 1 asc
