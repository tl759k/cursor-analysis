with new_dx as (
select
    date_trunc('week', dda.applied_date) applied_week
    , date_trunc('week', dda.first_dash_date) first_dash_week
    , datediff('week', applied_week, first_dash_week) as horizon
    , count(distinct dda.dasher_applicant_id) new_dx
from edw.dasher.dimension_dasher_applicants as dda
where 1=1
  and dda.applied_date between '2023-01-01' and dateadd('week', -1, date_trunc('week', current_date))
  and dda.dx_acquisition_bucket = 'Paid'
  and dda.applied_submarket_id = 81 -- Pitts
group by all
)

, applicants as (
select
    date_trunc('week', dda.applied_date) applied_week
    , count(distinct dda.dasher_applicant_id) apps
from edw.dasher.dimension_dasher_applicants as dda
where 1=1
  and dda.applied_date between '2023-01-01' and dateadd('week', -1, date_trunc('week', current_date))
  and dda.dx_acquisition_bucket = 'Paid'
  and dda.applied_submarket_id = 81 -- Pitts
group by all
) 

, weekly_results as (
select
  a.*
  , b.apps
  , div0(a.new_dx, b.apps) new_dx_ratio
from new_dx a
left join applicants b on a.applied_week = b.applied_week
-- where horizon <= 52
group by all
order by horizon asc
)


-- take 6/9 and 6/16 avg for horizon 0-12, and then 9/2/24 and 9/9/24 avg for horizon 13-52
, horizon_results as (
select
  horizon
  , avg(new_dx_ratio) new_dx_ratio
from weekly_results
where 1=1
  and applied_week between '2025-06-09' and '2025-06-16'
  and horizon between 0 and 12
group by all

union all

select
  horizon
  , avg(new_dx_ratio) new_dx_ratio
from weekly_results
where 1=1
  and applied_week between '2024-09-02' and '2024-09-09'
  and horizon between 13 and 52
group by all
order by horizon asc
)

select 
  horizon as horizon_conv
  , new_dx_ratio
from horizon_results
order by horizon asc