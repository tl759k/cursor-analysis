-- Applicants
with submarket_level as (
select 
  date_trunc('quarter', applied_date) as quarter
  , date_trunc('week', applied_date) as week
  , applied_submarket_id as submarket_id
  , count(distinct case when dx_acquisition_allocation_channel = 'Direct' then dasher_applicant_id end) applicants_organic
  , count(distinct case when dx_acquisition_allocation_channel = 'Referral' then dasher_applicant_id end) applicants_referral
  , count(distinct case when dx_acquisition_allocation_channel in ('Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
      then dasher_applicant_id end) applicants_paid_media_unoptimized_channel
  , count(distinct dasher_applicant_id) applicants_total
  , applicants_total - applicants_referral - applicants_organic - applicants_paid_media_unoptimized_channel as applicants_paid_media_optimized_channel
  , applicants_paid_media_optimized_channel + applicants_paid_media_unoptimized_channel as applicants_paid_media
from edw.dasher.dimension_dasher_applicants
where 1=1
  and applied_date between '2024-01-01' and '2025-06-30'
  and applied_submarket_id in (5, 7, 81)
group by all
)

,  global_level as (
select 
  date_trunc('quarter', applied_date) as quarter
  , date_trunc('week', applied_date) as week
  , 0 as submarket_id
  , count(distinct case when dx_acquisition_allocation_channel = 'Direct' then dasher_applicant_id end) applicants_organic
  , count(distinct case when dx_acquisition_allocation_channel = 'Referral' then dasher_applicant_id end) applicants_referral
  , count(distinct case when dx_acquisition_allocation_channel in ('Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
      then dasher_applicant_id end) applicants_paid_media_unoptimized_channel
  , count(distinct dasher_applicant_id) applicants_total
  , applicants_total - applicants_referral - applicants_organic - applicants_paid_media_unoptimized_channel as applicants_paid_media_optimized_channel
  , applicants_paid_media_optimized_channel + applicants_paid_media_unoptimized_channel as applicants_paid_media
from edw.dasher.dimension_dasher_applicants
where 1=1
  and applied_date between '2024-01-01' and '2025-06-30'
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
 , avg(applicants_paid_media) as avg_weekly_paid_media_apps
from actuals
group by all
order by quarter asc, submarket_id asc