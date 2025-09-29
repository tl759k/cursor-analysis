with spending as (
select
    date_trunc('week', fdsa.spend_date) as spend_week
    , fdsa.submarket_id
    , sum(fdsa.allocated_spend) paid_media_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
where 1=1
  and spend_week between '2023-10-01' and dateadd('week', -1, date_trunc('week', current_date))
  and m.allocation_channel not in ('Referral', 'Direct', 'Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
  and m.allocation_channel is not null
  and fdsa.submarket_id in (62, 167, 72, 86, 84, 87, 237, 334, 73, 236, 68, 305, 96, 83, 79, 78, 8, 2, 92, 55, 6, 81, 11, 39, 17, 36, 48, 37, 91,
 34, 31, 134, 99, 32, 58, 16, 25, 66, 38, 30, 59, 20, 56, 61, 70,  33, 21, 13, 63, 7, 3)
group by all
)

, applicants_and_new_dx as (
select
  date_trunc('week', dda.applied_date) applied_week
  , dda.applied_submarket_id
  , count(distinct dda.dasher_applicant_id) paid_media_applicants
  -- , count(distinct case when datediff('week', applied_week, date_trunc('week', first_dash_date)) between 0 and 25 then dda.dasher_applicant_id end) paid_media_new_dx_26w
  -- , count(distinct case when dda.dx_acquisition_bucket = 'Paid' and datediff('week', applied_week, date_trunc('week', first_dash_date)) between 0 and 51 then dda.dasher_applicant_id end) paid_media_new_dx_52w 
from edw.dasher.dimension_dasher_applicants as dda 
where 1=1
  and applied_week between '2023-10-01' and dateadd('week', -1, date_trunc('week', current_date))
  and dda.dx_acquisition_allocation_channel not in ('Referral', 'Direct', 'Referral', 'Direct', 'Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter')
  and dda.dx_acquisition_allocation_channel is not null
  and dda.applied_submarket_id in (62, 167, 72, 86, 84, 87, 237, 334, 73, 236, 68, 305, 96, 83, 79, 78, 8, 2, 92, 55, 6, 81, 11, 39, 17, 36, 48, 37, 91,
 34, 31, 134, 99, 32, 58, 16, 25, 66, 38, 30, 59, 20, 56, 61, 70,  33, 21, 13, 63, 7, 3)
group by all
)

select
  a.spend_week
  , a.submarket_id
  , a.paid_media_spend
  , b.paid_media_applicants
  , div0(a.paid_media_spend, b.paid_media_applicants) CPA
from spending a
left join applicants_and_new_dx b on a.spend_week = b.applied_week and a.submarket_id = b.applied_submarket_id
order by spend_week asc