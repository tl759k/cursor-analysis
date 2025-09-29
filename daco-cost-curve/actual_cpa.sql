-- Cost Curve Analysis: x = spend level, y = CPA
---- reporting weeks -----------------------------------------
with spending as (
select
    date_trunc('week', fdsa.spend_date) as spend_week
    , sum(fdsa.allocated_spend) paid_media_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
where 1=1
  and spend_week between '2024-01-01' and dateadd('week', -26, date_trunc('week', current_date))
  and m.allocation_channel not in ('Referral', 'Direct', 'Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
  and m.allocation_channel is not null
group by all
)

, applicants_and_new_dx as (
select
  date_trunc('week', dda.applied_date) applied_week
  , count(distinct dda.dasher_applicant_id) paid_media_applicants
  , count(distinct case when datediff('week', applied_week, date_trunc('week', first_dash_date)) between 0 and 25 then dda.dasher_applicant_id end) paid_media_new_dx_26w
  -- , count(distinct case when dda.dx_acquisition_bucket = 'Paid' and datediff('week', applied_week, date_trunc('week', first_dash_date)) between 0 and 51 then dda.dasher_applicant_id end) paid_media_new_dx_52w 
from edw.dasher.dimension_dasher_applicants as dda 
where 1=1
  and applied_week between '2024-01-01' and dateadd('week', -26, date_trunc('week', current_date))
  and dda.dx_acquisition_allocation_channel not in ('Referral', 'Direct', 'Referral', 'Direct', 'Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter')
  and dda.dx_acquisition_allocation_channel is not null
group by all
)

, wad as (
select 
  date_trunc('week', dda.applied_date) applied_week
  , date_trunc('week', a.active_date) active_week
  , datediff('week', applied_week, active_week) horizon
  , count(distinct a.dasher_id) as wad
from dimension_deliveries a
left join edw.dasher.dimension_dasher_applicants dda on dda.dasher_id = a.dasher_id
where true
  and date_trunc('week', dda.applied_date) between '2024-01-01' and dateadd('week', -26, date_trunc('week', current_date))
  and dda.dx_acquisition_allocation_channel not in ('Referral', 'Direct', 'Referral', 'Direct', 'Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter')
  and dda.dx_acquisition_allocation_channel is not null
  and a.is_filtered = true
  and a.is_consumer_pickup = false
  and nvl(a.fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
group by all
)

, summary as (
select
  a.spend_week
  , a.paid_media_spend
  , b.paid_media_applicants
  -- , b.paid_media_new_dx_26w
  -- , b.paid_media_new_dx_52w
  -- , c.paid_media_wad_26w
  -- , c.paid_media_wad_26w_avg
  -- , d.paid_media_wad_52w  -- commented out because we don't have 52w data
from spending a
left join applicants_and_new_dx b on a.spend_week = b.applied_week
-- left join (select applied_week, sum(wad) paid_media_wad_26w, avg(wad) paid_media_wad_26w_avg from wad where horizon between 0 and 25 group by all) c on b.applied_week = c.applied_week
-- left join (select applied_week, sum(wad) paid_media_wad_52w from wad where horizon between 0 and 51 group by all) d on b.applied_week = d.applied_week
)

select * from summary