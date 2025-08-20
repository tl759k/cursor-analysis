-- Cost Curve Analysis: x = spend level, y = CPA
---- reporting weeks -----------------------------------------
with spending as (
select
    date_trunc('week', fdsa.spend_date) as spend_week
    , sum(fdsa.allocated_spend) as spend
    , sum(case when m.allocation_channel in ('Referral') then fdsa.allocated_spend end) referral_spend
    , sum(case when m.allocation_channel in ('Direct') then fdsa.allocated_spend end) organic_spend    
    , sum(case when m.allocation_channel in ('Recruitics', 'Performance_Max', 'SEM_Non_brand', 'Zeta_Global', 'SEM_Brand', 'Facebook', 'ACi', 'Performance_Max_Spanish', 'Blisspoint', 'TikTok_Ads'
                                                    , 'Snapchat', 'Discovery_Ads', 'Snapchat_App', 'Discovery_Spanish', 'TikTok_Ads_App', 'Liftoff', 'TikTok_Ads_Spanish', 'Performance Max', 'Youtube') 
              then fdsa.allocated_spend end) paid_media_optimized_spend    
    , sum(case when m.allocation_channel in ('Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
              then fdsa.allocated_spend end) paid_media_unoptimized_spend    
    , paid_media_optimized_spend + paid_media_unoptimized_spend as paid_media_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
where 1=1
  and date_trunc('week', fdsa.spend_date) between '2024-01-01' and dateadd('week', -1, date_trunc('week', current_date))
group by all
)


, actual_applicants as (
select
  date_trunc('week', dda.applied_date) applied_week
  , count(distinct dda.dasher_applicant_id) total_applicants
  , count(distinct case when dx_acquisition_allocation_channel in ('Direct') then dda.dasher_applicant_id end) applicants_organic
  , count(distinct case when dx_acquisition_allocation_channel in ('Referral') then dda.dasher_applicant_id end) applicants_referral
  , count(distinct case when dx_acquisition_allocation_channel in ('Recruitics', 'Performance_Max', 'SEM_Non_brand', 'Zeta_Global', 'SEM_Brand', 'Facebook', 'ACi', 'Performance_Max_Spanish', 'Blisspoint', 'TikTok_Ads'
                                                    , 'Snapchat', 'Discovery_Ads', 'Snapchat_App', 'Discovery_Spanish', 'TikTok_Ads_App', 'Liftoff', 'TikTok_Ads_Spanish', 'Performance Max', 'Youtube') 
          then dda.dasher_applicant_id end) applicants_paid_media_optimized
  , count(distinct case when dx_acquisition_allocation_channel in ('Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter') 
          then dda.dasher_applicant_id end) applicants_paid_media_unoptimized
  , applicants_paid_media_optimized + applicants_paid_media_unoptimized as paid_media_applicants
from edw.dasher.dimension_dasher_applicants as dda 
where 1=1
group by all
)


, actual_new_dx as (
select
  date_trunc('week', dda.applied_date) applied_week
  , date_trunc('week', dda.first_dash_date) first_dash_week
  , count(distinct dda.dasher_applicant_id) new_dx
from edw.dasher.dimension_dasher_applicants as dda 
where 1=1
group by all
)

, first_dash_ratio as (
select
  a.applied_week
  , b.first_dash_week
  , datediff('week', a.applied_week, b.first_dash_week) as horizon
  , a.total_applicants
  , b.new_dx
  , div0(b.new_dx, a.total_applicants) new_dx_ratio
from actual_applicants a
left join actual_new_dx b on a.applied_week = b.applied_week
where 1=1
  and a.applied_week >= '2024-01-01' and a.applied_week <= dateadd('week', -1, date_trunc('week', current_date))
  and horizon >= 0
  and b.first_dash_week <= dateadd('week', -1, date_trunc('week', current_date))
group by all
)

, first_dash_ratio_avg as (
select 
  horizon
  , avg(new_dx_ratio) new_dx_ratio
from first_dash_ratio
where applied_week between '2024-01-01' and '2024-03-31'
group by all
order by horizon asc
)

select * from first_dash_ratio_avg
where horizon <= 52
;

