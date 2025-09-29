-- Cost Curve Analysis: x = spend level, y = CPA
---- reporting weeks -----------------------------------------
with spending as (
select
    date_trunc('week', fdsa.spend_date) as spend_week
    , submarket_id
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
  and fdsa.spend_date between '2024-01-01' and '2025-08-31'
  -- and (fdsa.spend_date between '2024-10-01' and '2025-03-31'
  --     or fdsa.spend_date between '2023-10-01' and '2024-03-31')
group by all
)

, actual_applicants as (
select
  date_trunc('week', dda.applied_date) applied_week
  , applied_submarket_id
  , count(distinct case when dda.dx_acquisition_allocation_channel in ('Recruitics', 'Performance_Max', 'SEM_Non_brand', 'Zeta_Global', 'SEM_Brand', 'Facebook', 'ACi', 'Performance_Max_Spanish', 'Blisspoint', 'TikTok_Ads'
                                                    , 'Snapchat', 'Discovery_Ads', 'Snapchat_App', 'Discovery_Spanish', 'TikTok_Ads_App', 'Liftoff', 'TikTok_Ads_Spanish', 'Performance Max', 'Youtube') 
              then dda.dasher_applicant_id end) paid_media_optimized_applicants
  , count(distinct case when dda.dx_acquisition_bucket = 'Paid' then dda.dasher_applicant_id end) paid_media_applicants
from edw.dasher.dimension_dasher_applicants as dda 
where 1=1
  and dda.applied_date between '2024-01-01' and '2025-08-31'
  -- and (dda.applied_date between '2024-10-01' and '2025-03-31'
      -- or dda.applied_date between '2023-10-01' and '2024-03-31')
group by all
)

select
  a.spend_week
  -- , submarket_id
  , sum(a.paid_media_optimized_spend) paid_media_spend
  -- , sum(b.total_applicants) total_applicants
  , sum(b.paid_media_optimized_applicants) paid_media_applicants
from spending a
left join actual_applicants b on a.spend_week = b.applied_week and a.submarket_id = b.applied_submarket_id
left join fact_region fr on a.submarket_id = fr.submarket_id
where 1=1
  -- and submarket_id = 81
  and fr.country_id = 1
group by all
order by spend_week desc