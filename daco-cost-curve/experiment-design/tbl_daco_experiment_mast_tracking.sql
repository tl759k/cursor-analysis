
-- assumes exp run for 4 mo between April - July, lookback 16 weeks, and set baseline as another 16 weeks back
set start_date = '2025-01-01';
set end_date =  '2025-03-31';
-- set baseline for another 16 weeks
set baseline_start_date = dateadd('week', -13, $start_date);
set baseline_end_date =  dateadd('week', -1, $start_date);


create or replace table static.tbl_daco_experiment_master_tracking_v2 as 

with supply_metrics as (
select
  date_trunc('week', local_hour) as week
  , case when week between $baseline_start_date and $baseline_end_date then 'pre' else 'post' end week_period
  , a.submarket_id
  , b.submarket_name
  , b.country_id
  , ex.exp_group
  , div0(sum(total_hours_undersupply), nullif(sum(total_hours_online_ideal),0)) as uh_pct
  , div0(sum(total_hours_oversupply), nullif(sum(total_hours_online_ideal),0)) as oh_pct
  , sum(total_hours_undersupply) as undersupplied_hours
  , sum(total_hours_oversupply) as oversupplied_hours
  , sum(total_hours_online_ideal) - sum(total_hours_undersupply) contribution_hours -- or non oversupply hours
  , sum(total_hours_online_ideal) as ideal_online_hours
  , sum(total_deliveries) as num_delivs
  , sum(total_hours_active) as active_hours
  , sum(total_adj_hours_online) as hours_online_actual
  , div0(sum(total_hours_active), nullif(sum(total_adj_hours_online), 0)) as utilization 
  , div0(sum(total_deliveries), sum(total_hours_active)) as AE
from edw.dasher.view_agg_supply_metrics_sp_hour a
inner join fact_region b on a.submarket_id = b.submarket_id
inner join static.daco_exp_group_split_0331_v2 ex on a.submarket_id = ex.submarket_id
where 1=1
  and date_trunc('week', local_hour) between $baseline_start_date and $end_date 
group by all
)

-- section 2: quality metrics
, quality_metrics as (
select
  date_trunc('week', dd.active_date) as week
  , dd.submarket_id
  -- , count(distinct dd.delivery_id) as num_delivs
  -- quality metrics
  , avg(case when is_asap then least(datediff('seconds', dd.created_at, dd.actual_delivery_time), 2*60*60) else null end) as asap_seconds
  , avg(case when is_asap then least(datediff('seconds', dd.created_at, dd.quoted_delivery_time), 2*60*60) else null end) as quoted_asap_seconds
  , avg(datediff('second', dd.first_assignment_made_time, dd.dasher_confirmed_time)) as conflat_seconds
  , avg(datediff('seconds', dd.quoted_delivery_time, dd.actual_delivery_time)) as lateness_seconds
  , avg(case when datediff('seconds', dd.quoted_delivery_time, dd.actual_delivery_time) > 20 * 60 then 1 else 0 end) as late20min_rate
  , avg(fcdm.is_missing_incorrect) as mni
  , avg(distinct_active_duration) as dat
  -- pay metrics
  -- , avg(dasher_base_pay)  as base_pay
  -- , avg(proactive_incentives) as proactive_incentives
  -- , avg(reactive_incentives) as reactive_incentives
  -- , avg(dasher_catch_all) as top_up_pay
  , count(distinct dd.dasher_id) wad
from dimension_deliveries dd 
inner join static.daco_exp_group_split_0331_v2 ex on dd.submarket_id = ex.submarket_id
left join fact_core_delivery_metrics fcdm on dd.delivery_id = fcdm.delivery_id
where 1=1
  and dd.is_filtered = 1
  and dd.is_consumer_pickup = 0
  and nvl(dd.fulfillment_type,'') not in ('merchant_fleet', 'consumer_pickup', 'virtual','shipping')
  and date_trunc('week', dd.active_date) between $baseline_start_date and $end_date 
group by all
)

-- section 3: active dx
-- , wad as (
-- select 
--   date_trunc('week', dd.active_date) week
--   , dd.submarket_id
--   -- , count(distinct delivery_id) weekly_volume
--   , count(distinct dd.dasher_id) wad
-- from dimension_deliveries dd
-- inner join static.daco_exp_group_split_0331_v2 ex on dd.submarket_id = ex.submarket_id
-- where true
--   and active_date between $baseline_start_date and $end_date 
--   and nvl(fulfillment_type,'') not in ('virtual','shipping','merchant_fleet')
-- group by all
-- )

-- section 4: spending metrics
, paid_media_spend as (
select
  date_trunc('week', fdsa.spend_date) as week
  , fdsa.submarket_id
  , sum(case when m.allocation_channel in ('Recruitics', 'Performance_Max', 'SEM_Non_brand', 'Zeta_Global', 'SEM_Brand', 'Facebook', 'ACi', 'Performance_Max_Spanish', 'Blisspoint', 'TikTok_Ads'
                                    , 'Snapchat', 'Discovery_Ads', 'Snapchat_App', 'Discovery_Spanish', 'TikTok_Ads_App', 'Liftoff', 'TikTok_Ads_Spanish', 'Youtube'
                                    , 'ACi_iOS', 'Apple_Search_Ads')
      then fdsa.allocated_spend end) as paid_media_optimized_channel_spend
  , sum(case when m.allocation_channel in ('Impact_Radius', 'ACe', 'CampusGroup', 'Partnerships', 'Wavemaker', 'Twitter', 'GetSales', 'Direct') 
      then fdsa.allocated_spend end) as paid_media_unoptimized_channel_spend
  , sum(fdsa.allocated_spend) as paid_media_spend
from edw.growth.fact_dasher_spend_allocation as fdsa
join proddb.static.dasher_spending_channel_mapping as m using(channel, subchannel, partner)
inner join static.daco_exp_group_split_0331_v2 ex on fdsa.submarket_id = ex.submarket_id
where 1=1
  and date_trunc('week', fdsa.spend_date) >= $baseline_start_date
group by all
)

-------- WL applicants ---- 


---========= Waitlist Applicants ========----------
--- all waitlist applicants
, old_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'TOF_WAITLIST_PAGE'
)

, old_bof as ( -- Launched June 2024
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'BACKGROUND_CHECK'
  and coalesce(is_in_bgc_waitlist_blocker_experiment, false) = true
)
  --- NEW WAITLIST ---  
, hard_block_render_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_HARD_BLOCK_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_AFTER_VEHICLE'
)

, hard_block_render_bof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_HARD_BLOCK_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_BEFORE_BGC'
)

-- Reserved  
, reserved_render_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_LIMITED_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_AFTER_VEHICLE'
)

, reserved_render_bof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_LIMITED_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_BEFORE_BGC'
)

, waitlist_apps as (
select distinct
  case
    when old_tof.unique_link is not null
    or old_bof.unique_link is not null
    or hbtof.unique_link is not null
    or hbbof.unique_link is not null
    or rtof.unique_link is not null
    or rbof.unique_link is not null then 'Hit Waitlist'
    else 'No Waitlist'
  end as all_waitlist_flag
  , dasher_applicant_id
from edw.dasher.dimension_dasher_applicants dda
-- Old WL
left join old_tof on old_tof.unique_link = dda.unique_link
left join old_bof on old_bof.unique_link = dda.unique_link
-- New WL 
left join hard_block_render_tof hbtof on hbtof.unique_link = dda.unique_link
left join hard_block_render_bof hbbof on hbbof.unique_link = dda.unique_link
left join reserved_render_tof rtof on rtof.unique_link = dda.unique_link
left join reserved_render_bof rbof on rbof.unique_link = dda.unique_link
)

-----=========== End of WL Applicants ===========-------------------------------------

, apps_conversion as (
select 
  date_trunc('week', applied_date) as week
  , applied_submarket_id as submarket_id
  , count(distinct case when dx_acquisition_bucket = 'Paid' then dda.dasher_applicant_id end) as paid_media_applicants
  , count(distinct case when dx_acquisition_bucket = 'Paid' and datediff('day', applied_date, first_dash_date) <= 6 then dda.dasher_applicant_id end) paid_media_new_dx_7d
  , count(distinct case when dx_acquisition_bucket = 'Direct' then dda.dasher_applicant_id end) as organic_applicants
  , count(distinct case when dx_acquisition_bucket = 'Direct' and datediff('day', applied_date, first_dash_date) <= 6 then dda.dasher_applicant_id end) organic_new_dx_7d
  , count(distinct case when dx_acquisition_bucket = 'Referral' then dda.dasher_applicant_id end) as referral_applicants
  , count(distinct case when dx_acquisition_bucket = 'Referral' and datediff('day', applied_date, first_dash_date) <= 6 then dda.dasher_applicant_id end) referral_new_dx_7d
  , count(distinct case when datediff('day', applied_date, first_dash_date) <= 6 then dda.dasher_applicant_id end) total_new_dx_7d
  , count(distinct dda.dasher_applicant_id) as total_applicants
  , div0(total_new_dx_7d, total_applicants) as first_delivery_7d_cvr
  -- non wl cvr
  , count(distinct case when wl.all_waitlist_flag = 'No Waitlist' and datediff('day', applied_date, first_dash_date) <= 6 then dda.dasher_applicant_id end) total_new_dx_7d_non_wl
  , count(distinct case when wl.all_waitlist_flag = 'No Waitlist' then dda.dasher_applicant_id end) as total_applicants_non_wl
  , div0(total_new_dx_7d_non_wl, total_applicants_non_wl) as first_delivery_7d_cvr_non_wl
from edw.dasher.dimension_dasher_applicants as dda
inner join static.daco_exp_group_split_0331_v2 ex on dda.applied_submarket_id = ex.submarket_id
left join waitlist_apps wl on dda.dasher_applicant_id = wl.dasher_applicant_id
where true
  and week between $baseline_start_date and $end_date 
group by all
)

, total_new_dx as (
select 
  date_trunc('week', first_dash_date) as week
  , applied_submarket_id as submarket_id
  , count(distinct case when dx_acquisition_bucket = 'Paid' then dasher_applicant_id end) as paid_media_new_dx
  , count(distinct case when dx_acquisition_bucket = 'Direct' then dasher_applicant_id end) as organic_new_dx
  , count(distinct case when dx_acquisition_bucket = 'Referral' then dasher_applicant_id end) as referral_new_dx
  , count(distinct dasher_applicant_id) as total_new_dx
from edw.dasher.dimension_dasher_applicants as dda
inner join static.daco_exp_group_split_0331_v2 ex on dda.applied_submarket_id = ex.submarket_id
where true
  and week between $baseline_start_date and $end_date 
group by all
)


, cpa_cpd as (
select
  a.*
  , b.paid_media_applicants
  , b.organic_applicants
  , b.referral_applicants
  , b.total_applicants
  , b.paid_media_new_dx_7d
  , b.organic_new_dx_7d
  , b.referral_new_dx_7d  
  , b.total_new_dx_7d  
  , b.first_delivery_7d_cvr
  , b.first_delivery_7d_cvr_non_wl
  , a.paid_media_spend / nullif(b.paid_media_applicants, 0) as paid_media_cpa
  , a.paid_media_spend / nullif(b.paid_media_new_dx_7d, 0) as paid_media_cpd
  , c.paid_media_new_dx
  , c.organic_new_dx
  , c.referral_new_dx
  , c.total_new_dx
from paid_media_spend a 
left join apps_conversion b on a.week = b.week and a.submarket_id = b.submarket_id
left join total_new_dx c on a.week = c.week and a.submarket_id = c.submarket_id
)

select
  a.week
  , a.week_period
  , a.submarket_id
  , a.submarket_name
  , a.exp_group
  , a.country_id
  , a.uh_pct
  , a.oh_pct
  , a.ideal_online_hours
  , a.num_delivs
  , a.utilization
  , a.ae
  , a.active_hours
  , a.hours_online_actual
  , a.contribution_hours
  , b.asap_seconds
  , b.quoted_asap_seconds
  , b.conflat_seconds
  , b.lateness_seconds
  , b.late20min_rate
  , b.mni
  , b.dat
  , b.wad
  , d.paid_media_spend
  , d.paid_media_applicants -- from cw apps
  , d.paid_media_new_dx_7d -- from cw apps
  , d.paid_media_cpa
  , d.paid_media_cpd
  , d.organic_applicants
  , d.organic_new_dx_7d
  , d.referral_applicants
  , d.referral_new_dx_7d
  , d.paid_media_new_dx
  , d.organic_new_dx
  , d.referral_new_dx
  , d.total_new_dx
  , d.total_new_dx_7d
  , d.total_applicants
  , d.first_delivery_7d_cvr
  , d.first_delivery_7d_cvr_non_wl
from supply_metrics a
left join quality_metrics b on a.week = b.week and a.submarket_id = b.submarket_id
-- left join wad c on a.week = c.week and a.submarket_id = c.submarket_id
left join cpa_cpd d on a.week = d.week and a.submarket_id = d.submarket_id
;

grant select on static.tbl_daco_experiment_master_tracking to public;