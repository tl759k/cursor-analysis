create or replace table proddb.static.tbl_major_steps_conversion_analysis_applied_L7D_cohort_snowflake as 

with week_dates as (
select distinct
  first_date_of_week_iso as week_start
  , last_date_of_week_iso as week_end
from edw.core.dimension_dates
where first_date_of_week_iso between '2024-01-01' and current_date
order by week_start desc
)

, good_or_bad_dashers as (
select distinct
  dasher_applicant_id
  , case 
      when is_deactivated_email = 1  then 'bad'
      when is_deactivated_phone = 1 then 'bad'      
      when is_deactivated_dl_token = 1 then 'bad'      
      when is_deactivated_ssn = 1 then 'bad'      
      when phone_count_applicants > 1 then 'bad' 
      when count_dash_ssn > 1 then 'bad'
      else 'good'
      end as good_or_bad_bucket
  , case
      when is_deactivated_email = 1  then 'bad'
      when is_deactivated_phone = 1 then 'bad'
      when is_deactivated_dl_token = 1 then 'bad'
      when is_deactivated_ssn = 1 then 'bad'
      when email_count_applicants>1 then 'bad' -- additional
      -- when VOIP_PHONE_INDICATOR = 1 then 'bad'
      when phone_count_applicants>1 then 'bad'
      when dl_count_dasher >1 then 'bad'  -- additional
      when count_dash_ssn >1 then 'bad'
      when COUNTRY_CONTINENT_NAME not in ('North America') then 'bad'  -- additional
      else 'good'
      end as good_or_bad_bucket_2
from sandhyasriraman.dasher_applicant_linkage_1
)

, phone_duplicates as ( 
select 
  phone_number
  , min(applied_date) as first_applied_date
  , count(distinct dasher_applicant_id) as dx_count
from edw.dasher.dimension_dasher_applicants 
where 1=1
group by 1
having dx_count > 1 
)


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


---====== Heatmap impression and Assignments ====-------------------------------------
, hm_impression_prep as (
select distinct
  w.week_start
  , w.week_end
  , hml.user_id as dasher_id
  , convert_timezone('UTC', 'America/Los_Angeles', hml.sent_at) as sent_timestamp
  , lower(sp_busyness) as sp_busyness
from week_dates w
join segment_events_raw.driver_production.m_home_heatmap_loaded as hml
  on hml.received_at::date between w.week_start and w.week_end

union all

select distinct 
  w.week_start
  , w.week_end
  , hml.dasher_id
  , convert_timezone('UTC', 'America/Los_Angeles', impression_timestamp_local) as sent_timestamp
  , lower(sp_busyness) as sp_busyness
from week_dates w
join edw.dasher.fact_dasher_access_impressions as hml
  on hml.impression_timestamp_utc::date between w.week_start and w.week_end
  and data_version_identifier = 'MONARCH'
)

, activated_new_dx_only as (
select distinct
  cast(a.dasher_id as int) dasher_id
  , a.week_start
  , a.week_end
  , a.sent_timestamp
  , a.sp_busyness
  , dda.applied_date
from hm_impression_prep a
left join edw.dasher.dimension_dasher_applicants as dda on cast(a.dasher_id as int) = dda.dasher_id
inner join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake as etl on cast(a.dasher_id as int) = etl.dasher_id  
where 1=1
  and etl.account_activation between a.week_start and a.week_end -- filter on dx who activated the same week -- dda.oriented_at_datetime::date
  and a.sent_timestamp >= etl.account_activation 
  -- and etl.first_shift_check_in -- before first shift check in  etl.account_activation
)

, find_first_and_last_impression as (
select
   week_start
   , week_end
   , dasher_id
   , max_by(sent_timestamp, sent_timestamp) last_hm_impression_time
   , max_by(sp_busyness, sent_timestamp) last_hm_impression
   , min_by(sent_timestamp, sent_timestamp) first_hm_impression_time
   , min_by(sp_busyness, sent_timestamp) first_hm_impression
   , count(sp_busyness) as total_impression
   , count(case when sp_busyness in ('busy', 'very_busy') then sp_busyness end) as busy_impression
   , div0(busy_impression, total_impression) dx_busy_impression_rate
   , case when busy_impression > 0 then 'Y' else 'N' end as dx_has_at_least_one_busy_impression
   , case when first_hm_impression in ('busy', 'very_busy') then 'Y' else 'N' end as first_hm_impression_is_busy
   , case when last_hm_impression in ('busy', 'very_busy') then 'Y' else 'N' end as last_hm_impression_is_busy
from activated_new_dx_only
group by 1,2,3
)

, tbl_shift_assignment_delivery as (
select
  w.week_start
  , w.week_end
  , dasher_id
  , sum(num_assigns) as num_assigns
  , sum(num_accepts) as num_accepts
  , sum(num_deliveries) as num_deliveries
  , div0(sum(num_accepts), sum(num_assigns)) acceptance_rate
  , div0(sum(num_deliveries), sum(num_accepts)) delivery_completion_rate
  , case when sum(num_assigns) > 0 then 'Y' else 'N' end as dx_has_assignment
  , case when sum(num_accepts) > 0 then 'Y' else 'N' end as dx_has_accepted_assignments
  , case when sum(num_deliveries) > 0 then 'Y' else 'N' end as dx_completed_deliveries
from week_dates w
join edw.dasher.dasher_shifts s on convert_timezone('UTC', 'America/Los_Angeles', s.check_in_time) between w.week_start and w.week_end
group by 1,2,3
)

, cohort_details as (
select
  a.week_start
  , case 
      when b.applied_date between a.week_start and a.week_end and (b.first_dash_date is null or b.first_dash_date >= a.week_start) then '1. Applied L7D' 
      when b.applied_date between dateadd('day', -30, a.week_start) and dateadd('day', -1, a.week_start) and (b.first_dash_date is null or b.first_dash_date >= a.week_start) then '2. Applied 7D-30D'
      when b.applied_date between dateadd('day', -180, a.week_start) and dateadd('day', -31, a.week_start) and (b.first_dash_date is null or b.first_dash_date >= a.week_start) then '3. Applied 30D-180D'
      when b.applied_date < dateadd('day', -180, a.week_start) and (b.first_dash_date is null or b.first_dash_date >= a.week_start)then '4. Applied 180D+' 
      when b.applied_date > a.week_end and (b.first_dash_date is null or b.first_dash_date >= a.week_start) then 'Applied After Measurement Period' 
      else 'Unmapped' end as cohort
  --==== dimensions ===-----    
  , dda.applied_country_id
  , dda.applied_country_name
  , dda.applied_submarket_id
  , dda.applied_submarket_name
  , case when dda.applied_submarket_id in (9723, 5037, 2543, 7185, 5038) then 'Yes' else 'No' end PR_flag
  , ifnull(d.good_or_bad_bucket, 'good')  good_or_bad_bucket
  , ifnull(d.good_or_bad_bucket_2, 'good') good_or_bad_bucket_2
  , case -- device type only becomes available for those dx apps who pass idv approve step
      when b.device_type like '%iOS%' then '1-iOS'
      when b.device_type like '%Android%' then '2-Android'
      else '3-Other'
    end as device_type
  , wla.all_waitlist_flag
  , case when (pd.phone_number is null -- No duplicate phone number
          or pd.first_applied_date = dda.applied_date) -- OR this is the first application from a phone number
      then 'Non-Dupe' else 'Dupe' end as phone_duplicate_flag
  , case when dda.dx_acquisition_bucket is null then 'Unclassified' else dda.dx_acquisition_bucket end as dx_acquisition_bucket
  --==== conversion steps ===-----
  -- 1/ apply to activation
  , count(distinct b.dasher_applicant_id) as applicants
  -- , count(distinct case when b.profile_submit::date between a.week_start and a.week_end then b.dasher_applicant_id end) as profile_submit
  , count(distinct case when b.vehicle_type_submit::date between a.week_start and a.week_end then b.dasher_applicant_id end) as vehicle_submit
  , count(distinct case when b.idv_submit::date between a.week_start and a.week_end then b.dasher_applicant_id end) as idv_submit 
  , count(distinct case when b.idv_approve::date between a.week_start and a.week_end then b.dasher_applicant_id end) as idv_approve 
  , count(distinct case when b.bgc_submit::date between a.week_start and a.week_end then b.dasher_applicant_id end) as bgc_submit
  , count(distinct case when b.account_activation::date between a.week_start and a.week_end then b.dasher_applicant_id end) as account_activation
  -- 2/ activation to first dash
  -- heatmap views
  , count(distinct case when coalesce(hm.first_hm_impression_time, b.first_shift_creation)::date between a.week_start and a.week_end then hm.dasher_id end) as has_hm_impression
  , count(distinct case when hm.first_hm_impression_is_busy = 'Y' then hm.dasher_id end) as first_hm_impression_is_busy
  , count(distinct case when hm.last_hm_impression_is_busy = 'Y' then hm.dasher_id end) as last_hm_impression_is_busy
  , count(distinct case when hm.dx_has_at_least_one_busy_impression = 'Y' then hm.dasher_id end) as at_least_one_busy_impression
  -- shift creation/checkin
  , count(distinct case when b.first_shift_creation::date between a.week_start and a.week_end then b.dasher_applicant_id end) as first_shift_creation
  , count(distinct case when b.first_shift_check_in::date between a.week_start and a.week_end then b.dasher_applicant_id end) as first_shift_check_in
  , count(distinct case when hm.dx_has_at_least_one_busy_impression = 'Y' and b.first_shift_check_in::date between a.week_start and a.week_end then b.dasher_applicant_id end) as first_shift_check_in_from_busy_impression
  , count(distinct case when hm.dx_has_at_least_one_busy_impression = 'N' and b.first_shift_check_in::date between a.week_start and a.week_end then b.dasher_applicant_id end) as first_shift_check_in_from_non_busy_impression
  -- assignment 
  , count(distinct case when dx_has_assignment = 'Y' then b.dasher_applicant_id end) as has_assignment_creation
  , count(distinct case when dx_has_accepted_assignments = 'Y' then b.dasher_applicant_id end) as has_accepted_assignment
  , count(distinct case when dx_completed_deliveries = 'Y' then b.dasher_applicant_id end) as has_completed_delivery
  , sum(asgn.num_assigns) assignment_creation_cnt
  , sum(asgn.num_accepts) assignment_accepted_cnt
  -- delivery
  , count(distinct case when b.first_dash_date between a.week_start and a.week_end then dda.dasher_id end) as first_delivery
  -- 3/ additional substeps of page render
  , count(distinct case when b.vehicle_type_rendered::date between a.week_start and a.week_end then b.dasher_applicant_id end) as vehicle_type_rendered
  , count(distinct case when b.idv_render::date between a.week_start and a.week_end then b.dasher_applicant_id end) as idv_render
  , count(distinct case when b.bgc_form_rendered::date between a.week_start and a.week_end then b.dasher_applicant_id end) as bgc_form_rendered
from week_dates a
cross join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b 
left join waitlist_apps as wla on wla.dasher_applicant_id = b.dasher_applicant_id
left join edw.dasher.dimension_dasher_applicants dda on b.unique_link = dda.unique_link
left join good_or_bad_dashers d on d.dasher_applicant_id = b.dasher_applicant_id
left join phone_duplicates pd on pd.phone_number = dda.phone_number 
left join find_first_and_last_impression hm on b.dasher_id = hm.dasher_id and a.week_start = hm.week_start
left join tbl_shift_assignment_delivery asgn on b.dasher_id = asgn.dasher_id and a.week_start = asgn.week_start
where cohort = '1. Applied L7D'
group by all
order by 1 desc
)

select
  week_start
  , applied_country_id
  , applied_country_name
  , case 
      when applied_country_id = 1 and PR_flag = 'Yes' then 'US - Puerto Rico' 
      when applied_country_id = 1 and PR_flag = 'No' then 'US - Other Region' 
      else applied_country_name 
     end as country_region
  , cohort
  , all_waitlist_flag
  , device_type
  , good_or_bad_bucket
  , good_or_bad_bucket_2
  , applied_submarket_id
  , applied_submarket_name
  -- , PR_flag
  , phone_duplicate_flag
  -- , dx_acquisition_bucket
  -- , dx_acquisition_channel
  -- , heatmap_busyness
  , sum(applicants) applicants
  -- , sum(profile_submit) profile_submit
  , sum(vehicle_type_rendered) vehicle_type_rendered
  , sum(vehicle_submit) vehicle_submit
  , sum(idv_render) idv_render
  , sum(idv_submit) idv_submit
  , sum(idv_approve) idv_approve
  , sum(bgc_form_rendered) bgc_form_rendered
  , sum(bgc_submit) bgc_submit
  , sum(account_activation) account_activation
  -- 2/ Activation to FD
  , sum(has_hm_impression) has_hm_impression
  , sum(first_hm_impression_is_busy) first_hm_impression_is_busy
  , sum(last_hm_impression_is_busy) last_hm_impression_is_busy
  , sum(at_least_one_busy_impression) at_least_one_busy_impression
  , sum(first_shift_creation) first_shift_creation
  , sum(first_shift_check_in) first_shift_check_in
  , sum(first_shift_check_in_from_busy_impression) first_shift_check_in_from_busy_impression
  , sum(first_shift_check_in_from_non_busy_impression) first_shift_check_in_from_non_busy_impression
  , sum(has_assignment_creation) has_assignment_creation
  , sum(has_accepted_assignment) has_accepted_assignment
  , sum(assignment_creation_cnt) assignment_creation_cnt
  , sum(assignment_accepted_cnt) assignment_accepted_cnt
  , div0(sum(assignment_accepted_cnt), sum(assignment_creation_cnt)) acceptance_rate
  , sum(first_delivery) first_delivery
  , sum(has_completed_delivery) has_completed_delivery
from cohort_details
group by all
order by 1 desc
;


  
-- -- additional timestamps
-- , dash_now_access as (
-- select
--   a.dasher_id
--   , count(1) n_impressions
--   , sum(a.is_dash_now) n_dash_now_impressions
--   , div0(n_dash_now_impressions, n_impressions) dash_now_impression_rate
-- from proddb.public.fact_dasher_access_correctness a
-- inner join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b on a.dasher_id = b.dasher_id
--        and date_trunc('week', a.active_date) = date_trunc('week', b.account_activation)
-- group by all
-- )

-- -- anyone who has an assignment
-- , tbl_assignment_creation_and_acceptance as (
-- select
--   a.dasher_id
--   , count(a.created_at) assignment_creation_cnt -- assignment_creation
--   , avg(case when a.accepted_at is not null and a.unassigned_at is null then 1 else 0 end) as acceptance_rate
-- from proddb.prod_assignment.shift_delivery_assignment a
-- inner join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b on a.dasher_id = b.dasher_id
--        and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.created_at)) = date_trunc('week', b.account_activation) -- in activation week
-- group by all
-- )

grant select on proddb.static.tbl_major_steps_conversion_analysis_applied_L7D_cohort_snowflake to public;