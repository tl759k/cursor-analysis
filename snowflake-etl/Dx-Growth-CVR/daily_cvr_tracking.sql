create or replace table proddb.static.tbl_daily_cvr_tracking as 

with week_dates as (
select distinct
  first_date_of_week_iso as week_start
  , last_date_of_week_iso as week_end
  , calendar_date as reporting_date
--   , day_short_name as reporting_day_short_name
from edw.core.dimension_dates
where calendar_date between '2025-07-01' and current_date
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


-- additional timestamps
, dash_now_access as (
select
  a.dasher_id
  , count(1) n_impressions
  , sum(a.is_dash_now) n_dash_now_impressions
  , div0(n_dash_now_impressions, n_impressions) dash_now_impression_rate
from proddb.public.fact_dasher_access_correctness a
inner join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b on a.dasher_id = b.dasher_id
       and date_trunc('week', a.active_date) = date_trunc('week', b.account_activation)
group by all
)

-- anyone who has an assignment
, tbl_assignment_creation_and_acceptance as (
select
  a.dasher_id
  , count(a.created_at) assignment_creation_cnt -- assignment_creation
  , avg(case when a.accepted_at is not null and a.unassigned_at is null then 1 else 0 end) as acceptance_rate
from proddb.prod_assignment.shift_delivery_assignment a
inner join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b on a.dasher_id = b.dasher_id
       and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.created_at)) = date_trunc('week', b.account_activation) -- in activation week
group by all
)

, cohort_details as (
select
  a.reporting_date
--   , case when date_diff('day', dda.applied_date, a.reporting_date) = 0 then '1. Applied L7D'
--          when date_diff('day', dda.applied_date, a.reporting_date) between 1 and 7 then '2. Applied 7D-30D'
--          when date_diff('day', dda.applied_date, a.reporting_date) between 30 and 180 then '3. Applied 30D-180D'
--          when date_diff('day', dda.applied_date, a.reporting_date) > 180 then '4. Applied 180D+'
--          else 'Unmapped' end as cohort
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
  , case 
     when dda.dx_acquisition_allocation_channel in ('Direct') then 'Organic'
     when dda.dx_acquisition_allocation_channel in ('Referral') then 'Referral'
     else 'Paid Media' end as acquisition_channel
  , count(distinct case when datediff('day', a.reporting_date, dda.applied_date) between 0 and 1 then b.dasher_applicant_id end) as applicants
  --==== conversion steps ===-----
  -- , count(distinct case when account_activation is null or account_activation >= week_start then dasher_applicant_id end) as applicant_count_ex_activate
  , count(distinct case when datediff('day', a.reporting_date, dda.applied_date) between 0 and 1
    and datediff('day', a.reporting_date, b.profile_submit::date) between 0 and 1 then b.dasher_applicant_id end) as profile_submit_1d

  , count(distinct case when datediff('day', a.reporting_date, dda.applied_date) between 0 and 1 --  applied 1d
    and datediff('day', a.reporting_date, coalesce(b.vehicle_type_rendered,b.vehicle_type_submit)::date) between 0 and 1 then b.dasher_applicant_id end) as vehicle_type_rendered_1d

  , count(distinct case when datediff('day', a.reporting_date, coalesce(b.vehicle_type_rendered,b.vehicle_type_submit)::date) between 0 and 1 -- completed vehicle type rendered step
    and datediff('day', a.reporting_date, b.vehicle_type_submit::date) between 0 and 1 then b.dasher_applicant_id end) as vehicle_submit_1d

  , count(distinct case when datediff('day', a.reporting_date, b.vehicle_type_submit::date) between 0 and 1 -- completed vehicle type submit step
    and datediff('day', a.reporting_date, coalesce(b.idv_render, b.idv_submit)::date) = 0 then b.dasher_applicant_id end) as idv_render_1d

  , count(distinct case when datediff('day', a.reporting_date, coalesce(b.idv_render, b.idv_submit)::date) between 0 and 1 -- completed idv render step
    and datediff('day', a.reporting_date, b.idv_submit::date) between 0 and 1 then b.dasher_applicant_id end) as idv_submit_1d

  , count(distinct case when datediff('day', a.reporting_date, b.idv_submit::date) between 0 and 1 -- completed idv submit step
    and datediff('day', a.reporting_date, convert_timezone('UTC', 'America/Los_Angeles', b.idv_approve)::date) between 0 and 1 then b.dasher_applicant_id end) as idv_approve_1d

  , count(distinct case when datediff('day', a.reporting_date, convert_timezone('UTC', 'America/Los_Angeles', b.idv_approve)::date) between 0 and 1 -- completed idv approval step
    and datediff('day', a.reporting_date, coalesce(b.bgc_form_rendered, b.bgc_submit)::date) between 0 and 1 then b.dasher_applicant_id end) as bgc_form_rendered_1d

  , count(distinct case when datediff('day', a.reporting_date, coalesce(b.bgc_form_rendered, b.bgc_submit)::date) between 0 and 1
    and datediff('day', a.reporting_date, coalesce(b.bgc_submit, b.bgc_submit_intl)::date) between 0 and 1 then b.dasher_applicant_id end) as bgc_submit_1d

  , count(distinct case when datediff('day', a.reporting_date, coalesce(b.bgc_submit, b.bgc_submit_intl)::date) between 0 and 1
    and datediff('day', a.reporting_date, b.account_activation::date) between 0 and 1 then b.dasher_applicant_id end) as account_activation_1d

  , count(distinct case when datediff('day', a.reporting_date, b.account_activation::date) between 0 and 1
    and datediff('day', a.reporting_date, b.first_dash_date::date) = 0 then dda.dasher_id end) as first_delivery_1d  
--   , count(distinct case when datediff('day', dda.applied_date, coalesce(b.first_impression_time, b.first_shift_creation)::date) = 0 then b.dasher_applicant_id end) as first_impression_time_1d
--   , count(distinct case when datediff('day', dda.applied_date, coalesce(b.first_shift_creation, b.first_shift_check_in)::date) = 0 then b.dasher_applicant_id end) as first_shift_creation_1d
--   , count(distinct case when datediff('day', dda.applied_date, coalesce(b.first_shift_check_in,b.first_dash_date)::date) = 0 then b.dasher_applicant_id end) as first_shift_check_in_1d
--   , count(distinct case when datediff('day', dda.applied_date, b.account_activation::date) = 0 and assignment_creation_cnt > 0 then b.dasher_applicant_id end) as has_assignment_creation_1d
--   , sum(case when datediff('day', dda.applied_date, b.account_activation::date) = 0 then assignment_creation_cnt end) assignment_creation_cnt_1d
--   , sum(case when datediff('day', dda.applied_date, b.account_activation::date) = 0 then assignment_creation_cnt * acceptance_rate end) assignment_accepted_cnt_1d
--   -- , avg(case when b.account_activation::date between week_start and week_end then acceptance_rate end) acceptance_rate
--   , avg(case when datediff('day', a.applied_date, b.account_activation::date) between 0 and 1 then dash_now_impression_rate end) dash_now_access_rate_1d

from week_dates a
cross join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake b 
left join waitlist_apps as wla on wla.dasher_applicant_id = b.dasher_applicant_id
left join edw.dasher.dimension_dasher_applicants dda on b.unique_link = dda.unique_link
left join good_or_bad_dashers d on d.dasher_applicant_id = b.dasher_applicant_id
left join phone_duplicates pd on pd.phone_number = dda.phone_number 
left join dash_now_access e on b.dasher_id = e.dasher_id
left join tbl_assignment_creation_and_acceptance f on b.dasher_id = f.dasher_id
where dda.applied_date between '2025-06-01' and current_date
-- where cohort = '1. Applied L7D'
group by all
order by 1 desc
)

select
  reporting_date
  , applied_country_id
  , applied_country_name
  , case 
      when applied_country_id = 1 and PR_flag = 'Yes' then 'US - Puerto Rico' 
      when applied_country_id = 1 and PR_flag = 'No' then 'US - Other Region' 
      else applied_country_name 
     end as country_region
  , all_waitlist_flag
  , device_type
  , good_or_bad_bucket
  , good_or_bad_bucket_2
  , applied_submarket_id
  , applied_submarket_name
  -- , PR_flag
  , phone_duplicate_flag
  , acquisition_channel
  -- , heatmap_busyness
  , sum(applicants) applicants
  , sum(profile_submit_1d) profile_submit_1d
  , sum(vehicle_type_rendered_1d) vehicle_type_rendered_1d
  , sum(vehicle_submit_1d) vehicle_submit_1d
  , sum(idv_render_1d) idv_render_1d
  , sum(idv_submit_1d) idv_submit_1d
  , sum(idv_approve_1d) idv_approve_1d
  , sum(bgc_form_rendered_1d) bgc_form_rendered_1d
  , sum(bgc_submit_1d) bgc_submit_1d
  , sum(account_activation_1d) account_activation_1d
--   , sum(first_impression_time_1d) first_impression_time_1d
--   , sum(first_shift_creation_1d) first_shift_creation_1d
--   , sum(first_shift_check_in_1d) first_shift_check_in_1d
--   , sum(has_assignment_creation_1d) has_assignment_creation_1d
--   , sum(assignment_creation_cnt_1d) assignment_creation_cnt_1d
--   , sum(assignment_accepted_cnt_1d) assignment_accepted_cnt_1d
--   , div0(sum(assignment_accepted_cnt), sum(assignment_creation_cnt)) acceptance_rate
--   , avg(dash_now_access_rate_1d) dash_now_access_rate_1d
  , sum(first_delivery_1d) first_delivery_1d
from cohort_details
group by all
order by 1 desc
;

grant select on proddb.static.tbl_daily_cvr_tracking  to public;