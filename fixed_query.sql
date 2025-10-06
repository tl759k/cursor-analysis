set start_week = dateadd('week', -2, date_trunc('week', current_date));
set end_week = dateadd('week', -1, date_trunc('week', current_date));


-- differences is that this uses the timestamps table which has backfilled data: 
-- select * from proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake  limit 10

create or replace table static.tbl_new_dx_cvr_wow_reporting_all_waitlist_flags_v2 as

with week_dates as (
select distinct
  first_date_of_week_iso as week_start
  , last_date_of_week_iso as week_end
from edw.core.dimension_dates
where first_date_of_week_iso between $start_week and $end_week
order by week_start desc
)

-- conversion funnel
, persona_status as ( 
select 
reference_id as unique_link
, min_by(status,updated_at) as min_status
, convert_timezone('UTC', 'America/Los_Angeles', min(updated_at)) as min_updated_at
, min(case when status = 'approved' then updated_at end) as first_approved_at
, min(case when status = 'declined' then updated_at end) as first_declined_at
, max_by(status,updated_at) as max_status
, convert_timezone('UTC', 'America/Los_Angeles', max(updated_at)) as max_updated_at
from RISK_DATA_PLATFORM_PROD.PUBLIC.PERSONA_INQUIRY 
where 1=1
    and template_id in ('tmpl_kfaFkGugqG9jqh6aAuc21Vwd'
                        , 'tmpl_dos19dD2bQ9wjrnVjAWRo1Ff'
                        , 'itmpl_XSPKhqhr8JrQkR9xGffNqbiD3DzN'
                        , 'itmpl_Ryrwhboy242TPqhtDFKuYKZskkuz'
                        , 'itmpl_U8gkVX5Z5YSULpkiZhwzuBABY1w2' -- New template from IDV Native Launch Apr. 2025
                        , 'itmpl_gTxrLPpupfjHj8K9MdYFYwbrZHV2') -- New template from IDV Native Launch Apr. 2025
group by 1 
)


, tbl_vehicle_type_submit as (
select
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as vehicle_type_submit 
from segment_events_raw.driver_production.workflow_step_submit_success 
where 1=1
  and page_id = 'VEHICLE_DETAILS_PAGE'
group by all
)

, tbl_account_activation as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as account_activation 
from segment_events_raw.driver_production.dasher_activated_time 
where 1=1
group by all
)


--- all waitlist applicants
, old_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'TOF_WAITLIST_PAGE'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

, old_bof as ( -- Launched June 2024
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'BACKGROUND_CHECK'
  and coalesce(is_in_bgc_waitlist_blocker_experiment, false) = true
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)
  --- NEW WAITLIST ---  
, hard_block_render_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_HARD_BLOCK_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_AFTER_VEHICLE'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

, hard_block_render_bof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_HARD_BLOCK_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_BEFORE_BGC'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

-- Reserved  
, reserved_render_tof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_LIMITED_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_AFTER_VEHICLE'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

, reserved_render_bof as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_rendered
where 1=1
  and page_id = 'WAITLIST_LIMITED_PAGE'
  and waitlist_step = 'DASHER_WAITLIST_STEP_BEFORE_BGC'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

, reserved_continue_signup as (
select distinct unique_link
from segment_events_raw.driver_production.workflow_step_submit_success
where 1=1
  and page_id = 'WAITLIST_LIMITED_PAGE'
  -- and ORIGINAL_TIMESTAMP >= CURRENT_DATE - INTERVAL '2 year'
)

, reserved_heatmap_load as (
select distinct dasher_id
from iguazu.server_events_production.limited_heatmap_loaded_event
where 1=1
  -- and IGUAZU_SENT_AT >= CURRENT_DATE - INTERVAL '2 year'
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
  , case
    when old_tof.unique_link is not null then 'ToF Waitlist'
    else ''
  end as legacy_tof_waitlist,
  case
    when old_bof.unique_link is not null then 'BoF Waitlist'
    else ''
  end as legacy_bof_waitlist,
  case
    when old_tof.unique_link is not null
    or old_bof.unique_link is not null then 'Legacy Waitlist'
    else ''
  end as legacy_waitlist,
  case
    when hbtof.unique_link is not null
    or hbbof.unique_link is not null then 'Hard Block'
    else ''
  end as hard_block,
  case
    when rtof.unique_link is not null
    or rbof.unique_link is not null
    then 'Reserved'
    else ''
  end as reserved,
  case
    when hbtof.unique_link is not null then 'Hard Block ToF'
    else ''
  end as hard_block_tof,
  case
    when hbbof.unique_link is not null then 'Hard Block BoF'
    else ''
  end as hard_block_bof,
  case
    when rtof.unique_link is not null then 'Reserved ToF'
    else ''
  end as reserved_tof,
  case
    when rbof.unique_link is not null then 'Reserved BoF'
    else ''
  end as reserved_bof,
  -- case
  --   when rc.unique_link is not null then 'Reserved Click Continue'
  --   else ''
  -- end as reserved_continue,
  -- case
  --   when rhl.dasher_id is not null then 'Reserved Heatmap Load'
  --   else ''
  -- end as reserved_heatmap_load,
  case
    when hbtof.unique_link is not null
    or hbbof.unique_link is not null
    or rtof.unique_link is not null
    or rbof.unique_link is not null then 'New Waitlist'
    else ''
  end as new_waitlist
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
left join reserved_continue_signup rc on rc.unique_link = dda.unique_link
left join reserved_heatmap_load rhl on rhl.dasher_id = dda.dasher_id
where 1=1 
  -- and applied_date >= CURRENT_DATE() - INTERVAL '2 year'
  -- and date_trunc ('week', applied_date) < date_trunc ('week', current_date())
)

, waitlist_apps_agg as (
SELECT
  CASE
    WHEN HARD_BLOCK = 'Hard Block'
    AND Reserved = '' THEN 'Hard Block'
    WHEN HARD_BLOCK = ''
    AND Reserved = 'Reserved' THEN 'Reserved'
    WHEN HARD_BLOCK = 'Hard Block'
    AND Reserved = 'Reserved' THEN 'Hard Block & Reserved'
    ELSE 'No Waitlist'
  END Waitlist_flag,
      CASE
        WHEN HARD_BLOCK = 'Hard Block'
        AND Reserved = '' THEN 2
        WHEN HARD_BLOCK = ''
        AND Reserved = 'Reserved' THEN 1
        WHEN HARD_BLOCK = 'Hard Block'
        AND Reserved = 'Reserved' THEN 3
        ELSE 4
      END FlagOrder,
dasher_applicant_id
from waitlist_apps
)

-------------------

, overall_results as (
select 
  a.week_start
  , a.week_end
  , case 
      when dda.applied_date between a.week_start and a.week_end and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start) then '1. Applied L7D' 
      when dda.applied_date between dateadd('day', -30, a.week_start) and dateadd('day', -1, a.week_start) and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start) then '2. Applied 7D-30D'
      when dda.applied_date between dateadd('day', -180, a.week_start) and dateadd('day', -31, a.week_start) and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start) then '3. Applied 30D-180D'
      when dda.applied_date between dateadd('day', -540, a.week_start) and dateadd('day', -181, a.week_start) and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start)  then '4. Applied 180D-540D' 
      when dda.applied_date < dateadd('day', -540, a.week_start) and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start) then '5. Applied 540D+' 
      when dda.applied_date > a.week_end and (dda.first_dash_date is null or dda.first_dash_date >= a.week_start) then 'Applied After Measurement Period' 
      else 'Unmapped' end as cohort
  -- , dda.is_waitlist -- Block WL only
  , wla.Waitlist_flag as is_waitlist
  , wla.FlagOrder
  , count(distinct dda.dasher_applicant_id) as applicant_count 
  , count(distinct case when dda.account_activation is null or dda.account_activation >= a.week_start then dda.dasher_applicant_id end) as applicant_count_ex_activate
  , count(distinct case when dda.vehicle_type_submit between a.week_start and a.week_end then dda.dasher_applicant_id end) as vehicle_submit
  , count(distinct case when dda.idv_approve between a.week_start and a.week_end then dda.dasher_applicant_id end) as idv_approve 
  , count(distinct case when dda.account_activation::date between a.week_start and a.week_end then dda.dasher_applicant_id end) as account_activation
  , count(distinct case when dda.first_dash_date::date between a.week_start and a.week_end then dda.dasher_applicant_id end) as first_delivery
from week_dates a
cross join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake dda
left join waitlist_apps_agg wla on wla.dasher_applicant_id = dda.dasher_applicant_id
group by all
)

, main_table as (
select 
  * 
  -- , div0(account_activation, applicant_count_ex_activate) apply_to_aa_cvr
  , div0(account_activation, sum(account_activation) over (partition by week_start, is_waitlist)) as aa_proportion 
from overall_results
where 1=1
  -- and week_start = '2025-06-09'
  and cohort not in ('1. Activated L7D' ,'Applied After Measurement Period' , 'Unmapped')
order by week_start, is_waitlist, cohort asc
)

-- extra account activation that not mapped to a cohort needs to be re-distributed back to each cohort
, extra_aa as (
select 
  week_start
  , is_waitlist
  , sum(account_activation) account_activation_extra
from overall_results
where 1=1
  and cohort in ('Applied After Measurement Period' , 'Unmapped')
group by all
)


, redistr_extra_aa as (
select
  a.*
  , b.account_activation_extra
  , b.account_activation_extra * a.aa_proportion as redistr_aa
  , b.account_activation_extra * a.aa_proportion + a.account_activation as final_account_activation
  , div0(final_account_activation, applicant_count_ex_activate) apply_to_aa_cvr
from main_table a
left join extra_aa b on a.week_start = b.week_start and a.is_waitlist = b.is_waitlist
order by a.week_start, a.is_waitlist, a.cohort asc
)


, activation_to_fd as (
select 
  a.week_start
  , a.week_end
  -- , dda.is_waitlist
  -- , dda.applied_submarket_id
  -- , dda.applied_submarket_name
  -- , dda.applied_country_id
  -- , dda.applied_country_name
  , wla.Waitlist_flag as is_waitlist
  , wla.FlagOrder
  , case 
      when dda.account_activation::date between a.week_start and a.week_end then '1. Activated L7D' 
      when dda.account_activation::date between dateadd('day', -30, a.week_start) and dateadd('day', -1, a.week_start)  then '2. Activated 7D-30D'
      when dda.account_activation::date between dateadd('day', -180, a.week_start) and dateadd('day', -31, a.week_start) then '3. Activated 30D-180D'
      when dda.account_activation::date between dateadd('day', -540, a.week_start) and dateadd('day', -181, a.week_start) then '4. Activated 180D-540D' 
      when dda.account_activation::date < dateadd('day', -540, a.week_start) then '5. Activated 540D+' 
      when dda.account_activation::date > a.week_end then 'Activated After Measurement Period' 
      when dda.account_activation is null then 'null activation date'
      else 'Unmapped' end as cohort 
  , count(distinct dda.dasher_applicant_id) as activated_dx
  , count(distinct case when dda.first_dash_date between a.week_start and a.week_end then dda.dasher_applicant_id end) as first_delivery
from week_dates a
cross join proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake dda
left join waitlist_apps_agg wla on wla.dasher_applicant_id = dda.dasher_applicant_id
where 1=1 
  -- and cohort not in ('Activated After Measurement Period' , 'Unmapped', 'null activation date')
group by all
order by week_start, is_waitlist, cohort asc
)

, activation_to_fd_main_table as (
select
  * 
  , div0(first_delivery, sum(first_delivery) over (partition by week_start, is_waitlist)) as fd_proportion 
from activation_to_fd 
where 1=1
  and cohort not in ('Activated After Measurement Period' , 'Unmapped', 'null activation date')
)

, extra_fd as (
select
  week_start
  , is_waitlist
  , sum(first_delivery) first_delivery_extra
from activation_to_fd 
where 1=1
  and cohort in ('Activated After Measurement Period' , 'Unmapped', 'null activation date')
group by all
)

, redistr_extra_fd as (
select
  a.*
  , b.first_delivery_extra
  , b.first_delivery_extra * a.fd_proportion as redistr_fd
  , b.first_delivery_extra * a.fd_proportion + a.first_delivery as final_first_delivery
  , div0(final_first_delivery, activated_dx) as activation_to_fd_cvr
from activation_to_fd_main_table a
left join extra_fd b on a.week_start = b.week_start and a.is_waitlist = b.is_waitlist
order by a.week_start, a.is_waitlist, a.cohort asc
)



select
  a.week_start
  , a.cohort
  , a.is_waitlist
  , a.FlagOrder
  , a.applicant_count
  , a.applicant_count_ex_activate
  , a.vehicle_submit
  , a.idv_approve
  , a.final_account_activation
  , a.first_delivery
  , a.apply_to_aa_cvr
  , b.activated_dx
  , b.final_first_delivery
  , b.activation_to_fd_cvr
from redistr_extra_aa a
left join redistr_extra_fd b on a.week_start = b.week_start and left(a.cohort,2) = left(b.cohort,2) and a.is_waitlist = b.is_waitlist
;


-- run summary
with waitlist_cat as (
select
  week_start
  , case when flagorder  in (1,2,3) then 'True' else 'False' end is_waitlist
  , cohort as cohort_cat
  -- , case when left(cohort,1) = 1 then '1. L7D cohort' else '2. Older cohort' end as cohort_cat
  , sum(applicant_count) applicant_count
  , sum(applicant_count_ex_activate) applicant_count_ex_activate
  , sum(vehicle_submit) vehicle_submit
  , sum(idv_approve) idv_approve
  , sum(final_account_activation) account_activation
  , sum(first_delivery) first_delivery
  , div0(sum(final_account_activation), sum(applicant_count_ex_activate)) apply_to_aa_cvr
  , sum(activated_dx) activated_dx
  , sum(final_first_delivery) final_first_delivery
  , div0(sum(final_first_delivery), sum(activated_dx)) aa_to_fd_cvr
-- from static.tbl_new_dx_cvr_wow_reporting
from static.tbl_new_dx_cvr_wow_reporting_all_waitlist_flags_v2
where 1=1
  -- and cohort_cat = '1. Applied L7D'
group by all
)

, all_wl as (
select
  week_start
  , 'All' is_waitlist
  -- , '999' flagorder
  , cohort as cohort_cat
  -- , case when left(cohort,1) = 1 then '1. L7D cohort' else '2. Older cohort' end as cohort_cat
  , sum(applicant_count) applicant_count
  , sum(applicant_count_ex_activate) applicant_count_ex_activate
  , sum(vehicle_submit) vehicle_submit
  , sum(idv_approve) idv_approve
  , sum(final_account_activation) account_activation
  , sum(first_delivery) first_delivery
  , div0(sum(final_account_activation), sum(applicant_count_ex_activate)) apply_to_aa_cvr
  , sum(activated_dx) activated_dx
  , sum(final_first_delivery) final_first_delivery
  , div0(sum(final_first_delivery), sum(activated_dx)) aa_to_fd_cvr
-- from static.tbl_new_dx_cvr_wow_reporting
from static.tbl_new_dx_cvr_wow_reporting_all_waitlist_flags_v2
where 1=1
group by all
)

, combined as (
select * from waitlist_cat
union all 
select * from all_wl
)

select * from combined
order by week_start, is_waitlist, cohort_cat asc