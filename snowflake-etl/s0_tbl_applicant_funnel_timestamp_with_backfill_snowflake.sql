create or replace table proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake as

with persona_status as ( 
select 
 reference_id as unique_link
 , min_by(status,updated_at) as min_status
 , min(updated_at) as min_updated_at
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

, tbl_vehicle_type_rendered as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as vehicle_type_rendered 
from 
  segment_events_raw.driver_production.workflow_step_rendered 
where 1=1
  and page_id = 'VEHICLE_DETAILS_PAGE'
group by 1
)

, tbl_vehicle_type_submit as (
select
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as vehicle_type_submit 
from segment_events_raw.driver_production.workflow_step_submit_success 
where 1=1
  and page_id = 'VEHICLE_DETAILS_PAGE'
group by 1 
)

, tbl_device_type_from_vt as (-- find device type from vehicle submit step
select distinct
  unique_link 
  , max_by(context_os_name, timestamp) context_os_name_last
  , min_by(context_os_name, timestamp) context_os_name_first
  , max_by(context_app_version, timestamp) context_app_version_last
  , min_by(context_app_version, timestamp) context_app_version_first  
from 
  segment_events_raw.driver_production.workflow_step_submit_success 
where 1=1
  and page_id = 'VEHICLE_DETAILS_PAGE'
  and context_os_name is not null
group by all
)

, tbl_idv_render as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as idv_render 
from 
  segment_events_raw.driver_production.workflow_step_rendered 
where 1=1
  and page_id = 'IDENTITY_VERIFICATION_LANDING_PAGE'
group by 1
)

, tbl_idv_submit as (
select 
  coalesce(unique_link, reference_id, user_id) as unique_link -- unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as idv_submit 
from 
  segment_events_raw.driver_production.DA_track_idv_steps 
where 1=1
  and name = 'complete'
group by 1
)

, tbl_bgc_form_rendered as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as bgc_form_rendered 
from 
  segment_events_raw.driver_production.workflow_step_rendered 
where 1=1
  and page_id = 'BACKGROUND_CHECK'
group by 1
)

, tbl_bgc_submit as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as bgc_submit 
from 
  segment_events_raw.driver_production.workflow_step_submit_success 
where 1=1
  and page_id = 'BACKGROUND_CHECK'
group by 1
)

, tbl_bgc_submit_intl as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as bgc_submit_intl 
from 
  segment_events_raw.driver_production.workflow_step_submit_success 
where 1=1
  and page_id = 'BACKGROUND_CHECK_STATUS'
group by 1
)

, tbl_account_activation as (
select 
  unique_link 
  , convert_timezone('UTC', 'America/Los_Angeles', min(timestamp)) as account_activation 
from 
  segment_events_raw.driver_production.dasher_activated_time 
group by 1
)

-- additional steps
, tbl_start_first_shift as (
select 
  dasher_id
  , min(convert_timezone('UTC', 'America/Los_Angeles', created_at)) first_shift_creation
  , min(convert_timezone('UTC', 'America/Los_Angeles', check_in_time)) first_shift_check_in
from edw.dasher.dasher_shifts 
where 1=1
group by all
)

, all_steps_timestamps as (
select
  dda.dasher_applicant_id
  , dda.dasher_id
  , dda.unique_link
  , dda.applied_date -- generated after phone and email, dasher_applicant_id and user_id are created, before profile submit
  , vtr.vehicle_type_rendered as vehicle_type_rendered_raw
  , vt.vehicle_type_submit as vehicle_type_submit_raw
  , idvr.idv_render as idv_render_raw
  , idvsu.idv_submit as idv_submit_raw
  , pers.max_updated_at as idv_approve_raw
  , bgcr.bgc_form_rendered as bgc_form_rendered_raw
  , coalesce(bgcs.bgc_submit, bgcs_intl.bgc_submit_intl) as bgc_submit_raw
  , aa.account_activation as account_activation_raw
  , fs.first_shift_creation as first_shift_creation_raw
  , fs.first_shift_check_in as first_shift_check_in_raw
  , dda.first_dash_date
  , case 
      when vt_dt.context_os_name_first is null and vt_dt.context_os_name_last is null then null
      when vt_dt.context_os_name_first = 'iOS' or vt_dt.context_os_name_last = 'iOS' then 'iOS'
      when vt_dt.context_os_name_first = 'Android' and vt_dt.context_os_name_last = 'Android' then 'Android'
      else concat(vt_dt.context_os_name_first, '-' , vt_dt.context_os_name_last)
     end as device_type  
  , case 
      when vt_dt.context_app_version_first = vt_dt.context_app_version_last then vt_dt.context_app_version_last
      else concat(vt_dt.context_app_version_first, '-' , vt_dt.context_app_version_last)
     end as app_version
from edw.dasher.dimension_dasher_applicants dda 
left join tbl_vehicle_type_rendered vtr on vtr.unique_link = dda.unique_link
left join tbl_vehicle_type_submit vt on vt.unique_link = dda.unique_link
left join tbl_device_type_from_vt vt_dt on vt_dt.unique_link = dda.unique_link
left join tbl_idv_render idvr on idvr.unique_link = dda.unique_link
left join tbl_idv_submit idvsu on idvsu.unique_link = dda.unique_link
left join persona_status pers on pers.unique_link = dda.unique_link and pers.max_status = 'approved'
left join tbl_bgc_form_rendered bgcr on bgcr.unique_link = dda.unique_link
left join tbl_bgc_submit bgcs on bgcs.unique_link = dda.unique_link
left join tbl_bgc_submit_intl bgcs_intl on bgcs_intl.unique_link = dda.unique_link
left join tbl_account_activation aa on aa.unique_link = dda.unique_link
left join tbl_start_first_shift fs on fs.dasher_id = dda.dasher_id
where 1=1 
group by all
)


select 
dasher_applicant_id
  , dasher_id
  , unique_link
  , device_type  
  , app_version
  , applied_date -- generated after phone and email, dasher_applicant_id and user_id are created, before profile submit
  -- backfill dates in backward orders: using simpliefied method
  , first_dash_date
  , coalesce(first_shift_check_in_raw, first_dash_date) as first_shift_check_in
  , coalesce(first_shift_creation_raw, first_shift_check_in) as first_shift_creation
  , coalesce(account_activation_raw, first_shift_creation) as account_activation
  , coalesce(bgc_submit_raw, account_activation) as bgc_submit
  , coalesce(bgc_form_rendered_raw, bgc_submit) as bgc_form_rendered
  , coalesce(idv_approve_raw, bgc_form_rendered) as idv_approve  
  , coalesce(idv_submit_raw, idv_approve) as idv_submit 
  , coalesce(idv_render_raw, idv_submit) as idv_render 
  , coalesce(vehicle_type_submit_raw, idv_render) as vehicle_type_submit 
  , coalesce(vehicle_type_rendered_raw, vehicle_type_submit) as vehicle_type_rendered 
from all_steps_timestamps
;

grant select on proddb.static.tbl_applicant_funnel_timestamp_with_backfill_snowflake to public
;
