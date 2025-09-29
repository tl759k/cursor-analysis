with daily_cvr_tracking as (
select
  -- convert_timezone('UTC', 'America/Los_Angeles', curre) last_updated_at_pst
  reporting_date

  -- , case 
  --     when device_type like '%iOS%' then '1-iOS'
  --     when device_type like '%Android%' then '2-Android'
  --     else '3-Other'
  --     end as device_type
  -- , concat(applied_country_id, '-', applied_country_name) as applied_country_id
  -- , case when applied_country_id = 1 then 'US' else 'Internatinal' end as global_region
  -- , country_region
  , all_waitlist_flag
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
  -- , sum(first_impression_time_1d) first_heatmap_impression_1d
  -- , sum(first_shift_creation_1d) first_shift_creation_1d
  -- , sum(first_shift_check_in_1d) first_shift_check_in_1d
  -- , sum(has_assignment_creation_1d) has_assignment_creation_1d
  , sum(first_delivery_1d) first_delivery_1d    
  -- , sum(assignment_creation_cnt_1d) assignment_creation_cnt_1d
  -- , sum(assignment_accepted_cnt_1d) assignment_accepted_cnt_1d
--   , div0(sum(assignment_accepted_cnt_1d), sum(assignment_creation_cnt_1d)) acceptance_rate_1d
--   , avg(dash_now_access_rate_1d) dash_now_access_rate_1d
from proddb.static.tbl_daily_cvr_tracking
where 1=1
  and all_waitlist_flag = 'No Waitlist' and reporting_date >= '2025-06-01'
group by all
)

select
    *
    , div0(account_activation_1d, applicants) cvr_1_1_apply_to_activation_1d
    , div0(first_delivery_1d, applicants) cvr_1_2_apply_to_fd_1d

    , div0(vehicle_type_rendered_1d, applicants) cvr_2_1_apply_to_vs_rendered_1d
    , div0(vehicle_submit_1d, vehicle_type_rendered_1d) cvr_2_2_vt_render_to_vt_submit_1d
    , div0(idv_render_1d, vehicle_submit_1d) cvr_2_3_vt_submit_to_idv_render_1d
    , div0(idv_submit_1d, idv_render_1d) cvr_2_4_idv_render_to_idv_submit_1d
    , div0(idv_approve_1d, idv_submit_1d) cvr_2_5_idv_submit_to_idv_approve_1d
    , div0(bgc_form_rendered_1d, idv_approve_1d) cvr_2_6_idv_approve_to_bgc_form_rendered_1d
    , div0(bgc_submit_1d, bgc_form_rendered_1d) cvr_2_7_bgc_form_rendered_to_bgc_submit_1d
    -- , div0(first_shift_creation_1d, applicants) cvr_2_8_first_shift_creation_to_first_shift_creation_1d
    -- , div0(first_shift_check_in_1d, applicants) cvr_2_9_first_shift_check_in_to_first_shift_check_in_1d
    -- , div0(has_assignment_creation_1d, applicants) cvr_2_10_has_assignment_creation_to_has_assignment_creation_1d
from daily_cvr_tracking
;