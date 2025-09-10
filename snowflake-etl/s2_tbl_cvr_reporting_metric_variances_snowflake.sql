create or replace table proddb.static.tbl_cvr_reporting_metric_variances_snowflake as

-- Global CVR
with base as (
  select
    cohort,
    week_start,
    sum(applicants) applicants,
    sum(profile_submit) profile_submit,
    sum(vehicle_submit) vehicle_submit,
    sum(idv_submit) idv_submit,
    sum(idv_approve) idv_approve,
    sum(bgc_submit) bgc_submit,
    sum(account_activation) account_activation,
    sum(first_delivery) first_delivery,

    -- Tiered CVRs
    div0(sum(first_delivery), sum(applicants)) as cvr_0_apply_to_fd,
    div0(sum(account_activation), sum(applicants)) as cvr_1_1_apply_to_activation,
    div0(sum(first_delivery), sum(account_activation)) as cvr_1_2_activation_to_fd,

    div0(sum(vehicle_submit), sum(applicants)) as cvr_2_1_apply_to_vs,
    div0(sum(idv_submit), sum(vehicle_submit)) as cvr_2_2_vs_to_idv_submit,
    div0(sum(idv_approve), sum(idv_submit)) as cvr_2_3_idv_submit_to_approve,
    div0(sum(bgc_submit), sum(idv_approve)) as cvr_2_4_idv_approve_to_bgc_submit,
    div0(sum(account_activation), sum(bgc_submit)) as cvr_2_5_bgc_submit_to_aa
  -- from static.tbl_major_steps_conversion_analysis_applied_L7D_cohort
  from proddb.static.tbl_major_steps_conversion_analysis_applied_L7D_cohort_snowflake
  where
    all_waitlist_flag = 'No Waitlist' and week_start < date_trunc('week', current_date)
    -- and device_type = 'iOS'
  group by all
)

, metric_base_long_format as (
  select cohort, week_start, 'C1' as metric_number, 'applicants' as metric_name, applicants as metric_value from base union all
  select cohort, week_start, 'C2', 'profile_submit', profile_submit from base union all
  select cohort, week_start, 'C3', 'vehicle_submit', vehicle_submit from base union all
  select cohort, week_start, 'C4', 'idv_submit', idv_submit from base union all
  select cohort, week_start, 'C5', 'idv_approve', idv_approve from base union all
  select cohort, week_start, 'C6', 'bgc_submit', bgc_submit from base union all
  select cohort, week_start, 'C7', 'account_activation', account_activation from base union all
  select cohort, week_start, 'C8', 'first_delivery', first_delivery from base union all
  select cohort, week_start, 'R0', 'cvr_0_apply_to_fd', cvr_0_apply_to_fd from base union all
  select cohort, week_start, 'R1.1', 'cvr_1_1_apply_to_activation', cvr_1_1_apply_to_activation from base union all
  select cohort, week_start, 'R1.2', 'cvr_1_2_activation_to_fd', cvr_1_2_activation_to_fd from base union all
  select cohort, week_start, 'R2.1', 'cvr_2_1_apply_to_vs', cvr_2_1_apply_to_vs from base union all
  select cohort, week_start, 'R2.2', 'cvr_2_2_vs_to_idv_submit', cvr_2_2_vs_to_idv_submit from base union all
  select cohort, week_start, 'R2.3', 'cvr_2_3_idv_submit_to_approve', cvr_2_3_idv_submit_to_approve from base union all
  select cohort, week_start, 'R2.4', 'cvr_2_4_idv_approve_to_bgc_submit', cvr_2_4_idv_approve_to_bgc_submit from base union all
  select cohort, week_start, 'R2.5', 'cvr_2_5_bgc_submit_to_aa', cvr_2_5_bgc_submit_to_aa from base
)


, monthly_avg as (
select
  cohort
  , date_trunc('month', week_start) week_month
  , metric_number
  , metric_name
  , avg(metric_value) as metric_value_monthly_avg
from metric_base_long_format
group by all
)


, lagged as (
  select
    *,
    lag(metric_value, 1) over (partition by cohort, metric_number order by week_start) as metric_value_pw,
    lag(metric_value, 2) over (partition by cohort, metric_number order by week_start) as metric_value_p2w,
    lag(metric_value, 3) over (partition by cohort, metric_number order by week_start) as metric_value_p3w,
    lag(metric_value, 4) over (partition by cohort, metric_number order by week_start) as metric_value_p4w,
    -- lag(metric_value, 5) over (partition by cohort, metric_number order by week_start) as metric_value_p5w,
    -- lag(metric_value, 6) over (partition by cohort, metric_number order by week_start) as metric_value_p6w,
    -- lag(metric_value, 7) over (partition by cohort, metric_number order by week_start) as metric_value_p7w,
    -- lag(metric_value, 8) over (partition by cohort, metric_number order by week_start) as metric_value_p8w
  from metric_base_long_format
)


, lagged_join_month as (
select
  a.*
  , b.metric_value_monthly_avg as metric_value_last_month_avg
from lagged a
left join monthly_avg b on dateadd('month', -1, date_trunc('month', a.week_start)) = b.week_month and a.metric_number = b.metric_number and a.metric_name = b.metric_name and a.cohort = b.cohort
)

, enriched as (
select
  *
  -- w/w comparison
  , metric_value - metric_value_pw as metric_wow_diff
  , div0(metric_value, nullif(metric_value_pw, 0)) - 1 as metric_wow_diff_pct
  , case when metric_wow_diff < 0 then 1 else 0 end as w1_decline_flag
  -- w over last month avg comparison
  , metric_value - metric_value_last_month_avg as metric_lm_avg_diff
  , div0(metric_value, nullif(metric_value_last_month_avg, 0)) - 1 as metric_lm_avg_diff_pct
  , case when metric_lm_avg_diff_pct < 0 then 1 else 0 end as lm_avg_decline_flag
  -- , metric_value - metric_avg_4w_ago as metric_w_4w_ago_baseline_diff
  -- , (metric_value_p5w + metric_value_p6w + metric_value_p7w + metric_value_p8w) / 4 as metric_avg_4w_ago
  -- , div0(metric_value, nullif(metric_value_p2w, 0)) - 1 as metric_w2w_diff_pct
  -- , div0(metric_value, nullif(metric_value_p3w, 0)) - 1 as metric_w3w_diff_pct
  -- , div0(metric_value, nullif(metric_value_p4w, 0)) - 1 as metric_w4w_diff_pct
  -- , div0(metric_value, nullif(metric_avg_4w_ago, 0)) - 1 as metric_w_4w_agao_baseline_diff_pct
from lagged_join_month
)

, streak_flagged as (
  select
    *,
    sum(case when w1_decline_flag = 0 then 1 else 0 end) 
      over (partition by cohort, metric_number order by week_start) as streak_group
  from enriched
)

, decline_streak_counted as (
  select
    *,
    case when w1_decline_flag = 1 then
      row_number() over (partition by cohort, metric_number, streak_group order by week_start) - 1
    else 0 end as consecutive_decline_weeks
  from streak_flagged
)


, with_index as (
  select
    *,
    row_number() over (partition by cohort, metric_number order by week_start) as week_index
  from decline_streak_counted
)


, joined_with_start_of_streak as (
select
  curr.*,
  prev.metric_value as metric_value_at_streak_start
from with_index curr
left join with_index prev
  on curr.cohort = prev.cohort
  and curr.metric_number = prev.metric_number
  and curr.week_index = prev.week_index + curr.consecutive_decline_weeks
)


, accumulated_decline as (
select
  week_start
  , metric_number
  , metric_name
  , metric_value
  , metric_value_pw
  , metric_wow_diff
  , metric_wow_diff_pct
  , w1_decline_flag
  , metric_value_last_month_avg
  , metric_lm_avg_diff
  , metric_lm_avg_diff_pct
  , lm_avg_decline_flag
  , streak_group
  , consecutive_decline_weeks
  , week_index
  , metric_value_at_streak_start
  , case
    when metric_value_at_streak_start is not null and metric_value_at_streak_start != 0 then
      metric_value - metric_value_at_streak_start
    else null
  end as accumulated_decline
  , case
    when metric_value_at_streak_start is not null and metric_value_at_streak_start != 0 then
      div0(metric_value, metric_value_at_streak_start)- 1
    else null
  end as accumulated_decline_pct
from joined_with_start_of_streak
-- where metric_name = 'cvr_0_apply_to_fd'
order by metric_number, week_start
)


, new_dx_impact as (
select
  a.*
  , case 
      when metric_name = 'cvr_0_apply_to_fd' then metric_wow_diff * b.applicants 
      when metric_name = 'cvr_1_1_apply_to_activation' then metric_wow_diff * b.applicants * b.cvr_1_2_activation_to_fd
      when metric_name = 'cvr_1_2_activation_to_fd' then metric_wow_diff * b.account_activation 
      when metric_name = 'cvr_2_1_apply_to_vs' then metric_wow_diff * b.applicants * (b.first_delivery / b.vehicle_submit)
      when metric_name = 'cvr_2_2_vs_to_idv_submit' then metric_wow_diff * b.vehicle_submit * (b.first_delivery / b.idv_submit) 
      when metric_name = 'cvr_2_3_idv_submit_to_approve' then metric_wow_diff * b.idv_submit * (b.first_delivery / b.idv_approve)
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' then metric_wow_diff * b.idv_approve * (b.first_delivery / b.bgc_submit)
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' then metric_wow_diff * b.bgc_submit * b.cvr_1_2_activation_to_fd
    end as new_dx_impact_wow_diff
  , case 
      when metric_name = 'cvr_0_apply_to_fd' then accumulated_decline * b.applicants 
      when metric_name = 'cvr_1_1_apply_to_activation' then accumulated_decline * b.applicants * b.cvr_1_2_activation_to_fd
      when metric_name = 'cvr_1_2_activation_to_fd' then accumulated_decline * b.account_activation 
      when metric_name = 'cvr_2_1_apply_to_vs' then accumulated_decline * b.applicants * (b.first_delivery / b.vehicle_submit)
      when metric_name = 'cvr_2_2_vs_to_idv_submit' then accumulated_decline * b.vehicle_submit * (b.first_delivery / b.idv_submit) 
      when metric_name = 'cvr_2_3_idv_submit_to_approve' then accumulated_decline * b.idv_submit * (b.first_delivery / b.idv_approve)
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' then accumulated_decline * b.idv_approve * (b.first_delivery / b.bgc_submit)
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' then accumulated_decline * b.bgc_submit * b.cvr_1_2_activation_to_fd
    end as new_dx_impact_acc_diff
  , case 
      when metric_name = 'cvr_0_apply_to_fd' then metric_lm_avg_diff * b.applicants 
      when metric_name = 'cvr_1_1_apply_to_activation' then metric_lm_avg_diff * b.applicants * b.cvr_1_2_activation_to_fd
      when metric_name = 'cvr_1_2_activation_to_fd' then metric_lm_avg_diff * b.account_activation 
      when metric_name = 'cvr_2_1_apply_to_vs' then metric_lm_avg_diff * b.applicants * (b.first_delivery / b.vehicle_submit)
      when metric_name = 'cvr_2_2_vs_to_idv_submit' then metric_lm_avg_diff * b.vehicle_submit * (b.first_delivery / b.idv_submit) 
      when metric_name = 'cvr_2_3_idv_submit_to_approve' then metric_lm_avg_diff * b.idv_submit * (b.first_delivery / b.idv_approve)
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' then metric_lm_avg_diff * b.idv_approve * (b.first_delivery / b.bgc_submit)
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' then metric_lm_avg_diff * b.bgc_submit * b.cvr_1_2_activation_to_fd
    end as new_dx_impact_lm_avg_diff
  , case 
      when metric_name = 'cvr_0_apply_to_fd' then 1500 / b.applicants 
      when metric_name = 'cvr_1_1_apply_to_activation' then 1200 / (b.applicants * b.cvr_1_2_activation_to_fd)
      when metric_name = 'cvr_1_2_activation_to_fd' then  650 / b.account_activation 
      when metric_name = 'cvr_2_1_apply_to_vs' then 600 / (b.applicants * (b.first_delivery / b.vehicle_submit))
      when metric_name = 'cvr_2_2_vs_to_idv_submit' then 350 / (b.vehicle_submit * (b.first_delivery / b.idv_submit))
      when metric_name = 'cvr_2_3_idv_submit_to_approve' then 550 / (b.idv_submit * (b.first_delivery / b.idv_approve))
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' then 400 / (b.idv_approve * (b.first_delivery / b.bgc_submit))
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' then 800 / (b.bgc_submit * b.cvr_1_2_activation_to_fd)
    end as metric_variance_threshold_abs
  , div0(metric_variance_threshold_abs, metric_value_pw) as metric_variance_threshold_rel
  , sum(new_dx_impact_acc_diff) over (partition by metric_number, streak_group order by a.week_start) as acc_new_dx_impact
  , div0(acc_new_dx_impact, b.applicants) as acc_cvr_impact_abs
  , div0(acc_cvr_impact_abs, b.cvr_0_apply_to_fd) as acc_cvr_impact_rel
from accumulated_decline a
left join base as b on b.week_start = a.week_start
)

select 
  current_timestamp as last_updated_at
  , a.* 
  -- overall variance threshold 1.5k or 2k
  , case when new_dx_impact_wow_diff <  - 1500 then 'No' else 'Yes' end as c1_wow_within_variance_threshold --  $cvr_0_new_dx_impact_threshold
  , case when acc_new_dx_impact <  - 1500 then 'No' else 'Yes' end as c2_acc_within_variance_threshold -- $cvr_0_new_dx_impact_threshold
  , case when new_dx_impact_lm_avg_diff <  - 1500 then 'No' else 'Yes' end as c3_lm_avg_within_variance_threshold  -- $cvr_0_new_dx_impact_threshold
  -- whether within substeps
  , case 
      when metric_name = 'cvr_0_apply_to_fd' and new_dx_impact_wow_diff <  - 1500 then 'No' -- $cvr_0_new_dx_impact_threshold
      when metric_name = 'cvr_1_1_apply_to_activation' and new_dx_impact_wow_diff <  - 1200 then 'No' -- $cvr_1_1_new_dx_impact_threshold
      when metric_name = 'cvr_1_2_activation_to_fd' and new_dx_impact_wow_diff <  - 650 then 'No'  -- $cvr_1_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_1_apply_to_vs' and new_dx_impact_wow_diff <  - 600 then 'No' -- $cvr_2_1_new_dx_impact_threshold
      when metric_name = 'cvr_2_2_vs_to_idv_submit' and new_dx_impact_wow_diff <  - 350 then 'No'  -- $cvr_2_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_3_idv_submit_to_approve' and new_dx_impact_wow_diff <  - 550 then 'No' -- $cvr_2_3_new_dx_impact_threshold
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' and new_dx_impact_wow_diff <  - 400 then 'No' -- $cvr_2_4_new_dx_impact_threshold      
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' and new_dx_impact_wow_diff <  - 800 then 'No' -- $cvr_2_5_new_dx_impact_threshold
      else 'Yes'
    end as c4_wow_within_substep_variance_threshold
  , case 
      when metric_name = 'cvr_0_apply_to_fd' and acc_new_dx_impact <  - 1500 then 'No' -- $cvr_0_new_dx_impact_threshold
      when metric_name = 'cvr_1_1_apply_to_activation' and acc_new_dx_impact <  - 1200 then 'No'  -- $cvr_1_1_new_dx_impact_threshold
      when metric_name = 'cvr_1_2_activation_to_fd' and acc_new_dx_impact <  - 650 then 'No'  -- $cvr_1_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_1_apply_to_vs' and acc_new_dx_impact <  - 600 then 'No' -- $cvr_2_1_new_dx_impact_threshold
      when metric_name = 'cvr_2_2_vs_to_idv_submit' and acc_new_dx_impact <  - 350 then 'No' -- $cvr_2_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_3_idv_submit_to_approve' and acc_new_dx_impact <  - 550 then 'No' -- $cvr_2_3_new_dx_impact_threshold
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' and acc_new_dx_impact <  - 400 then 'No' -- $cvr_2_4_new_dx_impact_threshold
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' and acc_new_dx_impact <  - 800 then 'No' -- $cvr_2_5_new_dx_impact_threshold
      else 'Yes'
    end as c5_acc_within_substep_variance_threshold
  , case 
      when metric_name = 'cvr_0_apply_to_fd' and new_dx_impact_lm_avg_diff <  - 1500 then 'No'  --$cvr_0_new_dx_impact_threshold
      when metric_name = 'cvr_1_1_apply_to_activation' and new_dx_impact_lm_avg_diff <  - 1200 then 'No' -- $cvr_1_1_new_dx_impact_threshold
      when metric_name = 'cvr_1_2_activation_to_fd' and new_dx_impact_lm_avg_diff <  - 650 then 'No'  -- $cvr_1_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_1_apply_to_vs' and new_dx_impact_lm_avg_diff <  - 600 then 'No' -- $cvr_2_1_new_dx_impact_threshold 
      when metric_name = 'cvr_2_2_vs_to_idv_submit' and new_dx_impact_lm_avg_diff <  - 350 then 'No' -- $cvr_2_2_new_dx_impact_threshold
      when metric_name = 'cvr_2_3_idv_submit_to_approve' and new_dx_impact_lm_avg_diff <  - 550 then 'No' -- $cvr_2_3_new_dx_impact_threshold
      when metric_name = 'cvr_2_4_idv_approve_to_bgc_submit' and new_dx_impact_lm_avg_diff <  - 400 then 'No' -- $cvr_2_4_new_dx_impact_threshold
      when metric_name = 'cvr_2_5_bgc_submit_to_aa' and new_dx_impact_lm_avg_diff <  - 800 then 'No' -- $cvr_2_5_new_dx_impact_threshold
      else 'Yes'
    end as c6_lm_avg_within_substep_variance_threshold
  , case when consecutive_decline_weeks <= 4 then 'Yes' else 'No' end as c7_weeks_meet_threshold
  , coalesce(b.holiday_name, 'Not a Holiday') as c8_is_a_holiday_week
from new_dx_impact a
left join static.tbl_us_federal_holidays_snowflake b on date_trunc('week', b.holiday_date) = a.week_start
;

grant select on proddb.static.tbl_cvr_reporting_metric_variances_snowflake to public;
