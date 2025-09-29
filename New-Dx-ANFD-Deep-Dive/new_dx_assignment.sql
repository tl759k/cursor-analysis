set start_date = dateadd('week', -4, date_trunc('week', current_date));
set end_date = dateadd('week', -1, date_trunc('week', current_date));

-- anyone who created/checkin a shift
with tbl_has_shift_creation as (
select distinct
  date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time)) as shift_check_in_week
  , convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time) as shift_check_in_time
  , shift_id
  , num_deliveries
  , total_active_time_seconds
  , adj_shift_seconds
  , active_efficiency
  , auto_assign
  , is_first_dash
  , is_dash_now
  , a.dasher_id
  , num_assigns
  , num_accepts
  , is_time_mode
  , is_high_ar_top_dasher_for_shift
  , total_pay_usd / 100 as total_pay_usd
  , total_mileage
  , total_profit_usd / 100 as total_profit_usd
  , case when date_trunc('week', c.applied_date) = date_trunc('week', shift_check_in_time::date) then 'Y' else 'N' end as new_dx_l7d
  , div0(num_accepts, num_assigns) ar
  , div0(num_deliveries, num_accepts) delivery_rate -- deliveries completed out of accepted
from edw.dasher.dasher_shifts a
left join edw.dasher.dimension_dasher_applicants c on a.dasher_id = c.dasher_id
where 1=1
  and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time)) between $start_date and $end_date 
)

, dasher_level_agg as (
select
  shift_check_in_week
  , new_dx_l7d
  , dasher_id
  , is_first_dash
  -- , case when adj_shift_seconds > 0 then 'online' else 'not_online' end spent_time_on_shift
  , case when num_deliveries > 0 then 'has_completion' else 'no_completion' end completed_delivery
  , case when num_assigns > 0 then 'has_assignment' else 'no_assignment' end has_assignment
  , count(distinct shift_id) shift_cnt
  , avg(num_deliveries) num_deliveries
  , avg(adj_shift_seconds / 3600) shift_hours
  , avg(num_assigns) num_assigns
  , avg(num_accepts) num_accepts
  , avg(ar) ar
  , avg(delivery_rate) delivery_rate
from tbl_has_shift_creation
group by all
)


select
  shift_check_in_week
  , new_dx_l7d
  , is_first_dash
  -- , spent_time_on_shift
  , completed_delivery
  , has_assignment
  , count(distinct dasher_id) dx_cnt
  , avg(shift_cnt) shift_cnt
  , avg(num_deliveries) num_deliveries
  , avg(shift_hours) shift_hours
  , avg(num_assigns) num_assigns
  , avg(num_accepts) num_accepts
  , avg(ar) ar
  , avg(delivery_rate) delivery_rate
from dasher_level_agg
group by all
order by shift_check_in_week asc

-- , all_assignments as (
-- select
--   dasher_id
--   , shift_id
--   , delivery_id
--   , convert_timezone('UTC', 'America/Los_Angeles', created_at) as assignment_creation_time
--   , convert_timezone('UTC', 'America/Los_Angeles', accepted_at) as assignment_accepted_time 
--   , case when accepted_at is not null and unassigned_at is not null then 'Y' else 'N' end unassigned_after_accepted
--   , unassign_reason_text
--   , is_batched
--   , row_number () over(partition by dasher_id, shift_id order by created_at asc) rn
-- from proddb.prod_assignment.shift_delivery_assignment
-- where 1=1
--   and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', created_at)) between $start_date and $end_date 
-- )

-- , first_assignment_within_the_shift as (
-- select
--   *
-- from all_assignments 
-- where rn = 1
-- )

-- , combine_results as (
-- select 
--   a.*
--   , b.delivery_id
--   , b.assignment_creation_time
--   , b.assignment_accepted_time 
--   , b.unassigned_after_accepted
--   , b.unassign_reason_text
--   , b.is_batched
--   , b.rn
--   , timediff('seconds', a.shift_creation_time, b.assignment_creation_time) as time_shift_creation_to_first_assignment
--   , c.applied_date
--   , case when date_trunc('week', c.applied_date) = date_trunc('week', a.shift_creation_time::date) then 'Y' else 'N' end as new_dx_l7d
-- from tbl_has_shift_creation a
-- left join first_assignment_within_the_shift b on a.dasher_id = b.dasher_id and a.shift_id = b.shift_id
-- left join edw.dasher.dimension_dasher_applicants c on a.dasher_id = c.dasher_id
-- -- where is_first_dash = true
-- )