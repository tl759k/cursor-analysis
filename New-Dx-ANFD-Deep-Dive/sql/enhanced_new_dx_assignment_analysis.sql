-- Enhanced New DX Assignment Analysis
-- Purpose: Analyze why new DX are not completing first deliveries
-- Focus: Assignment rates, timing, and completion patterns

set start_date = dateadd('week', -4, date_trunc('week', current_date));
set end_date = dateadd('week', -1, date_trunc('week', current_date));

-- Base shift data with new DX identification
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
  , div0(num_accepts, num_assigns) as ar
  , div0(num_deliveries, num_accepts) as delivery_rate
  , c.applied_date
from edw.dasher.dasher_shifts a
left join edw.dasher.dimension_dasher_applicants c on a.dasher_id = c.dasher_id
where 1=1
  and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time)) between $start_date and $end_date 
)

-- All assignments during the time period
, all_assignments as (
select
  dasher_id
  , shift_id
  , delivery_id
  , convert_timezone('UTC', 'America/Los_Angeles', created_at) as assignment_creation_time
  , convert_timezone('UTC', 'America/Los_Angeles', accepted_at) as assignment_accepted_time 
  , case when accepted_at is not null and unassigned_at is not null then 'Y' else 'N' end as unassigned_after_accepted
  , unassign_reason_text
  , is_batched
  , row_number() over(partition by dasher_id, shift_id order by created_at asc) as assignment_rn
from proddb.prod_assignment.shift_delivery_assignment
where 1=1
  and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', created_at)) between $start_date and $end_date 
)

-- First assignment per shift
, first_assignment_per_shift as (
select
  *
from all_assignments 
where assignment_rn = 1
)

-- Combined data with assignment timing
, shift_assignment_combined as (
select 
  a.*
  , b.delivery_id
  , b.assignment_creation_time
  , b.assignment_accepted_time 
  , b.unassigned_after_accepted
  , b.unassign_reason_text
  , b.is_batched
  , timediff('seconds', a.shift_check_in_time, b.assignment_creation_time) as seconds_to_first_assignment
  , timediff('minutes', a.shift_check_in_time, b.assignment_creation_time) as minutes_to_first_assignment
  , case 
      when b.assignment_creation_time is null then 'no_assignment'
      when timediff('minutes', a.shift_check_in_time, b.assignment_creation_time) <= 5 then 'within_5_min'
      when timediff('minutes', a.shift_check_in_time, b.assignment_creation_time) <= 15 then 'within_15_min'
      when timediff('minutes', a.shift_check_in_time, b.assignment_creation_time) <= 30 then 'within_30_min'
      else 'over_30_min'
    end as time_to_first_assignment_bucket
from tbl_has_shift_creation a
left join first_assignment_per_shift b on a.dasher_id = b.dasher_id and a.shift_id = b.shift_id
)

-- Final analysis output
select
  shift_check_in_week
  , new_dx_l7d
  , is_first_dash
  , case when num_deliveries > 0 then 'completed_delivery' else 'no_delivery' end as delivery_completion_status
  , case when num_assigns > 0 then 'received_assignment' else 'no_assignment' end as assignment_status
  , time_to_first_assignment_bucket
  , count(distinct dasher_id) as dasher_count
  , count(distinct shift_id) as shift_count
  , avg(num_assigns) as avg_assignments_per_shift
  , avg(num_accepts) as avg_accepts_per_shift
  , avg(num_deliveries) as avg_deliveries_per_shift
  , avg(ar) as avg_acceptance_rate
  , avg(delivery_rate) as avg_delivery_completion_rate
  , avg(adj_shift_seconds / 3600) as avg_shift_hours
  , avg(minutes_to_first_assignment) as avg_minutes_to_first_assignment
  , median(minutes_to_first_assignment) as median_minutes_to_first_assignment
  , percentile_cont(0.25) within group (order by minutes_to_first_assignment) as p25_minutes_to_first_assignment
  , percentile_cont(0.75) within group (order by minutes_to_first_assignment) as p75_minutes_to_first_assignment
  , percentile_cont(0.90) within group (order by minutes_to_first_assignment) as p90_minutes_to_first_assignment
from shift_assignment_combined
group by all
order by shift_check_in_week, new_dx_l7d, is_first_dash, delivery_completion_status, assignment_status, time_to_first_assignment_bucket
