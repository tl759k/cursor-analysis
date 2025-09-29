-- Summary Analysis: New DX vs Existing DX Assignment Patterns
-- Purpose: Compare assignment rates, timing, and completion between new and existing DX

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
  , is_first_dash
  , a.dasher_id
  , num_assigns
  , num_accepts
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
  , b.assignment_creation_time
  , timediff('minutes', a.shift_check_in_time, b.assignment_creation_time) as minutes_to_first_assignment
from tbl_has_shift_creation a
left join first_assignment_per_shift b on a.dasher_id = b.dasher_id and a.shift_id = b.shift_id
)

-- Final summary analysis
select
  new_dx_l7d
  , is_first_dash
  , count(distinct dasher_id) as total_dashers
  , count(distinct shift_id) as total_shifts
  
  -- Assignment metrics
  , sum(case when num_assigns > 0 then 1 else 0 end) as shifts_with_assignments
  , div0(sum(case when num_assigns > 0 then 1 else 0 end), count(distinct shift_id)) as pct_shifts_with_assignments
  , avg(num_assigns) as avg_assignments_per_shift
  
  -- Acceptance metrics  
  , sum(case when num_accepts > 0 then 1 else 0 end) as shifts_with_accepts
  , div0(sum(case when num_accepts > 0 then 1 else 0 end), count(distinct shift_id)) as pct_shifts_with_accepts
  , avg(num_accepts) as avg_accepts_per_shift
  , avg(ar) as avg_acceptance_rate
  
  -- Delivery completion metrics
  , sum(case when num_deliveries > 0 then 1 else 0 end) as shifts_with_deliveries
  , div0(sum(case when num_deliveries > 0 then 1 else 0 end), count(distinct shift_id)) as pct_shifts_with_deliveries
  , avg(num_deliveries) as avg_deliveries_per_shift
  , avg(delivery_rate) as avg_delivery_completion_rate
  
  -- Timing metrics
  , avg(minutes_to_first_assignment) as avg_minutes_to_first_assignment
  , median(minutes_to_first_assignment) as median_minutes_to_first_assignment
  , percentile_cont(0.90) within group (order by minutes_to_first_assignment) as p90_minutes_to_first_assignment
  
  -- Shift duration
  , avg(adj_shift_seconds / 3600) as avg_shift_hours

from shift_assignment_combined
group by new_dx_l7d, is_first_dash
order by new_dx_l7d, is_first_dash
