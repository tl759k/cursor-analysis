-- Extended New Dasher Assignment and Completion Analysis
-- This query provides detailed funnel metrics for understanding why new dashers don't complete deliveries

-- Base data: dasher shifts with assignment and completion metrics
with tbl_dasher_shifts as (
select distinct
  date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time)) as shift_check_in_week
  , convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time) as shift_check_in_time
  , shift_id
  , a.dasher_id
  , num_deliveries
  , total_active_time_seconds
  , adj_shift_seconds
  , active_efficiency
  , auto_assign
  , is_first_dash
  , is_dash_now
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
  , case when num_assigns > 0 then 'received_assignment' else 'no_assignment' end as assignment_status
  , case when num_accepts > 0 then 'accepted_assignment' else 'no_acceptance' end as acceptance_status
  , case when num_deliveries > 0 then 'completed_delivery' else 'no_completion' end as completion_status
  , case when adj_shift_seconds > 3600 then 'long_shift' else 'short_shift' end as shift_duration_category
  , city_id
  , starting_point_id
from edw.dasher.dasher_shifts a
left join edw.dasher.dimension_dasher_applicants c on a.dasher_id = c.dasher_id
where 1=1
  and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', a.check_in_time)) 
      between dateadd('week', -8, date_trunc('week', current_date)) 
      and dateadd('week', -1, date_trunc('week', current_date))
)

-- Assignment details for shifts that received assignments
, assignment_details as (
select 
  sda.shift_id
  , sda.dasher_id
  , sda.delivery_id
  , convert_timezone('UTC', 'America/Los_Angeles', sda.created_at) as assignment_created_at
  , convert_timezone('UTC', 'America/Los_Angeles', sda.accepted_at) as assignment_accepted_at
  , convert_timezone('UTC', 'America/Los_Angeles', sda.unassigned_at) as assignment_unassigned_at
  , sda.unassign_reason_text
  , sda.is_batched
  , case when accepted_at is not null then 1 else 0 end as was_accepted
  , case when accepted_at is not null and unassigned_at is not null then 1 else 0 end as unassigned_after_accept
  , row_number() over(partition by sda.shift_id, sda.dasher_id order by sda.created_at asc) as assignment_sequence
from proddb.prod_assignment.shift_delivery_assignment sda
where 1=1
  and date_trunc('week', convert_timezone('UTC', 'America/Los_Angeles', sda.created_at)) 
      between dateadd('week', -8, date_trunc('week', current_date)) 
      and dateadd('week', -1, date_trunc('week', current_date))
)

-- Time to first assignment for each shift
, first_assignment_timing as (
select 
  shift_id
  , dasher_id
  , min(assignment_created_at) as first_assignment_time
  , count(distinct delivery_id) as total_assignments_received
  , sum(was_accepted) as total_assignments_accepted
  , sum(unassigned_after_accept) as total_unassigned_after_accept
  , array_agg(distinct unassign_reason_text) within group (order by unassign_reason_text) as unassign_reasons
from assignment_details
group by shift_id, dasher_id
)

-- Delivery completion details
, delivery_completion as (
select 
  sda.shift_id
  , sda.dasher_id
  , sda.delivery_id
  , dd.actual_delivery_time
  , dd.cancelled_at
  , dd.is_cancelled
  , dd.cancellation_reason
  , dd.store_id
  , dd.subtotal_usd / 100 as order_subtotal_usd
  , dd.delivery_fee_usd / 100 as delivery_fee_usd
  , case when dd.actual_delivery_time is not null then 1 else 0 end as was_completed
  , case when dd.is_cancelled then 1 else 0 end as was_cancelled
from assignment_details sda
inner join edw.finance.dimension_deliveries dd on sda.delivery_id = dd.delivery_id
where sda.was_accepted = 1
)

-- Store characteristics for assignments
, store_context as (
select 
  dc.shift_id
  , dc.dasher_id
  , avg(dc.order_subtotal_usd) as avg_order_value
  , count(distinct dc.store_id) as unique_stores_assigned
  , array_agg(distinct ds.business_name) within group (order by ds.business_name) as store_names
  , avg(ds.prep_time_seconds) / 60 as avg_store_prep_time_minutes
from delivery_completion dc
left join edw.merchant.dimension_store ds on dc.store_id = ds.store_id
group by dc.shift_id, dc.dasher_id
)

-- Combine all data sources
, comprehensive_analysis as (
select 
  ds.shift_check_in_week
  , ds.shift_check_in_time
  , ds.shift_id
  , ds.dasher_id
  , ds.new_dx_l7d
  , ds.is_first_dash
  , ds.city_id
  , ds.starting_point_id
  
  -- Shift characteristics
  , ds.adj_shift_seconds / 3600.0 as shift_hours
  , ds.shift_duration_category
  , ds.is_time_mode
  , ds.auto_assign
  , ds.is_high_ar_top_dasher_for_shift
  
  -- Assignment funnel metrics
  , ds.assignment_status
  , ds.acceptance_status  
  , ds.completion_status
  , ds.num_assigns
  , ds.num_accepts
  , ds.num_deliveries
  , ds.ar
  , ds.delivery_rate
  
  -- Assignment timing and details
  , fat.first_assignment_time
  , datediff('minutes', ds.shift_check_in_time, fat.first_assignment_time) as minutes_to_first_assignment
  , fat.total_assignments_received
  , fat.total_assignments_accepted
  , fat.total_unassigned_after_accept
  , fat.unassign_reasons
  
  -- Delivery completion details  
  , count(distinct dc.delivery_id) as total_deliveries_attempted
  , sum(dc.was_completed) as total_deliveries_completed
  , sum(dc.was_cancelled) as total_deliveries_cancelled
  , avg(dc.order_subtotal_usd) as avg_order_value
  
  -- Store context
  , sc.unique_stores_assigned
  , sc.avg_store_prep_time_minutes
  
  -- Financial outcomes
  , ds.total_pay_usd
  , ds.total_profit_usd
  , div0(ds.total_pay_usd, ds.adj_shift_seconds / 3600.0) as hourly_pay
  
  -- Performance flags
  , case when ds.num_assigns = 0 then 'no_assignments'
         when ds.num_accepts = 0 then 'no_acceptances'  
         when ds.num_deliveries = 0 then 'no_completions'
         else 'successful_completion' end as funnel_outcome
         
from tbl_dasher_shifts ds
left join first_assignment_timing fat on ds.shift_id = fat.shift_id and ds.dasher_id = fat.dasher_id
left join delivery_completion dc on ds.shift_id = dc.shift_id and ds.dasher_id = dc.dasher_id
left join store_context sc on ds.shift_id = sc.shift_id and ds.dasher_id = sc.dasher_id
group by all
)

-- Final aggregated analysis
select 
  shift_check_in_week
  , new_dx_l7d
  , is_first_dash
  , funnel_outcome
  , city_id
  , shift_duration_category
  , is_time_mode
  , auto_assign
  
  -- Counts
  , count(distinct dasher_id) as dashers_count
  , count(distinct shift_id) as shifts_count
  
  -- Assignment metrics
  , avg(num_assigns) as avg_assignments_received
  , avg(num_accepts) as avg_assignments_accepted  
  , avg(num_deliveries) as avg_deliveries_completed
  , avg(ar) as avg_acceptance_rate
  , avg(delivery_rate) as avg_delivery_completion_rate
  
  -- Timing metrics
  , avg(minutes_to_first_assignment) as avg_minutes_to_first_assignment
  , avg(shift_hours) as avg_shift_hours
  
  -- Financial metrics
  , avg(hourly_pay) as avg_hourly_pay
  , avg(avg_order_value) as avg_order_value
  
  -- Completion funnel
  , sum(case when funnel_outcome = 'no_assignments' then 1 else 0 end) as no_assignments_count
  , sum(case when funnel_outcome = 'no_acceptances' then 1 else 0 end) as no_acceptances_count
  , sum(case when funnel_outcome = 'no_completions' then 1 else 0 end) as no_completions_count
  , sum(case when funnel_outcome = 'successful_completion' then 1 else 0 end) as successful_completion_count

from comprehensive_analysis
group by all
order by shift_check_in_week desc, new_dx_l7d, is_first_dash, funnel_outcome
