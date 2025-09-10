with prep as (
select 
  date_trunc('week', ds.active_date) as active_week
  , case 
      when a.age between 18 and 24 then '1-age_18_to_24' 
      -- else
      when a.age between 25 and 34 then '2-age_25_to_44'
      when a.age between 45 and 64 then '3-age_45_to_64'
      when a.age >= 65 then '4-age_65_above'
      else '5-not_mapped'
    end as age_bucket
  -- , ds.submarket_id
  , count(distinct ds.dasher_id) dx_cnt
  , sum(dx_cnt) over (partition by active_week) total_dx
  , div0(dx_cnt, total_dx) as dx_share
  , sum(ds.adj_shift_seconds) / 3600 as hours
  , sum(hours) over(partition by active_week) total_hours
  , div0(hours, total_hours) as hours_share
  , div0(hours, dx_cnt)as hours_per_dx
  , sum(ds.num_deliveries) as deliveries
  , sum(deliveries) over(partition by active_week) total_deliveries
  , div0(deliveries, total_deliveries) as deliveries_share
  , div0(deliveries, dx_cnt) as deliveries_per_dx
  , avg(ds.active_efficiency) as ae
  , sum(ds.num_deliveries) / sum(ds.total_active_time_seconds / 3600) as ae_per_dx
from edw.dasher.dasher_shift_starting_points ds
left join edw.dasher.dimension_dasher_applicants a on a.dasher_id = ds.dasher_id
where 1=1
  and ds.dasher_id is not null
  and ds.check_in_time is not null
  and ds.check_out_time is not null
  and ds.check_out_time > ds.check_in_time
  and ds.has_preassign = false
  and date_trunc('week', ds.active_date) between '2020-01-01' and '2025-07-31'
group by 1,2
)

select 
  *
from prep
order by active_week, age_bucket asc