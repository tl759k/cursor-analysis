with prep as (
select 
  date_trunc('week', ds.active_date) as active_week
  , hour(ds.check_in_time) dashing_hour
  , case 
      when a.age between 18 and 24 then '1-age_18_to_24' 
      when a.age between 25 and 34 then '2-age_25_to_44'
      when a.age between 45 and 64 then '3-age_45_to_64'
      when a.age >= 65 then '4-age_65_above'
      else '5-not_mapped'
    end as age_bucket
  -- , ds.submarket_id
  -- , fr.submarket_name
  , count(distinct ds.dasher_id) dx_cnt
  , sum(dx_cnt) over (partition by active_week, dashing_hour) total_dx
  , div0(dx_cnt, total_dx) as dx_share
  , sum(ds.adj_shift_seconds) / 3600 as hours
  , sum(hours) over(partition by active_week, dashing_hour) total_hours
  , div0(hours, total_hours) as hours_share
  , hours / dx_cnt as hours_per_dx
from edw.dasher.dasher_shift_starting_points ds
left join edw.dasher.dimension_dasher_applicants a on a.dasher_id = ds.dasher_id
left join fact_region fr on fr.submarket_id = ds.submarket_id
where 1=1
  and ds.dasher_id is not null
  and ds.check_in_time is not null
  and ds.check_out_time is not null
  and ds.check_out_time > ds.check_in_time
  and ds.has_preassign = false
  and date_trunc('week', ds.active_date) between '2025-01-01' and '2025-07-31'
  and fr.country_id = 1
group by all
)


select
  dashing_hour
  , age_bucket
  , avg(dx_cnt) dx_cnt
  , avg(total_dx) total_dx
  , avg(dx_share)  dx_share
  , avg(hours) hours
  , avg(total_hours) total_hours
  , avg(hours_share) hours_share
  , avg(hours_per_dx) hours_per_dx
from prep
group by all
order by 1, 2 asc