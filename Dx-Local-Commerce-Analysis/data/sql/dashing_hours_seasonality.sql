with prep as (
select 
  date_trunc('month', ds.active_date) as active_month
  , date_trunc('week', ds.active_date) as active_week
  , case 
      when a.age between 18 and 24 then '1-age_18_to_24' 
      when a.age between 25 and 34 then '2-age_25_to_44'
      when a.age between 45 and 64 then '3-age_45_to_64'
      when a.age >= 65 then '4-age_65_above'
      else '5-not_mapped'
    end as age_bucket
  , count(distinct ds.dasher_id) dx_cnt
  , sum(ds.adj_shift_seconds) / 3600 as hours
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
  and date_trunc('week', ds.active_date) between '2023-01-01' and '2025-07-31'
  and fr.country_id = 1
group by all
)

, output as (
select
  active_month
  , avg(dx_cnt) dx_cnt
  , avg(hours) hours
  , avg(hours_per_dx) hours_per_dx
  , avg(hours) / avg(dx_cnt) as hours_per_dx_calc
group by all
order by 1 asc
)

select * from output

-- select
--   current_date as Updated_as_of
--   , 'Dasher' dataset
--   , 'Dasher - dashing hour pattern by age bucket' description
--   , 'U.S.' geo_split
--   , 'U.S.' as geo_dimension
--   , age_bucket as item_category
--   , 'Hour in the day' as period
--   , dashing_hour as TIMESTAMP
--   , dx_cnt
--   , total_dx
--   , dx_share
--   , hours
--   , total_hours
--   , hours_share
--   , hours_per_dx
-- from output
-- order by age_bucket, dashing_hour asc