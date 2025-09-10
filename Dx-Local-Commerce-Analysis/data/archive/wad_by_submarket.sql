with prep as (
select 
  date_trunc('week', ds.active_date) as active_week
  -- , case 
  --     when a.age between 18 and 24 then '1-age_18_to_24'
  --     when a.age between 25 and 44 then '2-age_25_to_44'
  --     -- when a.age between 35 and 44 then '3-age_45_to_44'
  --     when a.age between 45 and 64 then '3-age_45_to_64'
  --     when a.age >= 65 then '5-age_65_above'
  --     else '7-not_mapped'
  --   end as age_bucket
  , ds.submarket_id
  , count(distinct case when a.age between 18 and 24 then ds.dasher_id end) dx_cnt_18_to_24
  , count(distinct case when a.age between 25 and 44 then ds.dasher_id end) dx_cnt_25_to_44
  , count(distinct case when a.age between 45 and 64 then ds.dasher_id end) dx_cnt_45_to_64
  , count(distinct case when a.age >= 65 then ds.dasher_id end) dx_cnt_65_above
  , count(distinct ds.dasher_id) dx_cnt
--   , sum(dx_cnt) over (partition by active_week) total_dx
--   , div0(dx_cnt, total_dx) as dx_share
--   , sum(ds.adj_shift_seconds) / 3600 as hours
--   , sum(hours) over(partition by active_week) total_hours
--   , div0(hours, total_hours) as hours_share
from edw.dasher.dasher_shift_starting_points ds
left join edw.dasher.dimension_dasher_applicants a on a.dasher_id = ds.dasher_id
where 1=1
  and ds.dasher_id is not null
  and ds.check_in_time is not null
  and ds.check_out_time is not null
  and ds.check_out_time > ds.check_in_time
  and ds.has_preassign = false
  and date_trunc('week', ds.active_date) between '2023-01-01' and '2025-07-31'
group by all
)

select 
  *
from prep